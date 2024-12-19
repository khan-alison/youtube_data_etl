import os
import json
from io import BytesIO
from minio import Minio
from datetime import datetime
from airflow.operators.python import PythonOperator
from helper.logger import LoggerSimple


class MinioUtils:
    def __init__(self):
        self.logger = LoggerSimple.get_logger(__name__)
        self.minio_client = Minio(
            os.getenv("MINIO_ENDPOINT"),
            access_key=os.getenv("MINIO_ACCESS_KEY"),
            secret_key=os.getenv("MINIO_SECRET_KEY"),
            secure=False
        )
        self.bucket_name = os.getenv("DATALAKE_BUCKET")

    def generate_metadata_file_for_raw(self, source_system, database, table, batch_run_timestamp, date_str):
        """
        Generate control.json file for raw data stored in CSV format.
        """
        try:
            folder_path = f"{source_system}/{database}/{table}/data/date_{date_str}/batch_run_timestamp-{batch_run_timestamp}/"

            objects_list = list(self.minio_client.list_objects(
                self.bucket_name, prefix=folder_path, recursive=True))

            # Collect all CSV files
            partitions = [
                f"{obj.object_name}" for obj in objects_list if obj.object_name.endswith(".csv")
            ]

            metadata = {
                "event_type": "Raw Data Metadata",
                "date": date_str,
                "batch_run_timestamp": batch_run_timestamp,
                "partitions": partitions
            }

            metadata_path = f"{source_system}/{database}/{table}/metadata/date_{date_str}/batch_run_timestamp-{batch_run_timestamp}/control_file.json"
            metadata_json = json.dumps(metadata, indent=4)
            metadata_bytes = metadata_json.encode('utf-8')

            self.minio_client.put_object(
                self.bucket_name,
                metadata_path,
                data=BytesIO(metadata_bytes),
                length=len(metadata_bytes),
                content_type="application/json"
            )
            self.logger.info(
                f"Control file generated for raw data at {metadata_path}")

        except Exception as e:
            self.logger.error(
                f"Failed to generate metadata for raw data: {str(e)}")

    def generate_metadata_file_for_golden(self, source_system, database, table):
        """
        Generate control.json file for golden data stored in Parquet/Delta format.
        """
        try:
            folder_path = f"{source_system}/{database}/{table}/"

            objects_list = list(self.minio_client.list_objects(
                self.bucket_name, prefix=folder_path, recursive=True))

            partitions = [
                f"{obj.object_name}" for obj in objects_list if obj.object_name.endswith(".parquet")
            ]

            metadata = {
                "event_type": "Golden Data Metadata",
                "dataset": table,
                "partitions": partitions
            }

            metadata_path = f"{folder_path}control.json"
            metadata_json = json.dumps(metadata, indent=4)
            metadata_bytes = metadata_json.encode('utf-8')

            self.minio_client.put_object(
                self.bucket_name,
                metadata_path,
                data=BytesIO(metadata_bytes),
                length=len(metadata_bytes),
                content_type="application/json"
            )
            self.logger.info(
                f"Control file generated for golden data at {metadata_path}")

        except Exception as e:
            self.logger.error(
                f"Failed to generate metadata for golden data: {str(e)}")

    def create_metadata_task(self, task_id, source_system, database, table, dag):
        """
        Create an Airflow task to generate metadata files for raw or golden datasets.
        """
        if database == "raw":
            callable_func = self._generate_metadata_for_raw_wrapper
        elif database == "golden":
            callable_func = self._generate_metadata_for_golden_wrapper
        else:
            raise ValueError(
                f"Invalid database: {database}. Use 'raw' or 'golden'.")

        return PythonOperator(
            task_id=task_id,
            python_callable=callable_func,
            op_kwargs={
                'source_system': source_system,
                'database': database,
                'table': table
            },
            provide_context=True,
            dag=dag
        )

    def _generate_metadata_for_raw_wrapper(self, source_system, database, table, **context):
        """
        Wrapper for generate_metadata_file_for_raw to extract batch_run_timestamp and date_str from XCom.
        """
        ti = context['ti']
        batch_run_timestamp = ti.xcom_pull(task_ids='create_data_folder_task')
        date_str = datetime.now().strftime('%Y%m%d')
        self.generate_metadata_file_for_raw(
            source_system, database, table, batch_run_timestamp, date_str)

    def _generate_metadata_for_golden_wrapper(self, source_system, database, table, **context):
        """
        Wrapper for generate_metadata_file_for_golden.
        """
        self.generate_metadata_file_for_golden(source_system, database, table)
