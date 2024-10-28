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

    def generate_metadata_file(self, ti, source_system, database, table):
        try:
            batch_run_timestamp = ti.xcom_pull(task_ids='create_data_folder_task')
            date_str = datetime.now().strftime('%Y%m%d')

            folder_path = f"{source_system}/{database}/{table}/data/date_{date_str}/batch_run_timestamp-{batch_run_timestamp}/"

            objects_list = list(self.minio_client.list_objects(
                self.bucket_name, prefix=folder_path, recursive=True))

            partitions = [
                f"{obj.object_name}" for obj in objects_list if obj.object_name.endswith(".csv")]

            metadata = {
                "event_type": "Fetch data",
                "date": date_str,
                "batch_run_timestamp": batch_run_timestamp,
                "partitions": partitions
            }

            metadata_json = json.dumps(metadata, indent=4)
            metadata_bytes = metadata_json.encode('utf-8')

            metadata_path = f"{source_system}/{database}/{table}/metadata/date_{date_str}/batch_run_timestamp-{batch_run_timestamp}/control_file.json"

            self.minio_client.put_object(
                self.bucket_name,
                metadata_path,
                data=BytesIO(metadata_bytes),
                length=len(metadata_bytes),
                content_type="application/json"
            )
        except Exception as e:
            self.logger.error(f"Failed to generate metadata: {str(e)}")

    def create_metadata_task(self, task_id, source_system, database, table, dag):
        return PythonOperator(
            task_id=task_id,
            python_callable=self.generate_metadata_file,
            op_kwargs={
                'source_system': source_system,
                'database': database,
                'table': table
            },
            trigger_rule='all_done',
            dag=dag
        )