from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime


class SparkUtils:
    def __init__(self):
        pass

    def create_ingestion_spark_bash_operator(self, task_id, script_name, dag):
        return BashOperator(
            task_id=task_id,
            bash_command=(
                'batch_run_timestamp="{{ ti.xcom_pull(task_ids=\'create_data_folder_task\') }}" && '
                'current_date="{{ ti.xcom_pull(task_ids=\'create_data_folder_task\', key=\'current_date\') }}" && '
                'spark-submit --master spark://spark-master:7077 '
                '--jars /opt/airflow/jars/aws-java-sdk-bundle-1.12.316.jar,'
                '/opt/airflow/jars/delta-spark_2.12-3.2.0.jar,'
                '/opt/airflow/jars/delta-storage-3.2.0.jar,'
                '/opt/airflow/jars/hadoop-aws-3.3.4.jar,'
                '/opt/airflow/jars/hadoop-common-3.3.4.jar '
                f'/opt/airflow/jobs/ingestion/{script_name}.py --batch_run_timestamp $batch_run_timestamp --current_date $current_date'
            ),
            dag=dag
        )

    def create_data_folder(self, **kwargs):
        batch_run_timestamp = int(datetime.now().timestamp() * 1000)
        current_date = datetime.now().strftime('%Y%m%d')
        kwargs['ti'].xcom_push(key='batch_run_timestamp',
                               value=batch_run_timestamp)
        kwargs['ti'].xcom_push(key='current_date', value=current_date)
        return batch_run_timestamp

    def create_data_folder_task(self, dag):
        return PythonOperator(
            task_id='create_data_folder_task',
            python_callable=self.create_data_folder,
            provide_context=True,
            dag=dag
        )

    def create_g2i_spark_bash_operator(self, task_id, dataset_name, dag):
        """
        Creates a BashOperator to run a G2I Spark job.
        """
        return BashOperator(
            task_id=task_id,
            bash_command=(
                'dataset_name="{{ dag_run.conf.get(\'dataset\') }}" && '
                'config_path="/opt/airflow/job_entries/g2i/$dataset_name/config.json" && '
                'spark-submit --master spark://spark-master:7077 '
                '--jars /opt/airflow/jars/aws-java-sdk-bundle-1.12.316.jar,'
                '/opt/airflow/jars/delta-spark_2.12-3.2.0.jar,'
                '/opt/airflow/jars/delta-storage-3.2.0.jar,'
                '/opt/airflow/jars/hadoop-aws-3.3.4.jar,'
                '/opt/airflow/jars/hadoop-common-3.3.4.jar '
                '--packages io.delta:delta-spark_2.12:3.0.0 '
                '--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" '
                '--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" '
                '--conf "spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore" '
                '/opt/airflow/jobs/g2i/$dataset_name.py --dataset_name $dataset_name --config_path $config_path'
            ),
            dag=dag
        )
