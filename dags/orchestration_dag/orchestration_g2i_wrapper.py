from airflow import DAG
from datetime import datetime, timedelta
from dags.libs_dag.spark_utils import SparkUtils

default_args = {
    'depends_on_past': False,
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

spark_utils = SparkUtils()

with DAG(
    'orchestration_g2i_wrapper',
    default_args=default_args,
    description='Orchestrate golden data to generate insights from data.',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1
) as dag:

    create_data_folder_task = spark_utils.create_data_folder_task(dag)

    g2i_spark_job = spark_utils.create_g2i_spark_bash_operator(
        task_id='g2i_spark_job',
        dataset_name="{{ dag_run.conf.get('dataset') }}",
        dag=dag
    )

    create_data_folder_task >> g2i_spark_job
