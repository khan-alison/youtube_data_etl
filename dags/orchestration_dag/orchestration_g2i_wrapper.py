from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)

default_args = {
    'depends_on_past': False,
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}


with DAG(
    'orchestration_g2i_wrapper',
    default_args=default_args,
    description='Orchestrate golden data to generate insights from data.',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1
) as dag:

    spark_jobs = BashOperator(
        task_id='analysis_dataset',
        bash_command=('echo hello'),
        dag=dag
    )

    spark_jobs
