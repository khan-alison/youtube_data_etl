from airflow import DAG
from datetime import datetime, timedelta
from helper.logger import LoggerSimple
import os
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from urllib.parse import unquote
from common.hive_table_manager import HiveTableManager
import socket
from pyhive import hive


logger = LoggerSimple.get_logger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}


def extract_conf(**context):
    """
    Extract configuration from DAG run context and validate it
    """
    try:
        dag_run_conf = context['dag_run'].conf
        if not dag_run_conf:
            raise ValueError('No configuration provided in DAG trigger. 🤣')
        object_key = unquote(dag_run_conf.get('object_key', ''))

        logger.info(f'object_key {object_key}')
        logger.info(f"Processing configuration: {dag_run_conf}")

        ti = context['ti']
        ti.xcom_push(
            key='control_file_path',
            value=object_key
        )
        ti.xcom_push(
            key='config_file_path',
            value=dag_run_conf.get('config_path')
        )
        ti.xcom_push(
            key='bucket_name',
            value=dag_run_conf.get('bucket_name')
        )
        ti.xcom_push(
            key='source_system',
            value=dag_run_conf.get('source_system')
        )
        ti.xcom_push(key='table_name', value=dag_run_conf.get('table'))
    except Exception as e:
        logger.error(f"Error extracting configuration: {str(e)}")
        raise


with DAG(
    'orchestration_r2g_wrapper',
    default_args=default_args,
    description='A DAG that wrap the spark job.',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1
) as dag:

    extract_config_task = PythonOperator(
        task_id='extract_r2g_configuration',
        python_callable=extract_conf,
        dag=dag,
        provide_context=True,
    )

    spark_r2g_job = BashOperator(
        task_id='generic_etl_r2g_module',
        bash_command=(
            'control_file_path="{{ ti.xcom_pull(task_ids=\'extract_r2g_configuration\', key=\'control_file_path\') }}" && '
            'bucket_name="{{ ti.xcom_pull(task_ids=\'extract_r2g_configuration\', key=\'bucket_name\') }}" && '
            'table_name="{{ ti.xcom_pull(task_ids=\'extract_r2g_configuration\', key=\'table_name\') }}" && '
            'source_system="{{ ti.xcom_pull(task_ids=\'extract_r2g_configuration\', key=\'source_system\') }}" && '
            'spark-submit --master spark://spark-master:7077 '
            '--jars /opt/airflow/jars/aws-java-sdk-bundle-1.12.316.jar,'
            '/opt/airflow/jars/delta-core_2.12-2.4.0.jar,'
            '/opt/airflow/jars/delta-storage-2.3.0.jar,'
            '/opt/airflow/jars/hadoop-aws-3.3.4.jar,'
            '/opt/airflow/jars/hadoop-common-3.3.4.jar '
            '/opt/airflow/jobs/r2g/generic_etl_r2g_module.py '
            '--control_file_path "$control_file_path" '
            '--bucket_name "$bucket_name" '
            '--table_name "$table_name" '
            '--source_system "$source_system"'
        ),
        dag=dag
    )

    extract_config_task >> spark_r2g_job
