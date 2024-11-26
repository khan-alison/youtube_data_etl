from airflow import DAG
from datetime import datetime, timedelta
from helper.logger import LoggerSimple
import os
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from jobs.g2i.create_or_repair_table import create_or_repair_table
from urllib.parse import unquote
import socket
import json
from libs_dag.logging_manager import LoggingManager, TaskLog


logger = LoggerSimple.get_logger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}


def log_task_execution(task_id: str, status: str, **context):
    """Helper function to log task execution"""
    try:
        logger.info(f"Called log_task_execution with task_id: {task_id}, status: {status}")
        logging_manager = LoggingManager()

        dag_run_conf = context['dag_run'].conf or {}
        logger.info(f"Dag run configuration: {dag_run_conf}")

        task_log = TaskLog(
            job_id=task_id,
            step_function=context['dag'].dag_id,
            data_time={
                "execution_date": context['execution_date'].isoformat(),
                "source_system": dag_run_conf.get('source_system', ''),
                "table": dag_run_conf.get('table', '')
            },
            finish_events=[{
                "status": status,
                "timestamp": datetime.now().isoformat()
            }],
            trigger_events=[{
                "type": "minio_event",
                "details": {
                    "bucket": dag_run_conf.get('bucket_name'),
                    "object_key": dag_run_conf.get('object_key')
                }
            }],
            conditions=[],
            priority=1,
            step_function_input=dag_run_conf,
            is_disabled=False,
            tags=["r2g", dag_run_conf.get('source_system', '')]
        )
        logger.info(f"TaskLog instance created: {task_log}")

        logging_manager.log_task(task_log)
        logger.info(f"Successfully logged task execution: {task_id}")
    except Exception as e:
        logger.exception(f"Error logging task execution: {str(e)}")
        raise


def monitor_task_execution(**context):
    """Monitor task execution statistics"""
    try:
        logging_manager = LoggingManager()
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=24)

        task_ids = ['extract_r2g_configuration',
                    'generic_etl_r2g_module', 'create_or_repair_tables_task']

        for task_id in task_ids:
            logs = logging_manager.get_task_logs(
                job_id=task_id,
                time_range=(start_time, end_time)
            )

            if logs:
                success_count = sum(1 for log in logs
                                    if any(event['status'] == 'SUCCESS'
                                           for event in log['finish_events']))

                logger.info(f"Task {task_id} statistics:")
                logger.info(f"Total executions: {len(logs)}")
                logger.info(f"Successful executions: {success_count}")

    except Exception as e:
        logger.error(f"Error monitoring task execution: {str(e)}")


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


def execute_create_or_repair_tables(**context):
    """Create or repair Trino tables based on configuration"""
    try:
        ti = context.get('ti')
        config_file_path = ti.xcom_pull(
            task_ids="extract_r2g_configuration", key='table_name')
        with open(f'/tmp/{config_file_path}_output.json', 'r') as f:
            configs = json.load(f)

        logger.info(f"configs {configs}")

        if not configs:
            raise ValueError("No configuration found from previous task.")
        create_or_repair_table(configs)
    except Exception as e:
        logger.error(f"Error creating or repairing tables: {str(e)}")
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
            '/opt/airflow/jars/delta-spark_2.12-3.2.0.jar,'
            '/opt/airflow/jars/delta-storage-3.2.0.jar,'
            '/opt/airflow/jars/hadoop-aws-3.3.4.jar,'
            '/opt/airflow/jars/hadoop-common-3.3.4.jar '
            '--packages io.delta:delta-spark_2.12:3.0.0 '
            '--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" '
            '--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" '
            '--conf "spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore" '
            '--conf "spark.sql.catalog.spark_catalog.warehouse=s3a://lakehouse/youtube/golden" '
            '/opt/airflow/jobs/r2g/generic_etl_r2g_module.py '
            '--control_file_path "$control_file_path" '
            '--bucket_name "$bucket_name" '
            '--table_name "$table_name" '
            '--source_system "$source_system"'
        ),
        do_xcom_push=True,
        dag=dag
    )

    log_success_task = PythonOperator(
        task_id='log_success',
        python_callable=log_task_execution,
        op_kwargs={'task_id': 'generic_etl_r2g_module', 'status': 'SUCCESS'},
        trigger_rule='all_success',
        dag=dag,
    )

    log_failure_task = PythonOperator(
        task_id='log_failure',
        python_callable=log_task_execution,
        op_kwargs={'task_id': 'generic_etl_r2g_module', 'status': 'FAILURE'},
        trigger_rule='one_failed',
        dag=dag,
    )

    create_or_repair_tables_task = PythonOperator(
        task_id='create_or_repair_tables_task',
        python_callable=execute_create_or_repair_tables,
        dag=dag
    )

    monitor_execution_task = PythonOperator(
        task_id='monitor_execution',
        python_callable=monitor_task_execution,
        dag=dag
    )

    # Define task dependencies
    extract_config_task >> spark_r2g_job
    spark_r2g_job >> [log_success_task, log_failure_task]
    [log_success_task, log_failure_task] >> create_or_repair_tables_task
    create_or_repair_tables_task >> monitor_execution_task