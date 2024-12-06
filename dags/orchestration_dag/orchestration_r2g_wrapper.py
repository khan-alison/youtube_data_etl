from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from helper.logger import LoggerSimple
import os
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from jobs.r2g.create_or_repair_table import create_or_repair_table
from libs_dag.table_creation_producer import TableCreationProducer
from urllib.parse import unquote
import socket
import time
import json
from libs_dag.logging_manager import LoggingManager, TaskRun, ExecutionLog, TableRun
from common.trino_table_manager import TrinoTableManager

logger = LoggerSimple.get_logger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
    'on_failure_callback': None,
    'provide_context': True
}


def log_execution_start(**context):
    logging_manager = LoggingManager()
    ti = context['ti']
    dag_run = context['dag_run']

    dag_id = ti.dag_id
    execution_date = ti.execution_date
    run_id = dag_run.run_id
    status = 'RUNNING'

    if isinstance(execution_date, pendulum.DateTime):
        execution_date = execution_date.in_timezone('UTC').replace(tzinfo=None)
    elif isinstance(execution_date, datetime):
        execution_date = execution_date.replace(tzinfo=None)

    execution_log = ExecutionLog(
        dag_id=dag_id,
        execution_date=execution_date,
        run_id=run_id,
        status=status,
    )

    execution_id = logging_manager.log_execution(execution_log)
    logger.info(
        f"Execution started for {dag_id} with execution_id: {execution_id}")

    ti.xcom_push(key='execution_id', value=execution_id)
    logger.info(f"Execution ID {execution_id} pushed to XCom")


def log_execution_end(**context):
    """
    Log the final execution status of the DAG run.
    """
    execution_id = context['ti'].xcom_pull(
        task_ids='log_execution_start', key='execution_id')
    logging_manager = LoggingManager()
    dag_run = context['dag_run']
    current_task = context['task']

    task_instances = [
        ti for ti in dag_run.get_task_instances()
        if ti.task_id != current_task.task_id
    ]

    has_failed = any(ti.state == 'failed' for ti in task_instances)
    still_running = any(
        ti.state in ('running', 'up_for_retry', 'up_for_reschedule')
        for ti in task_instances
    )
    all_others_success = all(
        ti.state == 'success' or ti.state == 'skipped'
        for ti in task_instances
    )

    logger.info(
        f"Task states summary - Failed: {has_failed}, Running: {still_running}, All Others Success: {all_others_success}")
    logger.info(f"Individual task states:")
    for ti in task_instances:
        logger.info(f"Task {ti.task_id}: {ti.state}")

    if has_failed:
        status = 'FAILED'
    elif all_others_success:
        status = 'SUCCESS'
    else:
        status = 'FAILED'
        logger.error(
            f"Unexpected state combination in DAG execution {execution_id}")
        logger.error(
            f"Task states: {[(ti.task_id, ti.state) for ti in task_instances]}")

    logging_manager.update_execution_status(execution_id, status)
    logger.info(f"Execution status for {execution_id} updated to: {status}")


def log_task_run_start(task_id: str, **context):
    execution_id = context['ti'].xcom_pull(
        task_ids='log_execution_start', key='execution_id')
    logging_manager = LoggingManager()
    dag_run_conf = context['dag_run'].conf or {}
    task_run = TaskRun(
        execution_id=execution_id,
        task_id=task_id,
        source_system=dag_run_conf.get('source_system', ''),
        database_name=dag_run_conf.get('database', ''),
        table_name=dag_run_conf.get('table', ''),
        start_time=datetime.utcnow(),
        status='RUNNING'
    )
    task_run_id = logging_manager.log_task_run(task_run)
    logger.info(f'Task ruin id {task_run_id}')
    context['ti'].xcom_push(key='task_run_id', value=task_run_id)


def log_task_run_end(task_id: str, start_task_id: str, **context):
    task_run_id = context['ti'].xcom_pull(
        task_ids=start_task_id, key='task_run_id')
    logger.info(f'Task run id {task_run_id}')

    logging_manager = LoggingManager()

    main_task_instance = context['dag_run'].get_task_instance(task_id)
    task_state = main_task_instance.current_state()

    status = 'SUCCESS' if task_state == 'success' else 'FAILED'

    if task_run_id:
        logging_manager.update_task_run(task_run_id, datetime.utcnow(), status)
    else:
        logger.error(f"Failed to retrieve task_run_id for task: {task_id}")


def process_config(config: dict, trino_table_manager: TrinoTableManager) -> None:
    """
    Process a single configuration to write data to the target location and create/repair Trino tables.
    """
    try:
        logger.info(f"Processing config: {config}")
        dataframe_name = config.get('dataframe')
        table_path = config.get('path')
        schema = config.get('schema')
        partition_by = config.get('partition_by', [])
        store_type = config.get('store_type', 'SCD1')

        path_parts = table_path.rstrip('/').split('/')

        logger.info(f"path_parts {path_parts}")
        if not all([dataframe_name, table_path, schema]):
            raise ValueError(
                "Missing required configuration parameters.")
        table_name = table_path.rstrip('/').split('/')[-1]
        logger.info(f"Table name: {table_name}")
        if store_type == 'SCD4':
            base_path = '/'.join(path_parts[:-1])
            table_name = path_parts[-2]
            logger.info(f"Table name1: {table_name}")
            table_path = table_path.replace('/current', '')
        logger.info(
            f"Creating/repairing table {table_name} with path {table_path}")
        results = trino_table_manager.create_or_repair_table(
            table_name=table_name,
            schema=schema,
            location=table_path,
            partition_by=partition_by,
            store_type=store_type
        )

        if results:
            logger.info(f"Successfully processed table {table_name}")
        else:
            logger.warning(
                f"Table {table_name} processing completed with warnings.")
    except Exception as e:
        logger.error(f"Error processing config: {config}, error: {str(e)}")
        raise


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
        ti.xcom_push(key='configs', value=dag_run_conf)
    except Exception as e:
        logger.error(f"Error extracting configuration: {str(e)}")
        raise


def execute_create_or_repair_tables(**context):
    """Create or repair Trino tables based on configuration"""
    try:
        ti = context.get('ti')
        config_file_path = ti.xcom_pull(
            task_ids="extract_r2g_configuration", key='table_name')
        logging_manager = LoggingManager()
        with open(f'/tmp/{config_file_path}_output.json', 'r') as f:
            configs = json.load(f)
        logger.info(f"configs2 {configs}")

        if not configs:
            raise ValueError("No configuration found from previous task.")
        execution_id = ti.xcom_pull(
            task_ids='log_execution_start', key='execution_id')
        task_run_id = ti.xcom_pull(
            task_ids='log_task_run_start_create_tables', key='task_run_id')
        logging = LoggingManager()
        trino_table_manager = TrinoTableManager()

        for output_config in configs.get('output_configs', []):
            table_path = output_config.get('path')
            logger.info(f"output config {output_config}")
            logger.info(f"table_path config {table_path}")
            path_parts = table_path.rstrip('/').split('/')
            table_name = path_parts[-1]
            start_time = datetime.utcnow()
            store_type = output_config.get('store_type', 'SCD1')
            partition_by = output_config.get('partition_by', [])
            status = 'RUNNING'

            if store_type == 'SCD4':
                table_name = path_parts[-2]

            table_run = TableRun(
                execution_id=execution_id,
                task_run_id=task_run_id,
                task_id='create_or_repair_tables_task',
                table_name=table_name,
                start_time=start_time,
                status=status,
            )
            table_run_id = logging_manager.log_table_run(table_run)

            try:
                process_config(output_config, trino_table_manager)
                status = 'SUCCESS'
            except Exception as e:
                status = 'FAILED'
                logger.error(f"Error processing table {table_name}: {str(e)}")

            end_time = datetime.utcnow()
            logging_manager.update_table_run(table_run_id, end_time, status)
    except Exception as e:
        logger.error(f"Error creating or repairing tables: {str(e)}")
        raise


def send_table_creation_event(**context):
    """
    Send table creation event to Kafka after successful table creation
    """
    try:
        ti = context['ti']
        dag_run_conf = context['dag_run'].conf or {}
        source_system = dag_run_conf.get('source_system')
        database = dag_run_conf.get('database')
        config_file_path = dag_run_conf.get('config_path')
        bucket_name = dag_run_conf.get('bucket_name')

        table_name = ti.xcom_pull(
            task_ids='extract_r2g_configuration', key='table_name')
        with open(f'/tmp/{table_name}_output.json', 'r') as f:
            configs = json.load(f)

        with TableCreationProducer() as producer:
            for output_config in configs.get('output_configs', []):
                logger.info(f"output_config {output_config}")
                table_path = output_config.get('path')
                path_parts = table_path.rstrip('/').split('/')
                logger.info(f"table table_path {table_path}")
                store_type = output_config.get('store_type', 'SCD1')
                table_name = output_config.get('path').rstrip('/').split('/')[-1]
                if store_type == 'SCD4':
                    table_name = path_parts[-2]
                logger.info(f"type {output_config.get('store_type')} table name {table_name}")
                if not all([source_system, database, table_name]):
                    logger.error(
                        "Missing required configuration for table creation event")
                    continue

                producer.send_table_completion_event(
                    source_system=source_system,
                    database=database,
                    table=table_name,
                    config_file_path=config_file_path,
                    bucket_name=bucket_name
                )
                logger.info(
                    f"Successfully sent table creation event for {source_system}.{database}.{table_name}")

    except Exception as e:
        logger.error(f"Failed to send table creation event: {str(e)}")
        raise


with DAG(
    'orchestration_r2g_wrapper',
    default_args=default_args,
    description='A DAG that wrap the spark job.',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1
) as dag:

    log_execution_start_task = PythonOperator(
        task_id='log_execution_start',
        python_callable=log_execution_start,
        dag=dag,
        provide_context=True
    )

    log_task_run_start_extract = PythonOperator(
        task_id='log_task_run_start_extract',
        python_callable=log_task_run_start,
        op_kwargs={'task_id': 'extract_r2g_configuration'},
        provide_context=True,
    )

    extract_config_task = PythonOperator(
        task_id='extract_r2g_configuration',
        python_callable=extract_conf,
        dag=dag,
        provide_context=True,
    )

    log_task_run_end_extract = PythonOperator(
        task_id='log_task_run_end_extract',
        python_callable=log_task_run_end,
        op_kwargs={
            'task_id': 'extract_r2g_configuration',
            'start_task_id': 'log_task_run_start_extract'
        },
        provide_context=True,
        trigger_rule='all_done',
    )

    log_task_run_start_spark = PythonOperator(
        task_id='log_task_run_start_spark',
        python_callable=log_task_run_start,
        op_kwargs={'task_id': 'generic_etl_r2g_module'},
        provide_context=True,
    )

    spark_r2g_job = BashOperator(
        task_id='generic_etl_r2g_module',
        bash_command=(
            'execution_id="{{ ti.xcom_pull(task_ids=\'log_execution_start\', key=\'execution_id\') }}" && '
            'task_run_id="{{ ti.xcom_pull(task_ids=\'log_task_run_start_spark\', key=\'task_run_id\') }}" && '
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
            '--source_system "$source_system" '
            '--execution_id "$execution_id" '
            '--task_run_id "$task_run_id"'
        ),
        do_xcom_push=True,
        dag=dag
    )

    log_task_run_end_spark = PythonOperator(
        task_id='log_task_run_end_spark',
        python_callable=log_task_run_end,
        op_kwargs={
            'task_id': 'generic_etl_r2g_module',
            'start_task_id': 'log_task_run_start_spark'
        },
        provide_context=True,
        trigger_rule='all_done',
    )

    log_task_run_start_create_tables = PythonOperator(
        task_id='log_task_run_start_create_tables',
        python_callable=log_task_run_start,
        op_kwargs={'task_id': 'create_or_repair_tables_task'},
        provide_context=True,
    )

    create_or_repair_tables_task = PythonOperator(
        task_id='create_or_repair_tables_task',
        python_callable=execute_create_or_repair_tables,
        provide_context=True,
    )

    send_table_event = PythonOperator(
        task_id='send_table_creation_event',
        python_callable=send_table_creation_event,
        provide_context=True,
        trigger_rule='all_success',
        dag=dag
    )

    log_task_run_start_send_event = PythonOperator(
        task_id='log_task_run_start_send_event',
        python_callable=log_task_run_start,
        op_kwargs={'task_id': 'send_table_creation_event'},
        provide_context=True,
    )

    log_task_run_end_send_event = PythonOperator(
        task_id='log_task_run_end_send_event',
        python_callable=log_task_run_end,
        op_kwargs={
            'task_id': 'send_table_creation_event',
            'start_task_id': 'log_task_run_start_send_event'
        },
        provide_context=True,
        trigger_rule='all_done',
    )

    log_task_run_end_create_tables = PythonOperator(
        task_id='log_task_run_end_create_tables',
        python_callable=log_task_run_end,
        op_kwargs={
            'task_id': 'create_or_repair_tables_task',
            'start_task_id': 'log_task_run_start_create_tables'
        },
        provide_context=True,
        trigger_rule='all_done',
    )

    log_execution_end_task = PythonOperator(
        task_id='log_execution_end',
        python_callable=log_execution_end,
        provide_context=True,
        trigger_rule='all_done',
        retries=0,
        dag=dag
    )

    log_execution_start_task >> log_task_run_start_extract >> extract_config_task >> log_task_run_end_extract
    log_task_run_end_extract >> log_task_run_start_spark >> spark_r2g_job >> log_task_run_end_spark
    log_task_run_end_spark >> log_task_run_start_create_tables >> create_or_repair_tables_task >> log_task_run_end_create_tables
    log_task_run_end_create_tables >> log_task_run_start_send_event >> send_table_event >> log_task_run_end_send_event >> log_execution_end_task
