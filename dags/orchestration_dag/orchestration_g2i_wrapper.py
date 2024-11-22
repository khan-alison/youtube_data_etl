from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import trino
from urllib.parse import unquote
from helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}


def extract_conf(**context):
    """
    Extract configuration from DAG run context and validate it
    """
    try:
        dag_run_conf = context['dag_run'].conf
        if not dag_run_conf:
            raise ValueError('No configuration provided in DAG trigger. 💩💩')
        ti = context['ti']
        output_configs = dag_run_conf.get('output_configs')
        if not output_configs:
            raise ValueError(
                "No configuration found in DAG run configuration.")
        ti.xcom_push(key='output_configs',
                     value=output_configs)
        logger.info(f"Processing configuration: {dag_run_conf}")
    except Exception as e:
        logger.error(f"Failed to extract configuration: {str(e)}")
        raise


def test_trino_connection():
    """Test Trino connection and run a simple query"""
    try:
        conn = trino.dbapi.connect(
            host='yt-trino',
            port='8080',
            user='trino',
            catalog='minio',
            schema='youtube'
        )

        cursor = conn.cursor()

        queries = [
            "SHOW CATALOGS",
            "SHOW SCHEMAS FROM minio"
        ]

        for query in queries:
            logger.info(f"Executing query: {query}")
            cursor.execute(query)
            rows = cursor.fetchall()
            logger.info(f"Results: {rows}")
        logger.info("Trino connection test completed successfully!")
    except Exception as e:
        logger.error(f"Failed to connect to Trino: {str(e)}")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


def process_tables(**context):

    output_configs = context['ti'].xcom_pull(
        task_ids='extract_g2i_configuration', key='output_configs')

    for config in output_configs:
        logger.info(f"Conf {config}")


with DAG(
    'orchestration_g2i_wrapper',
    default_args=default_args,
    description='Orchestrate golden data to generate insight from data.',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1
) as dag:

    extract_config_task = PythonOperator(
        task_id='extract_g2i_configuration',
        python_callable=extract_conf,
        dag=dag,
        provide_context=True
    )

    # test_connection = PythonOperator(
    #     task_id='test_trino_connection',
    #     python_callable=test_trino_connection,
    #     dag=dag
    # )

    create_or_repair_tables = BashOperator(
        task_id="create_or_repair_tables_task",
        bash_command=(
            'echo \'{{ ti.xcom_pull(task_ids="extract_g2i_configuration", key="output_configs") | tojson }}\' > /tmp/configs.json && '
            'spark-submit '
            '--master spark://spark-master:7077 '
            '--jars /opt/airflow/jars/aws-java-sdk-bundle-1.12.316.jar,'
            '/opt/airflow/jars/delta-core_2.12-2.4.0.jar,'
            '/opt/airflow/jars/delta-storage-2.3.0.jar,'
            '/opt/airflow/jars/hadoop-aws-3.3.4.jar,'
            '/opt/airflow/jars/hadoop-common-3.3.4.jar '
            '/opt/airflow/jobs/g2i/create_or_repair_table.py '
            '--configs "$(cat /tmp/configs.json)"'
        )
    )

    extract_config_task >> create_or_repair_tables
