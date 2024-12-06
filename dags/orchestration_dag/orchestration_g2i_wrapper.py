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


def extract_conf(**kwargs):
    """
    Extract configuration parameters from the provided context.
    """
    try:
        dag_run_conf = kwargs['dag_run'].conf
        if not dag_run_conf:
            raise ValueError('No configuration provided in DAG trigger.')
        table_name = dag_run_conf.get('table_name')
        if not table_name:
            raise ValueError('No table_name provided in DAG configuration.')
        logger.info(f"Processing configuration: {dag_run_conf}")
        return table_name
    except Exception as e:
        logger.error(f"Error extracting configuration: {str(e)}")
        raise


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
    )

    spark_jobs = BashOperator(
        task_id='analysis_table',
        bash_command=(
            'table_name="{{ ti.xcom_pull(task_ids=\'extract_g2i_configuration\') }}" && '
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
            '/opt/airflow/jobs/g2i/${table_name}.py --table_name "${table_name}"'
        ),
        dag=dag
    )

    extract_config_task >> spark_jobs
