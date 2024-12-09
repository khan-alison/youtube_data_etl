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


def extract_conf(**context):
    """
    Extract configuration parameters from the provided context.
    """
    try:
        ti = context['ti']
        dag_run_conf = context['dag_run'].conf
        if not dag_run_conf:
            raise ValueError('No configuration provided in DAG trigger.')
        dataset_name = dag_run_conf.get('dataset')
        config_path = dag_run_conf.get('config_path')
        if not dataset_name:
            raise ValueError('No dataset provided in DAG configuration.')
        logger.info(f"Processing configuration: {dag_run_conf}")
        ti.xcom_push(key='config_path', value=config_path)
        return dataset_name
    except Exception as e:
        logger.error(f"Error extracting configuration: {str(e)}")
        raise


with DAG(
    'create_analysis_dataset',
    default_args=default_args,
    description='Create analysis dataset.',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1
) as dag:

    extract_config_task = PythonOperator(
        task_id='extract_g2i_configuration',
        python_callable=extract_conf,
    )

    spark_jobs = BashOperator(
        task_id='analysis_dataset',
        bash_command=(
            'dataset_name="{{ ti.xcom_pull(task_ids=\'extract_g2i_configuration\') }}" && '
            'config_path="{{ ti.xcom_pull(task_ids=\'extract_g2i_configuration\', key=\'config_path\') }}" && '
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
            '/opt/airflow/jobs/create_analysis_dataset/${dataset_name}.py --dataset_name "${dataset_name}" --config_path "${config_path}"'
        ),
        dag=dag
    )

    extract_config_task >> spark_jobs
