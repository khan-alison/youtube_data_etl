from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from helper.logger import LoggerSimple
from common.spark_session import SparkSessionManager
from libs_dag.logging_manager import LoggingManager 
import logging

logger = LoggerSimple.get_logger(__name__)

default_args = {
    'depends_on_past': False,
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}


def check_dependencies_for_video_performance(**context):
    """Check if required dependencies are ready based on logging system"""
    try:
        logging_manager = LoggingManager()
        execution_date = context['execution_date']

        required_tables = [
            ('youtube', 'trending', 'trending_videos'),
            ('youtube', 'trending', 'channels_information'),
            ('youtube', 'trending', 'comment_threads'),
            ('youtube', 'trending', 'replies')
        ]

        with logging_manager.db.get_connection() as conn:
            cursor = conn.cursor()
            for source_system, database, table in required_tables:

                query = """
                    SELECT status 
                    FROM task_runs 
                    WHERE source_system = %s 
                    AND database_name = %s 
                    AND table_name = %s 
                    AND DATE(start_time) = DATE(%s)
                    AND status = 'SUCCESS'
                    ORDER BY end_time DESC 
                    LIMIT 1
                """
                cursor.execute(
                    query, (source_system, database, table, execution_date))
                result = cursor.fetchone()

                if not result:
                    logger.info(
                        f"Dependency not met: {source_system}.{database}.{table}")
                    return 'notify_missing_dependencies'

        logger.info("All dependencies are met")
        return 'process_video_performance'

    except Exception as e:
        logger.error(f"Error checking dependencies: {str(e)}")
        return 'notify_missing_dependencies'


def notify_missing_dependencies(**context):
    """Handle the case when dependencies are not ready"""
    logger.warning("One or more required source tables are not ready")
    return


with DAG(
    'orchestration_g2i_wrapper',
    default_args=default_args,
    description='Orchestrate golden data to generate insight from data.',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1
) as dag:

    check_dependencies = BranchPythonOperator(
        task_id='check_dependencies_for_video_performance',
        python_callable=check_dependencies_for_video_performance,
    )

    process_video_performance = BashOperator(
        task_id='process_video_performance',
        bash_command='python /opt/airflow/jobs/g2i/{video_performance}.py '
                     '--batch_run_timestamp="{{ ts_nodash }}" '
                     '--current_date="{{ ds_nodash }}"'
    )

    notify_missing_dependencies_task = PythonOperator(
        task_id='notify_missing_dependencies',
        python_callable=notify_missing_dependencies,
    )

    check_dependencies >> [process_video_performance,
                           notify_missing_dependencies_task]
