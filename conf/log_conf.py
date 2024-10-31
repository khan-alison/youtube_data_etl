from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
import os

LOGGING_CONFIG = DEFAULT_LOGGING_CONFIG

# Customize the logging configuration
LOGGING_CONFIG['handlers']['task']['base_log_folder'] = '/opt/airflow/logs'
LOGGING_CONFIG['handlers']['task'][
    'filename_template'] = '{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log'
