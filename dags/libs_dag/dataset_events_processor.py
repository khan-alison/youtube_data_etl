from kafka import KafkaConsumer
from airflow.api.client.local_client import Client
import json
import pendulum
import signal
from helper.logger import LoggerSimple
from dags.libs_dag.logging_manager import LoggingManager

logger = LoggerSimple.get_logger(__name__)


class DatasetEventsProcessor:
    def __init__(self, topic: str, bootstrap_servers: str, group_id: str, control_file_path: str):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.control_file_path = control_file_path
        self.consumer = self._create_consumer()
        self.analytical_datasets = self._load_control_file()
        self.logging_manager = LoggingManager()
        self.processed_analytical_datasets = set()
        self.total_analytical_datasets = len(self.analytical_datasets)
        self.running = True

    def _create_consumer(self):
        try:
            consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logger.info(
                f"Successfully created Kafka consumer for topic: {self.topic}")
            return consumer
        except Exception as e:
            logger.error(f"Failed to create Kafka consumer: {str(e)}")
            raise

    def _load_control_file(self):
        """
        Load the control file containing analytical datasets and their dependencies.
        Previously referenced 'tables', now changed to 'datasets'.
        """
        try:
            with open(self.control_file_path, 'r') as f:
                control_data = json.load(f)
            datasets = control_data.get('datasets', {})
            if not datasets:
                logger.info("No datasets found in control file")
            return datasets
        except FileNotFoundError:
            logger.error(f"Control file not found at {self.control_file_path}")
            return {}
        except json.JSONDecodeError:
            logger.error("Invalid JSON in control file")
            return {}

    def _check_dependencies_met(self, dependencies):
        """
        Check if all component datasets in 'dependencies' have status 'SUCCESS' in the logging system.
        """
        for dataset in dependencies:
            status = self.logging_manager.get_dataset_status(dataset=dataset)
            if status != 'SUCCESS':
                logger.info(
                    f"Dataset {dataset} status is {status}. Dependencies not met.")
                return False
        logger.info("All dependencies have status 'SUCCESS'")
        return True

    def _trigger_dag(self, dataset: str):
        """
        Trigger a downstream DAG (e.g., orchestration_g2i_wrapper) once all dependencies for the dataset are met.
        """
        #TODO rename create_a... -> g2i
        dag_conf = {
            'dataset': dataset,
            'config_path': f'/opt/airflow/jobs_entries/create_analysis_dataset/{dataset}/config.json'
        }
        client = Client(None)
        run_id = f"create_analysis_dataset_{dataset}_{pendulum.now().int_timestamp}"

        logger.info(
            f"👺Triggering orchestration_g2i_wrapper DAG with config: {dag_conf}")
        client.trigger_dag(
            dag_id='create_analysis_dataset',
            conf=dag_conf,
            run_id=run_id
        )
        logger.info(f"Successfully triggered G2I DAG for {dataset}")

    def process_messages(self):
        logger.info(f"Starting to process messages from topic: {self.topic}")
        logger.info(
            f"Current processed analytical datasets: {self.processed_analytical_datasets}")

        try:
            while self.running:
                for message in self.consumer:
                    if not self.running:
                        break
                    event_data = message.value
                    logger.info(f"Received event data: {event_data}")

                    component_dataset_name = event_data.get('dataset')
                    if not component_dataset_name:
                        logger.error("Missing 'dataset' in event data")
                        continue

                    for analytical_dataset_name, config in self.analytical_datasets.items():
                        if analytical_dataset_name in self.processed_analytical_datasets:
                            logger.info(
                                f"Analytical dataset {analytical_dataset_name} has already been processed; skipping.")
                            continue

                        dependencies = config.get(
                            'dependencies', {}).get('r2g_datasets', [])

                        if component_dataset_name in dependencies:
                            logger.info(
                                f"Component dataset {component_dataset_name} is a dependency of analytical dataset {analytical_dataset_name}")

                            if self._check_dependencies_met(dependencies):
                                self._trigger_dag(analytical_dataset_name)
                                self.processed_analytical_datasets.add(
                                    analytical_dataset_name)
                                logger.info(
                                    f"Analytical dataset {analytical_dataset_name} has been processed and added to the processed list.")
                            else:
                                logger.info(
                                    f"Dependencies not met for analytical dataset {analytical_dataset_name}")
                        else:
                            logger.debug(
                                f"Component dataset {component_dataset_name} is not a dependency of analytical dataset {analytical_dataset_name}")

                    if len(self.processed_analytical_datasets) == self.total_analytical_datasets:
                        logger.info(
                            "All analytical datasets have been processed. Starting new batch.")
                        self.processed_analytical_datasets.clear()
        except Exception as e:
            logger.error(f"Error in consumer: {str(e)}")
        finally:
            try:
                self.consumer.close()
                logger.info("Consumer closed successfully")
            except Exception as e:
                logger.error(f"Error closing consumer: {str(e)}")

    def stop(self):
        self.running = False
