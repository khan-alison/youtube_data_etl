import json
import pendulum
import uuid
from typing import Set, Dict, Any
from kafka import KafkaConsumer
from airflow.api.client.local_client import Client
from helper.logger import LoggerSimple
from dags.libs_dag.logging_manager import LoggingManager

logger = LoggerSimple.get_logger(__name__)


class DatasetEventsProcessor:
    """
    A processor that listens to Kafka messages for specified events and triggers Airflow DAGs
    when all required component datasets for a given dataset have been successfully completed.
    """

    def __init__(self, topic: str, bootstrap_servers: str, group_id: str, mapping_file: str):
        """
        Initialize the DatasetEventsProcessor.

        Args:
            topic: The Kafka topic to consume.
            bootstrap_servers: Kafka broker addresses.
            group_id: Consumer group ID.
            mapping_file: Path to the JSON file mapping events to datasets.
        """
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.mapping_file = mapping_file

        self.consumer = self._create_consumer()
        self.event_map = self._load_event_map()
        self.all_datasets = set(self.event_map.values())

        self.received_events: Dict[str, str] = {}
        self.processed_datasets: Set[str] = set()
        self.running = True
        self.logging_manager = LoggingManager()

    def _create_consumer(self) -> KafkaConsumer:
        """
        Create and return a Kafka consumer.
        """
        try:
            consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logger.info(f"Kafka consumer created for topic: {self.topic}")
            return consumer
        except Exception as e:
            logger.error(
                f"Failed to create Kafka consumer for topic {self.topic}: {e}", exc_info=True)
            raise

    def _load_event_map(self) -> Dict[str, str]:
        """
        Load the event-to-dataset mapping from the provided JSON file.
        """
        try:
            with open(self.mapping_file, 'r', encoding='utf-8') as f:
                event_map = json.load(f)
            logger.info(
                f"Event map loaded from {self.mapping_file}: {event_map}")
            return event_map
        except FileNotFoundError:
            logger.error(f"Mapping file not found at {self.mapping_file}")
            return {}
        except json.JSONDecodeError:
            logger.error(
                f"Invalid JSON in mapping file at {self.mapping_file}")
            return {}

    def _check_component_status(self, component_dataset: str) -> bool:
        """
        Check if a component dataset is successfully created.
        """
        try:
            status = self.logging_manager.get_dataset_status(
                dataset=component_dataset)
            logger.debug(
                f"Status check for component dataset '{component_dataset}': {status}")
            return status == 'SUCCESS'
        except Exception as e:
            logger.error(
                f"Error checking status for component dataset '{component_dataset}': {e}",
                exc_info=True
            )
            return False

    def _check_and_trigger(self):
        """
        Check if the received events match any key in the event_map and trigger the corresponding dataset DAG.
        After attempting to trigger all possible datasets, if all are processed, reset the state for next batch.
        """
        triggered_any = False

        for event_key, dataset_name in self.event_map.items():
            required_events = set(event_key.split(','))

            if not required_events.issubset(self.received_events):
                logger.debug(
                    f"Not all required events found for dataset '{dataset_name}'. "
                    f"Required events: {required_events}, Received events: {self.received_events.keys()}"
                )
                continue

            if dataset_name in self.processed_datasets:
                logger.debug(
                    f"Dataset '{dataset_name}' already processed, skipping.")
                continue

            if self._all_components_ready(required_events):
                self.processed_datasets.add(dataset_name)
                self._trigger_dag(dataset_name=dataset_name)
                triggered_any = True
            else:
                logger.debug(
                    f"Some components for dataset '{dataset_name}' are not ready. "
                    "Will not trigger DAG at this time."
                )

        if self.processed_datasets == self.all_datasets:
            logger.info(
                "All datasets in the event map have been processed. Resetting state for next batch.")
            self.processed_datasets.clear()
            self.received_events.clear()

    def _all_components_ready(self, required_events: Set[str]) -> bool:
        """
        Check if all component datasets corresponding to the required events are successfully created.
        """
        component_statuses = {}
        for event in required_events:
            component_dataset = self.received_events[event]
            status = self._check_component_status(component_dataset)
            component_statuses[component_dataset] = status

        if all(component_statuses.values()):
            logger.info(
                f"All component datasets ready: {list(component_statuses.keys())}."
            )
            return True
        else:
            failed_components = [
                ds for ds, status in component_statuses.items() if not status]
            logger.warning(
                f"Not all components are ready. Failed components: {failed_components}"
            )
            return False

    def _trigger_dag(self, dataset_name: str):
        """
        Trigger the Airflow DAG for the given dataset.
        """
        dag_conf = {
            'dataset': dataset_name,
            'config_path': f"/opt/airflow/job_entries/g2i/{dataset_name}/config.json"
        }
        run_id = f"{dataset_name}_{pendulum.now().int_timestamp}_{uuid.uuid4().hex}"

        try:
            client = Client(None)
            logger.info(
                f"Triggering DAG 'orchestration_g2i_wrapper' for dataset '{dataset_name}' "
                f"with conf: {dag_conf} and run_id: {run_id}"
            )
            client.trigger_dag(
                dag_id='orchestration_g2i_wrapper',
                conf=dag_conf,
                run_id=run_id
            )
            logger.info(
                f"Successfully triggered DAG for dataset: {dataset_name}")
        except Exception as e:
            logger.error(
                f"Failed to trigger DAG for dataset '{dataset_name}': {e}",
                exc_info=True
            )

    def process_messages(self):
        """
        Process incoming messages from the Kafka topic until stopped.
        """
        logger.info(f"Starting message processing from topic: {self.topic}")

        try:
            for message in self.consumer:
                if not self.running:
                    logger.info(
                        "Stop signal received. Stopping message processing.")
                    break
                self._handle_message(message.value)
        except Exception as e:
            logger.error(
                f"Catastrophic error in message processing: {e}", exc_info=True
            )
        finally:
            self._cleanup()

    def _handle_message(self, event_data: Any):
        """
        Handle a single message from Kafka.
        """
        try:
            self._process_single_message(event_data)
        except ValueError as ve:
            logger.error(f"Validation error: {ve}")
        except Exception as e:
            logger.error(
                f"Error processing message: {e}", exc_info=True
            )

    def _process_single_message(self, event_data: Any):
        """
        Process a single message with validation and trigger checks.
        """
        if not isinstance(event_data, dict):
            raise ValueError("Invalid message format: Expected a dictionary.")

        finish_event = event_data.get('finish_event')
        dataset = event_data.get('dataset')

        if not finish_event:
            raise ValueError("Missing 'finish_event' in message.")
        if not dataset:
            raise ValueError("Missing 'dataset' in message.")

        self.received_events[finish_event] = dataset
        logger.info(
            f"Received event '{finish_event}' with component dataset '{dataset}'."
        )

        self._check_and_trigger()

    def _cleanup(self):
        """
        Perform cleanup operations.
        """
        try:
            if self.consumer:
                self.consumer.close()
                logger.info("Kafka consumer closed successfully.")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}", exc_info=True)

    def stop(self):
        """
        Stop the consumer loop.
        """
        self.running = False
        logger.info(
            "Stop method called. Consumer will stop after current processing completes.")
