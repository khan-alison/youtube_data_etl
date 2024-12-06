from kafka import KafkaConsumer
from airflow.api.client.local_client import Client
import json
import pendulum
import signal
from helper.logger import LoggerSimple
from dags.libs_dag.logging_manager import LoggingManager

logger = LoggerSimple.get_logger(__name__)


class TableEventsProcessor:
    def __init__(self, topic: str, bootstrap_servers: str, group_id: str, control_file_path: str):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.control_file_path = control_file_path
        self.consumer = self._create_consumer()
        self.analytical_tables = self._load_control_file()
        self.logging_manager = LoggingManager()
        self.processed_analytical_tables = set()
        self.total_analytical_tables = len(self.analytical_tables)
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
        try:
            with open(self.control_file_path, 'r') as f:
                control_data = json.load(f)
            tables = control_data.get('tables', {})
            if not tables:
                logger.info("No tables found in control file")
            return tables
        except FileNotFoundError:
            logger.error(f"Control file not found at {self.control_file_path}")
            return {}
        except json.JSONDecodeError:
            logger.error("Invalid JSON in control file")
            return {}

    def _check_dependencies_met(self, dependencies):
        """
        Check if all component tables in 'dependencies' have status 'SUCCESS' in the logging system.
        """
        for table in dependencies:
            status = self.logging_manager.get_table_status(table_name=table)
            if status != 'SUCCESS':
                logger.info(
                    f"Table {table} status is {status}. Dependencies not met.")
                return False
        logger.info("All dependencies have status 'SUCCESS'")
        return True

    def _trigger_dag(self, table_name: str):
        dag_conf = {
            'table_name': table_name,
            'config_path': f'/opt/airflow/jobs/g2i/{table_name}.json'
        }
        client = Client(None)
        run_id = f"g2i_{table_name}_{pendulum.now().int_timestamp}"

        logger.info(
            f"👺Triggering orchestration_g2i_wrapper DAG with config: {dag_conf}")
        client.trigger_dag(
            dag_id='orchestration_g2i_wrapper',
            conf=dag_conf,
            run_id=run_id
        )
        logger.info(f"Successfully triggered G2I DAG for {table_name}")

    def process_messages(self):
        logger.info(f"Starting to process messages from topic: {self.topic}")
        logger.info(f"tablesssss {self.processed_analytical_tables}")
        try:
            while self.running:
                for message in self.consumer:
                    if not self.running:
                        break
                    event_data = message.value
                    logger.info(f"Received event data: {event_data}")

                    component_table_name = event_data.get('table_name')
                    if not component_table_name:
                        logger.error("Missing 'table_name' in event data")
                        continue

                    for analytical_table_name, config in self.analytical_tables.items():
                        if analytical_table_name in self.processed_analytical_tables:
                            logger.info(
                                f"Analytical table {analytical_table_name} has already been processed; skipping.")
                            continue
                        dependencies = config.get(
                            'dependencies', {}).get('r2g_tables', [])
                        if component_table_name in dependencies:
                            logger.info(
                                f"Component table {component_table_name} is a dependency of analytical table {analytical_table_name}")

                            if self._check_dependencies_met(dependencies):
                                self._trigger_dag(analytical_table_name)
                                self.processed_analytical_tables.add(
                                    analytical_table_name)
                                logger.info(
                                    f"Analytical table {analytical_table_name} has been processed and added to the processed list.")
                            else:
                                logger.info(
                                    f"Dependencies not met for analytical table {analytical_table_name}")
                        else:
                            logger.debug(
                                f"Component table {component_table_name} is not a dependency of analytical table {analytical_table_name}")
                    if len(self.processed_analytical_tables) == self.total_analytical_tables:
                        logger.info(
                            "All analytical tables have been processed. Starting new batch.")
                        self.processed_analytical_tables.clear()
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
