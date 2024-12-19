from kafka import KafkaConsumer
from airflow.api.client.local_client import Client
import json
import pendulum
from helper.logger import LoggerSimple
from urllib.parse import unquote
from common.config_manager import ConfigManager


logger = LoggerSimple.get_logger(__name__)


class KafkaMinIOEventsProcessor:
    def __init__(self, topic: str, bootstrap_servers: str, group_id: str):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.consumer = self._create_consumer()

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

    def _trigger_orchestration_r2g_wrapper_dag(self, bucket_name: str, object_key: str, event_name: str):
        try:
            decoded_key = unquote(object_key)
            path_parts = decoded_key.split('/')
            if len(path_parts) >= 3:
                source_system = path_parts[0]
                database = path_parts[1]
                table = path_parts[2]

                config_manager = ConfigManager(
                    control_file_path=decoded_key,
                    bucket_name=bucket_name,
                    process_type='r2g'
                )

                try:
                    config_path = ConfigManager.get_config_path(
                        table_name=table,
                        process_type='r2g'
                    )

                    conf = {
                        "bucket_name": bucket_name,
                        "object_key": decoded_key,
                        "event_name": event_name,
                        "trigger_time": pendulum.now().isoformat(),
                        "source_system": source_system,
                        "database": database,
                        "table": table,
                        "config_path": config_path
                    }

                    logger.info(f"Triggering DAG with config: {conf}")

                    client = Client(None)
                    client.trigger_dag(
                        dag_id='orchestration_r2g_wrapper',
                        conf=conf,
                        run_id=f"triggered_from_kafka_{pendulum.now().int_timestamp}"
                    )
                    return True
                except FileNotFoundError as e:
                    logger.error(
                        f"Configuration file not found for {table}: {str(e)}")
                    return False
                except ValueError as e:
                    logger.error(
                        f"Invalid configuration for {table}: {str(e)}")
                    return False
            else:
                logger.error(
                    f"Invalid object key structure: {decoded_key}. "
                    "Expected at least 3 path components."
                )
                return False
        except Exception as e:
            logger.error(f"Failed to trigger DAG: {str(e)}")
            return False

    def _trigger_orchestration_g2i_wrapper_dag(self, bucket_name: str, object_key: str, event_name: str):
        """
        Trigger DAG for creating tables in Trino based on objects in the 'golden' zone.
        """
        try:
            decoded_key = unquote(object_key)
            path_parts = decoded_key.split('/')
            if 'golden' in decoded_key and len(path_parts) >= 3:
                dataset_name = path_parts[-1].split('.')[0]
                conf = {
                    "bucket_name": bucket_name,
                    "object_key": decoded_key,
                    "event_name": event_name,
                    "trigger_time": pendulum.now().isoformat(),
                    "dataset_name": dataset_name,
                }

                logger.info(
                    f"Triggering orchestration_g2i_wrapper DAG with config: {conf}")

                client = Client(None)
                client.trigger_dag(
                    dag_id='orchestration_g2i_wrapper',
                    conf=conf,
                    run_id=f"g2i_trigger_{pendulum.now().int_timestamp}"
                )
                return True
            else:
                logger.error(
                    f"Object key does not meet requirements for golden zone: {decoded_key}"
                )
                return False
        except Exception as e:
            logger.error(f"Failed to trigger DAG: {str(e)}")
            return False

    def process_messages(self):
        logger.info(f"Starting to process messages from topic: {self.topic}")

        try:
            for message in self.consumer:
                try:
                    data = message.value
                    event_name = data.get("EventName", "Unknown")
                    records = data.get("Records", [{}])[0]
                    object_key = records.get("s3", {}).get(
                        "object", {}).get("key", "Unknown")
                    bucket_name = records.get("s3", {}).get(
                        "bucket", {}).get("name", "Unknown")

                    logger.info(
                        f"Received event - Name: {event_name}, Object: {object_key}")

                    if '_delta_log' in object_key:
                        logger.info(f"Skipping delta log file: {object_key}")
                        continue

                    if event_name == "s3:ObjectCreated:Put":
                        if 'raw' in object_key and object_key.endswith('.json'):
                            self._trigger_orchestration_r2g_wrapper_dag(
                                bucket_name, object_key, event_name)
                        else:
                            logger.info(
                                f"Skipping event {event_name} for object {object_key}")
                    else:
                        logger.info(
                            f"Unhandled event {event_name} for object {object_key}")
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")
                    continue

        except Exception as e:
            logger.error(f"Fatal error in message processing: {str(e)}")
            raise
        finally:
            self.consumer.close()
            logger.info("Kafka consumer closed")
