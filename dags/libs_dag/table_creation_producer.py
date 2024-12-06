from kafka import KafkaProducer
import json
import pendulum
from helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


class TableCreationProducer:
    def __init__(self, bootstrap_servers: str = 'kafka:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.topic = 'table_creation_events'
        self.producer = None

    def __enter__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            retries=5
        )
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Producer closed")

    def send_table_completion_event(self, source_system: str, database: str, table: str,
                                    config_file_path: str, bucket_name: str, status: str = 'COMPLETED'):
        """
        Send a table completion event with source system information
        """
        try:
            event = {
                'source_system': source_system,
                'database': database,
                'table_name': table,
                'status': status,
                'timestamp': pendulum.now().isoformat(),
                'control_file_path': config_file_path,
                'bucket_name': bucket_name
            }
            logger.info(f"Sending table completion event: {event}")
            self.producer.send(self.topic, value=event)
            logger.info(
                f"Successfully sent table completion event for {source_system}.{database}.{table}")
        except Exception as e:
            logger.error(f"Error sending table completion event: {str(e)}")
            raise
