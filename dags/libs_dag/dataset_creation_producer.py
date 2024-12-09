from kafka import KafkaProducer
import json
import pendulum
from helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


class DatasetCreationProducer:
    def __init__(self, bootstrap_servers: str = 'kafka:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.topic = 'dataset_creation_events'
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
            self.producer[[]].flush()
            self.producer.close()
            logger.info("Producer closed")

    def send_dataset_completion_events(self, dataset: str, source_system: str = None,
                                       database: str = None, config_file_path: str = None,
                                       bucket_name: str = None, status: str = 'COMPLETED'):
        """
        Send a table completion event with source system information
        """
        try:
            event = {
                'dataset': dataset,
                'status': status,
                'source_system': source_system,
                'database': database,
                'config_file_path': config_file_path,
                'bucket_name': bucket_name,
                'timestamp': pendulum.now().isoformat()
            }
            logger.info(f"Sending dataset completion event: {event}")
            self.producer.send(self.topic, value=event)
            logger.info(
                f"Successfully sent dataset completion event for {source_system}.{database}.{dataset}")
        except Exception as e:
            logger.error(f"Error sending table completion event: {str(e)}")
            raise