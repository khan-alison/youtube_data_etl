from kafka import KafkaProducer
import json
import pendulum
from helper.logger import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


class TableCompletionProducer(Producer):
    def __init__(self, bootstrap_servers: str = 'kafka:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        self.topic = 
