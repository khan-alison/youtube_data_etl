import json
import os
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import googleapiclient.discovery
from kafka.errors import TopicAlreadyExistsError


def main():
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    DEVELOPER_KEY = "AIzaSyDXd_5M63YPHqyRoPyDZMwi1ME4u_LIIOI"

    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=DEVELOPER_KEY)

    # request = youtube.commentThreads().list(
    #     part="snippet, replies",
    #     videoId="q8q3OFFfY6c"
    # )
    # response = request.execute()

    request = youtube.channels().list(
        part="snippet,statistics",
        id="UC_x5XG1OV2P6uZZ5FSM9Ttw"
    )
    response = request.execute()

    pretty_response = json.dumps(response, indent=4)
    print(pretty_response)

    send_data_to_kafka(response)


def create_kafka_topic(topic_name, bootstrap_servers='localhost:9092'):
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id='test_client')

    topic_list = [
        NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic '{topic_name}' created.")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic_name}' already exists.")


def send_data_to_kafka(data, topic_name='youtube-data', bootstrap_servers='localhost:9092'):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    producer.send(topic_name, data)
    producer.flush()
    print(f"Data sent to Kafka topic '{topic_name}'.")


if __name__ == "__main__":
    create_kafka_topic("youtube-data")
    main()
    consumer = KafkaConsumer(
        'youtube-data',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    # Print messages as they are consumed
    for message in consumer:
        print(f"Received message: {message.value}")
        break
