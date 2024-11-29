import os
import pyarrow.parquet as pq
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from airflow.hooks.base_hook import BaseHook
import json
from kafka import KafkaConsumer
import pandas as pd

def empty_files(file_paths):
    for file in file_paths:
        if os.path.exists(file):
            try:
                open(file, 'w').close()
                print(f"Emptied file: {file}")
            except OSError as e:
                print(f"Error while emptying {file}: {e}")
        else:
            print(f"File does not exist: {file}")

def convert_duration(duration):
    if isinstance(duration, str) and ':' in duration:
        minutes, seconds = duration.split(':')
        total_seconds = int(minutes) * 60 + float(seconds)
    elif isinstance(duration, str):
        total_seconds = float(duration)  
    else:
        total_seconds = None
    
    return total_seconds

def create_kafka_producer():
    kafka_conn_id = 'kafka'  
    kafka_conn = BaseHook.get_connection(kafka_conn_id)
    bootstrap_servers = f"{kafka_conn.host}:{kafka_conn.port}"

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda x: json.dumps(x).encode('utf-8') if x else None,  # Ensure `x` is handled properly
        key_serializer=lambda k: k.encode('utf-8') if k is not None else None    # Ensure `k` is handled properly
    )

    return producer  # Only one return statement


def ensure_topic_exists(topic_name, bootstrap_servers):

    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    existing_topics = admin_client.list_topics()
    if topic_name not in existing_topics:
        topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([topic])
        print(f"Created topic: {topic_name}")
    else:
        print(f"Topic {topic_name} already exists.")

def consume_function(message):
    """Process each consumed message from Kafka."""
    if message:
        try:
            message_content = json.loads(message.value())
            print(f"Consumed message: {message_content}")
            return message_content  # Return the valid message
        except Exception as e:
            print(f"Failed to parse message: {e}")
            return None  # Or handle the failure case appropriately
    else:
        print("Received empty or None message.")
        return None

def time_to_milliseconds(time_str):
  
    minutes, seconds = time_str.split(':')
    seconds, milliseconds = seconds.split('.')

    minutes = int(minutes)
    seconds = int(seconds)
    milliseconds = int(milliseconds)

    total_milliseconds = (minutes * 60 * 1000) + (seconds * 1000) + milliseconds
    return total_milliseconds

def duration_to_milliseconds(duration: str) -> int:
    try:
        milliseconds = int(float(duration) * 1000)
        return milliseconds
    except ValueError:
        raise ValueError(f"Invalid duration format: {duration}")