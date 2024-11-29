from datetime import datetime
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.base import BaseSensorOperator
from kafka import KafkaConsumer

class CustomKafkaSensor(BaseSensorOperator):
    def __init__(self, topic, bootstrap_servers, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers

    def poke(self, context):
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        for message in consumer:
            self.log.info(f"Message received: {message.value}")
            return True
        return False

@dag(start_date=datetime(2023, 11, 1), schedule="@daily", catchup=False, dag_id="kafka_listener_dag")
def listener_dag():

    start = DummyOperator(task_id='start_listening')


    kafka_sensor_task = CustomKafkaSensor(
        task_id="custom_kafka_sensor",
        topic="data_topic",
        bootstrap_servers="kafka:9092",
        poke_interval=10,
        timeout=300,
    )


    trigger_consumer_dag = TriggerDagRunOperator(
        task_id='trigger_consumer_dag',
        trigger_dag_id='kafka_consumer_dag',  
        wait_for_completion=True  
    )


    start >> kafka_sensor_task >> trigger_consumer_dag

listener_dag()
