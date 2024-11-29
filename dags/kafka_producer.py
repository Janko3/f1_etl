
from airflow.decorators import task, dag
from airflow.providers.http.operators.http import HttpOperator
from datetime import datetime
from airflow.hooks.base_hook import BaseHook
import sys
sys.path.append('/opt/airflow/dags/programs/utils')
import utils
import requests

base_url = "https://ergast.com/api/f1"
kafka_id = 'kafka'
topic = 'data_topic'

@task
def send_driver_data_to_kafka(driver_data):
    kafka_conn_id = kafka_id
    kafka_conn = BaseHook.get_connection(kafka_conn_id)
    bootstrap_servers = f"{kafka_conn.host}:{kafka_conn.port}"

    topic_name = topic
    utils.ensure_topic_exists(topic_name, bootstrap_servers)

    producer = utils.create_kafka_producer()
    message_key = "drivers" 
    message_value = driver_data
    producer.send(topic_name,key = message_key,value=message_value)
    producer.flush()
    print("Driver Data Sent and Flushed")

@task
def send_circuit_data_to_kafka(circuit_data):
    kafka_conn_id = kafka_id
    kafka_conn = BaseHook.get_connection(kafka_conn_id)
    bootstrap_servers = f"{kafka_conn.host}:{kafka_conn.port}"

    topic_name = topic
    utils.ensure_topic_exists(topic_name, bootstrap_servers)

    producer = utils.create_kafka_producer()
    message_key = "circuits" 
    message_value = circuit_data
    producer.send(topic_name,key = message_key,value=message_value)
    producer.flush()
    print("Circuit Data Sent and Flushed")

@task
def send_constructor_data_to_kafka(constructor_data):
    kafka_conn_id = kafka_id
    kafka_conn = BaseHook.get_connection(kafka_conn_id)
    bootstrap_servers = f"{kafka_conn.host}:{kafka_conn.port}"
    
    topic_name = topic
    utils.ensure_topic_exists(topic_name,bootstrap_servers)

    producer = utils.create_kafka_producer()
    message_key = "constructors" 
    message_value = constructor_data
    producer.send(topic_name,key = message_key,value=message_value)
    producer.flush()
    print("Constructor Data Sent and Flushed")

@task
def send_status_data_to_kafka(status_data):
    kafka_conn_id = kafka_id
    kafka_conn = BaseHook.get_connection(kafka_conn_id)
    bootstrap_servers = f"{kafka_conn.host}:{kafka_conn.port}"

    topic_name = topic
    utils.ensure_topic_exists(topic_name,bootstrap_servers)

    producer = utils.create_kafka_producer()
    message_key = "status" 
    message_value = status_data
    producer.send(topic_name,key = message_key,value=message_value)
    producer.flush()
    print("Status Data Sent and Flushed")


@task
def send_races_data_to_kafka(race_data):
    kafka_conn_id = kafka_id
    kafka_conn = BaseHook.get_connection(kafka_conn_id)
    bootstrap_servers = f"{kafka_conn.host}:{kafka_conn.port}"

    topic_name = topic
    utils.ensure_topic_exists(topic_name,bootstrap_servers)

    producer = utils.create_kafka_producer()
    message_key = "races" 
    message_value = race_data
    producer.send(topic_name,key = message_key,value=message_value)
    producer.flush()
    print("Races Data Sent and Flushed")


@task
def send_seasons_data_to_kafka(data):
    kafka_conn_id = kafka_id
    kafka_conn = BaseHook.get_connection(kafka_conn_id)
    bootstrap_servers = f"{kafka_conn.host}:{kafka_conn.port}"

    topic_name = topic
    utils.ensure_topic_exists(topic_name,bootstrap_servers)
    message_key = "seasons" 
    message_value = data
    producer = utils.create_kafka_producer()
    producer.send(topic_name,key = message_key,value=message_value)
    producer.flush()
    print(" Data Sent and Flushed")

@task
def send_lap_data_to_kafka(all_rounds_data: dict):
    
    kafka_conn_id = kafka_id
    kafka_conn = BaseHook.get_connection(kafka_conn_id)
    bootstrap_servers = f"{kafka_conn.host}:{kafka_conn.port}"

    topic_name = topic
    utils.ensure_topic_exists(topic_name, bootstrap_servers)

    producer = utils.create_kafka_producer()
    lap={}
    for round_number, lap_data in all_rounds_data.items():
        if lap_data:  
            message_key = f"round_{round_number}"  
            message_value = lap_data
            lap[message_key] = message_value

      
            
            print(f"Data for round {round_number} sent to Kafka.")
        else:
            print(f"No data available for round {round_number}, skipping.")
    producer.send(topic_name, key="lap", value=lap)
    producer.flush()
    print("All data sent and producer flushed.")

@task
def send_stop_data_to_kafka(all_rounds_data: dict):
    
    kafka_conn_id = kafka_id
    kafka_conn = BaseHook.get_connection(kafka_conn_id)
    bootstrap_servers = f"{kafka_conn.host}:{kafka_conn.port}"

    topic_name = topic
    utils.ensure_topic_exists(topic_name, bootstrap_servers)

    producer = utils.create_kafka_producer()
    stop={}
    for round_number, stop_data in all_rounds_data.items():
        if stop_data:  
            message_key = f"round_{round_number}"  
            message_value = stop_data
            stop[message_key] = message_value

      
            
            print(f"Data for round {round_number} sent to Kafka.")
        else:
            print(f"No data available for round {round_number}, skipping.")
    producer.send(topic_name, key="stop", value=stop)
    producer.flush()
    print("All data sent and producer flushed.")

@task
def send_qual_data_to_kafka(all_rounds_data: dict):
    
    kafka_conn_id = kafka_id
    kafka_conn = BaseHook.get_connection(kafka_conn_id)
    bootstrap_servers = f"{kafka_conn.host}:{kafka_conn.port}"

    topic_name = topic
    utils.ensure_topic_exists(topic_name, bootstrap_servers)

    producer = utils.create_kafka_producer()
    qual = {}
    for round_number, stop_data in all_rounds_data.items():
        if stop_data:  
            message_key = f"round_{round_number}"  
            message_value = stop_data
            qual[message_key] = message_value
      
            
            print(f"Data for round {round_number} sent to Kafka.")
        else:
            print(f"No data available for round {round_number}, skipping.")
    producer.send(topic_name, key="qual", value=qual)
    producer.flush()
    print("All data sent and producer flushed.")

@task
def send_sprint_data_to_kafka(all_rounds_data: dict):
    
    kafka_conn_id = kafka_id
    kafka_conn = BaseHook.get_connection(kafka_conn_id)
    bootstrap_servers = f"{kafka_conn.host}:{kafka_conn.port}"

    topic_name = topic
    utils.ensure_topic_exists(topic_name, bootstrap_servers)

    producer = utils.create_kafka_producer()
    sprint ={}
    for round_number, stop_data in all_rounds_data.items():
        if stop_data:  
            message_key = f"round_{round_number}"  
            message_value = stop_data
            sprint[message_key] = message_value

      
            
            print(f"Data for round {round_number} sent to Kafka.")
        else:
            print(f"No data available for round {round_number}, skipping.")
    producer.send(topic_name, key="sprint", value=sprint)
    producer.flush()
    print("All data sent and producer flushed.")

@task
def send_results_data_to_kafka(all_rounds_data: dict):
    
    kafka_conn_id = kafka_id
    kafka_conn = BaseHook.get_connection(kafka_conn_id)
    bootstrap_servers = f"{kafka_conn.host}:{kafka_conn.port}"

    topic_name = topic
    utils.ensure_topic_exists(topic_name, bootstrap_servers)

    producer = utils.create_kafka_producer()
    result = {}
    for round_number, result_data in all_rounds_data.items():
        if result_data:  
            message_key = f"round_{round_number}"  
            message_value = result_data
            result[message_key] = message_value

      
            print(f"Data for round {round_number} sent to Kafka.")
        else:
            print(f"No data available for round {round_number}, skipping.")
    
    producer.send(topic_name, key="result", value=result)
    producer.flush()
    print("All data sent and producer flushed.")

@task
def send_constructor_standings_data_to_kafka(all_rounds_data: dict):
    kafka_conn_id = kafka_id
    kafka_conn = BaseHook.get_connection(kafka_conn_id)
    bootstrap_servers = f"{kafka_conn.host}:{kafka_conn.port}"

    topic_name = topic
    utils.ensure_topic_exists(topic_name, bootstrap_servers)

    producer = utils.create_kafka_producer()
    constructor_standings ={}
    for round_number, result_data in all_rounds_data.items():
        if result_data:  
            message_key = f"round_{round_number}"  
            message_value = result_data
            constructor_standings[message_key] = message_value

      
            print(f"Data for round {round_number} sent to Kafka.")
        else:
            print(f"No data available for round {round_number}, skipping.")
    
    producer.send(topic_name, key="constructor_standings", value=constructor_standings)
    producer.flush()
    print("All data sent and producer flushed.")

@task
def send_driver_standings_data_to_kafka(all_rounds_data: dict):
    kafka_conn_id = kafka_id
    kafka_conn = BaseHook.get_connection(kafka_conn_id)
    bootstrap_servers = f"{kafka_conn.host}:{kafka_conn.port}"

    topic_name = topic
    utils.ensure_topic_exists(topic_name, bootstrap_servers)

    producer = utils.create_kafka_producer()
    driver_standings= {}
    for round_number, result_data in all_rounds_data.items():
        if result_data:  
            message_key = f"round_{round_number}"  
            message_value = result_data
            driver_standings[message_key] = message_value
      
            
            print(f"Data for round {round_number} sent to Kafka.")
        else:
            print(f"No data available for round {round_number}, skipping.")
    producer.send(topic_name, key="driver_standings", value=driver_standings)
    producer.flush()
    print("All data sent and producer flushed.")

@task
def get_lap_data():

        all_rounds_data = {}
        #should be from 1 to 25 by the end of the season
        for round_number in range(1, 23):
            endpoint = f"{base_url}/2024/{round_number}/laps.json"
            response = requests.get(endpoint)

            if response.status_code == 200:
                print(f"Fetched data for round {round_number}")
                lap_data = response.json().get('MRData', {}).get('RaceTable', {}).get('Races', {})
                all_rounds_data[round_number] = lap_data
            else:
                print(f"Failed to fetch data for round {round_number}: {response.status_code}")
                all_rounds_data[round_number] = None

        return all_rounds_data

@task
def get_stop_data():

        all_rounds_data = {}
        #should be from 1 to 25 by the end of the season
        for round_number in range(1, 23):
            endpoint = f"{base_url}/2024/{round_number}/pitstops.json"
            response = requests.get(endpoint)

            if response.status_code == 200:
                print(f"Fetched data for round {round_number}")
                stop_data = response.json().get('MRData', {}).get('RaceTable', {}).get('Races', {})
                all_rounds_data[round_number] = stop_data
            else:
                print(f"Failed to fetch data for round {round_number}: {response.status_code}")
                all_rounds_data[round_number] = None

        return all_rounds_data

@task
def get_qual_data():

        all_rounds_data = {}
        #should be from 1 to 25 by the end of the season
        for round_number in range(1, 23):
            endpoint = f"{base_url}/2024/{round_number}/qualifying.json"
            response = requests.get(endpoint)

            if response.status_code == 200:
                print(f"Fetched data for round {round_number}")
                qual_data = response.json().get('MRData', {}).get('RaceTable', {}).get('Races', {})
                all_rounds_data[round_number] = qual_data
            else:
                print(f"Failed to fetch data for round {round_number}: {response.status_code}")
                all_rounds_data[round_number] = None

        return all_rounds_data

@task
def get_sprint_data():

        all_rounds_data = {}
        #should be from 1 to 25 by the end of the season
        for round_number in range(1, 23):
            endpoint = f"{base_url}/2024/{round_number}/sprint.json"
            response = requests.get(endpoint)

            if response.status_code == 200:
                print(f"Fetched data for round {round_number}")
                sprint_data = response.json().get('MRData', {}).get('RaceTable', {}).get('Races', {})
                all_rounds_data[round_number] = sprint_data
            else:
                print(f"Failed to fetch data for round {round_number}: {response.status_code}")
                all_rounds_data[round_number] = None

        return all_rounds_data

@task
def get_result_data():

        all_rounds_data = {}
        #should be from 1 to 25 by the end of the season
        for round_number in range(1, 23):
            endpoint = f"{base_url}/2024/{round_number}/results.json"
            response = requests.get(endpoint)

            if response.status_code == 200:
                print(f"Fetched data for round {round_number}")
                result_data = response.json().get('MRData', {}).get('RaceTable', {}).get('Races', {})
                all_rounds_data[round_number] = result_data
            else:
                print(f"Failed to fetch data for round {round_number}: {response.status_code}")
                all_rounds_data[round_number] = None

        return all_rounds_data

@task
def get_constructor_standings_data():
    all_rounds_data = {}
    #should be from 1 to 25 by the end of the season
    for round_number in range(1, 23):
            endpoint = f"{base_url}/2024/{round_number}/constructorStandings.json"
            response = requests.get(endpoint)

            if response.status_code == 200:
                print(f"Fetched data for round {round_number}")
                standings_data = response.json().get('MRData',{}).get('StandingsTable',{}).get('StandingsLists', {})
                all_rounds_data[round_number] = standings_data
            else:
                print(f"Failed to fetch data for round {round_number}: {response.status_code}")
                all_rounds_data[round_number] = None

    return all_rounds_data

@task
def get_driver_standings_data():
    all_rounds_data = {}
    #should be from 1 to 25 by the end of the season
    for round_number in range(1, 23):
            endpoint = f"{base_url}/2024/{round_number}/driverStandings.json"
            response = requests.get(endpoint)

            if response.status_code == 200:
                print(f"Fetched data for round {round_number}")
                standings_data = response.json().get('MRData',{}).get('StandingsTable',{}).get('StandingsLists', {})
                all_rounds_data[round_number] = standings_data
            else:
                print(f"Failed to fetch data for round {round_number}: {response.status_code}")
                all_rounds_data[round_number] = None

    return all_rounds_data

@dag(start_date=datetime(2023, 11, 1), schedule="@daily", catchup=False, dag_id='kafka_producer_dag')
def kafka_producer_dag():

    driver_data_task = HttpOperator(
        task_id="get_driver_data",
        http_conn_id="ergast_api",  
        endpoint="api/f1/2024/drivers.json",
        method="GET", 
        response_filter=lambda response: response.json()['MRData']['DriverTable']['Drivers'], 
        log_response=True,
    )
    circuit_data_task = HttpOperator(
        task_id="get_circuit_data",
        http_conn_id="ergast_api",  
        endpoint="api/f1/2024/circuits.json",
        method="GET", 
        response_filter=lambda response: response.json()['MRData']['CircuitTable']['Circuits'], 
        log_response=True,
    )
    constructor_data_task = HttpOperator(
        task_id="get_constructor_data",
        http_conn_id="ergast_api",  
        endpoint="api/f1/2024/constructors.json",
        method="GET", 
        response_filter=lambda response: response.json()['MRData']['ConstructorTable']['Constructors'], 
        log_response=True,
    )
    status_data_task = HttpOperator(
        task_id="get_status_data",
        http_conn_id="ergast_api",  
        endpoint="api/f1/status.json",
        method="GET", 
        response_filter=lambda response: response.json()['MRData']['StatusTable']['Status'], 
        log_response=True,
    )

   
    season_data_task = HttpOperator(
        task_id="get_season_data",
        http_conn_id="ergast_api",  
        endpoint="api/f1/seasons.json",
        method="GET", 
        response_filter=lambda response: response.json()['MRData']['SeasonTable']['Seasons'], 
        log_response=True,
    )
    
    races_task = HttpOperator(
        task_id="get_races_data",
        http_conn_id="ergast_api",  
        endpoint="api/f1/2024.json",
        method="GET", 
        response_filter=lambda response: response.json()['MRData']['RaceTable']['Races'], 
        log_response=True,
    )
   
  
    
 
    send_driver_data_task = send_driver_data_to_kafka(driver_data_task.output)
    send_circuit_data_task = send_circuit_data_to_kafka(circuit_data_task.output)
    send_constructor_data_task = send_constructor_data_to_kafka(constructor_data_task.output)
    send_status_data_task = send_status_data_to_kafka(status_data_task.output)
    send_seasons_data_task = send_seasons_data_to_kafka(season_data_task.output)
    
    send_races_data_task = send_races_data_to_kafka(races_task.output)
    lap_data_task = get_lap_data()
    send_lap_data_task = send_lap_data_to_kafka(lap_data_task)
    stop_data_task = get_stop_data()
    send_stop_data_task = send_stop_data_to_kafka(stop_data_task)
    qual_data_task = get_qual_data()
    send_qual_data_task = send_qual_data_to_kafka(qual_data_task)
    sprint_data_task = get_sprint_data()
    send_sprint_data_task = send_sprint_data_to_kafka(sprint_data_task)
    results_data_task = get_result_data()
    send_results_data_task = send_results_data_to_kafka(results_data_task)
    constructor_standings_task = get_constructor_standings_data()
    send_constructor_standings_data_task = send_constructor_standings_data_to_kafka(constructor_standings_task)
    driver_standings_task = get_driver_standings_data()
    send_driver_standings_data_task = send_driver_standings_data_to_kafka(driver_standings_task)



    driver_data_task >> send_driver_data_task
    circuit_data_task >> send_circuit_data_task
    circuit_data_task >> send_constructor_data_task
    status_data_task >> send_status_data_task
    driver_standings_task >> send_driver_standings_data_task
    results_data_task >> send_results_data_task
    season_data_task >> send_seasons_data_task
    constructor_standings_task >> send_constructor_standings_data_task
    races_task >> send_races_data_task
    qual_data_task >> send_qual_data_task
    sprint_data_task >> send_sprint_data_task
    lap_data_task >> send_lap_data_task
    stop_data_task >> send_stop_data_task

kafka_producer_dag()
