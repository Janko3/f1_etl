from airflow.decorators import task, dag
from kafka import KafkaConsumer
import json
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch 
from dotenv import load_dotenv
import os
from kafka import KafkaConsumer
from airflow.operators.dummy import DummyOperator
from kafka import KafkaConsumer
import pandas as pd
import json
import sys
sys.path.append('/opt/airflow/dags/programs/utils')
import utils


load_dotenv(dotenv_path='.env')

db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

consumer = KafkaConsumer(
    "data_topic",
    bootstrap_servers='kafka:9092',
    max_poll_records=100,
    value_deserializer=lambda m: json.loads(m.decode('ascii')),
    auto_offset_reset='earliest'
)

@task
def process_driver_data():
    messages = []  
    desired_key = "drivers"
        
    records = consumer.poll(timeout_ms=1000) 
    if not records:
        print("No records were consumed.")
        return None

 
    for tp, msgs in records.items():
        for msg in msgs:
            key = msg.key.decode('utf-8') if msg.key else None
            if key == desired_key:
                    value = msg.value if isinstance(msg.value, (dict, list)) else json.loads(msg.value)
                    messages.append(value)

    if not messages:
        print("No valid messages with the desired key were consumed.")
        return None

    print(f"Processed {len(messages)} messages with the key '{desired_key}'.")

    flattened_messages = [item for sublist in messages for item in sublist] if isinstance(messages[0], list) else messages

    try:
        driver_df = pd.DataFrame(flattened_messages)
        print(f"Initial DataFrame columns: {driver_df.columns}")
    except Exception as e:
        print(f"Error creating DataFrame: {e}")
        return None

    driver_df = driver_df.drop_duplicates(subset=["driverId"], keep="first")

    driver_df = driver_df.replace(r'\N', None)

    print(f"Processed driver data:\n{driver_df.head()}")
    return driver_df
@task
def process_lap_data():
  
    desired_key = "lap" 

    records = consumer.poll(timeout_ms=1000)
    if not records:
        print("No records were consumed.")
        return None

    lap_data = []
    print(f"NESTO: {records.items()}")
    for tp, msgs in records.items():
        
        for msg in msgs:
            key = msg.key.decode('utf-8') if msg.key else None
            if key == desired_key: 
                all_rounds_data = msg.value 

                for round_key, round_entries in all_rounds_data.items():
                    if round_entries: 
                        for entry in round_entries:
                            season = entry.get("season")
                            round_number = entry.get("round")
                            lap = entry.get("Laps", [])

                            for standing in lap:
                                timings = standing.get('Timings', [])  
                                for timing in timings: 
                                    standings_frame = {
                                        'season': season,
                                        'round': round_number,
                                        'lap': standing.get('number'),
                                        'driverId': timing.get('driverId'),
                                        'position': timing.get('position'),
                                        'time': timing.get('time'),
                                    }
                                lap_data.append(standings_frame)

    print(f"Collected {len(lap_data)} rows of lap  data.")
    try:
        df = pd.DataFrame(lap_data)
    except Exception as e:
        print(f"Error creating DataFrame: {e}")
        return None

    #df = df.drop_duplicates(subset=["driverId", "season", "round"], keep="first")
    df = df.replace(r'\N', None)
    print(f"Processed lap  data:\n{df.head()}")

    return df

@task
def process_stop_data():
    desired_key = "stop" 
    records = consumer.poll(timeout_ms=1000)
    if not records:
        print("No records were consumed.")
        return None

    stop_data = []

    for tp, msgs in records.items():
        for msg in msgs:
            key = msg.key.decode('utf-8') if msg.key else None
            if key == desired_key: 
                all_rounds_data = msg.value 

                for round_key, round_entries in all_rounds_data.items():
                    if round_entries: 
                        for entry in round_entries:
                            season = entry.get("season")
                            round_number = entry.get("round")
                            stop = entry.get("PitStops", [])

                            for standing in stop:
                                standings_frame = {
                                    'season': season,
                                    'round': round_number,
                                    'driverId': standing.get('driverId'),
                                    'lap': standing.get('lap'),
                                    'time': standing.get('time'),
                                    'duration': standing.get('duration'),
                                    'stop': standing.get('stop')
                                }
                                stop_data.append(standings_frame)

    print(f"Collected {len(stop_data)} rows of stop  data.")
    try:
        df = pd.DataFrame(stop_data)
    except Exception as e:
        print(f"Error creating DataFrame: {e}")
        return None

    #df = df.drop_duplicates(subset=["driverId", "season", "round"], keep="first")
    df = df.replace(r'\N', None)
    print(f"Processed stop  data:\n{df.head()}")

    return df

    
@task
def process_circuit_data():
    messages = []  
  
    desired_key = "circuits"

    while True:
        
        records = consumer.poll(timeout_ms=1000) 
        if not records:
            break  

 
        for tp, msgs in records.items():
            for msg in msgs:
                key = msg.key.decode('utf-8') if msg.key else None
                if key == desired_key:
                    value = msg.value if isinstance(msg.value, (dict, list)) else json.loads(msg.value)
                    messages.append(value)

    if not messages:
        print("No valid messages with the desired key were consumed.")
        return None

    print(f"Processed {len(messages)} messages with the key '{desired_key}'.")



    flattened_messages = [item for sublist in messages for item in sublist] if isinstance(messages[0], list) else messages

    try:
        circuit_df = pd.DataFrame(flattened_messages)

        print(f"Initial DataFrame columns: {circuit_df.columns}")
    except Exception as e:
        print(f"Error creating DataFrame: {e}")
        return None


    circuit_df = circuit_df.drop_duplicates(subset=["circuitId"], keep="first")
    circuit_df = circuit_df.replace(r'\N', None)
    
    print(f"Processed circuit data:\n{circuit_df.head()}")
    return circuit_df

@task
def process_constructor_data():
    messages = []  
  
    desired_key = "constructors"

    while True:
        
        records = consumer.poll(timeout_ms=1000) 
        if not records:
            break  

 
        for tp, msgs in records.items():
            for msg in msgs:
                key = msg.key.decode('utf-8') if msg.key else None
                if key == desired_key:
                    value = msg.value if isinstance(msg.value, (dict, list)) else json.loads(msg.value)
                    messages.append(value)

    if not messages:
        print("No valid messages with the desired key were consumed.")
        return None

    print(f"Processed {len(messages)} messages with the key '{desired_key}'.")



    flattened_messages = [item for sublist in messages for item in sublist] if isinstance(messages[0], list) else messages

    try:
        constructor_df = pd.DataFrame(flattened_messages)

        print(f"Initial DataFrame columns: {constructor_df.columns}")
    except Exception as e:
        print(f"Error creating DataFrame: {e}")
        return None


    constructor_df = constructor_df.drop_duplicates(subset=["constructorId"], keep="first")
    constructor_df = constructor_df.replace(r'\N', None)
    
    print(f"Processed circuit data:\n{constructor_df.head()}")
    return constructor_df

@task
def process_status_data():
    messages = []  
  
    desired_key = "status"

    while True:
        
        records = consumer.poll(timeout_ms=1000) 
        if not records:
            break  

 
        for tp, msgs in records.items():
            for msg in msgs:
                key = msg.key.decode('utf-8') if msg.key else None
                if key == desired_key:
                    value = msg.value if isinstance(msg.value, (dict, list)) else json.loads(msg.value)
                    messages.append(value)

    if not messages:
        print("No valid messages with the desired key were consumed.")
        return None

    print(f"Processed {len(messages)} messages with the key '{desired_key}'.")


    flattened_messages = [item for sublist in messages for item in sublist] if isinstance(messages[0], list) else messages

    try:
        status_df = pd.DataFrame(flattened_messages)

        print(f"Initial DataFrame columns: {status_df.columns}")
    except Exception as e:
        print(f"Error creating DataFrame: {e}")
        return None


    status_df = status_df.drop_duplicates(subset=["status"], keep="first")
    status_df = status_df.replace(r'\N', None)
    
    print(f"Processed status data:\n{status_df.head()}")
    return status_df

@task
def process_driver_standings_data():
   
    desired_key = "driver_standings" 
    records = consumer.poll(timeout_ms=1000)
    if not records:
        print("No records were consumed.")
        return None

    driver_standings_data = []

    for tp, msgs in records.items():
        for msg in msgs:
            key = msg.key.decode('utf-8') if msg.key else None
            if key == desired_key: 
                all_rounds_data = msg.value 

                for round_key, round_entries in all_rounds_data.items():
                    if round_entries: 
                        for entry in round_entries:
                            season = entry.get("season")
                            round_number = entry.get("round")
                            driver_standings = entry.get("DriverStandings", [])

                            for standing in driver_standings:
                                driver = standing.get("Driver", {})
                                standings_frame = {
                                    'season': season,
                                    'round': round_number,
                                    'driverId': driver.get('driverId'),
                                    'points': standing.get('points'),
                                    'position': standing.get('position'),
                                    'wins': standing.get('wins'),
                                }
                                driver_standings_data.append(standings_frame)

    print(f"Collected {len(driver_standings_data)} rows of driver standings data.")
    try:
        df = pd.DataFrame(driver_standings_data)
    except Exception as e:
        print(f"Error creating DataFrame: {e}")
        return None

    df = df.drop_duplicates(subset=["driverId", "season", "round"], keep="first")
    df = df.replace(r'\N', None)
    print(f"Processed driver standings data:\n{df.head()}")

    return df


@task
def process_constructor_standings_data():
    desired_key = "constructor_standings" 
    records = consumer.poll(timeout_ms=1000)  
    
    if not records:
        print("No records were consumed.")
        return None
    
    constructor_standings_data = []

    for tp, msgs in records.items():
            for msg in msgs:
                key = msg.key.decode('utf-8') if msg.key else None
                if key == desired_key: 
                    all_rounds_data = msg.value 

                    for round_key, round_entries in all_rounds_data.items():
                        if round_entries: 
                            for entry in round_entries:
                                season = entry.get("season")
                                round_number = entry.get("round")
                                constructor_standings = entry.get('ConstructorStandings', [])

                                for standing in constructor_standings:
                                    constructor = standing.get('Constructor', {})

                                    standings_frame = {
                                        'season': season,
                                        'round': round_number,
                                        'constructorId': constructor.get('constructorId', None),
                                        'points': standing.get('points', None),
                                        'position': standing.get('position', None),
                                        'wins': standing.get('wins', None),
                                    }
                                    constructor_standings_data.append(standings_frame)


    print(f"Collected {len(constructor_standings_data)} rows of constructor standings data.")

    try:
        df = pd.DataFrame(constructor_standings_data)
    except Exception as e:
        print(f"Error creating DataFrame: {e}")
        return None
    
    df = df.drop_duplicates(subset=["constructorId", "season","round"], keep="first")
    df = df.replace(r'\N', None)
    print(f"Processed session data:\n{df.head()}")
    
    return df



@task
def process_result_data():  
    desired_key = "result" 
    sprint_key = "sprint"
    records = consumer.poll(timeout_ms=1000)  
    if not records:
        print("No records were consumed.")
        return None

    result_data = []
    sprint_points_dict = {}

    for tp, msgs in records.items():
        for msg in msgs:
            key = msg.key.decode('utf-8') if msg.key else None

            if key == sprint_key: 
                all_rounds_data = msg.value
                for round_key, round_entries in all_rounds_data.items():
                    if round_entries:
                        for entry in round_entries:
                            round_number = entry.get("round")
                            sprint_results = entry.get('SprintResults', [])
                            for result in sprint_results:
                                driver = result.get('Driver', {})
                                points = int(result.get('points', 0)) 
                                driver_id = driver.get('driverId')
                                sprint_points_dict[(driver_id, round_number)] = points

            elif key == desired_key:  
                all_rounds_data = msg.value
                for round_key, round_entries in all_rounds_data.items():
                    if round_entries:
                        for entry in round_entries:
                            season = entry.get("season")
                            round_number = entry.get("round")
                            results = entry.get('Results', [])
                            
                            for result in results:
                                constructor = result.get('Constructor', {})
                                driver = result.get('Driver', {})
                                time_info = result.get('Time', {})
                                fastest_lap_info = result.get('FastestLap', {})
                                fastest_lap_speed_info = fastest_lap_info.get('AverageSpeed', {})

                                points = int(result.get('points', 0))
                                driver_id = driver.get('driverId', None)

                                sprint_points = sprint_points_dict.get((driver_id, round_number), 0)

                                total_points = points + sprint_points

                                result_frame = {
                                    'season': season,
                                    'round': round_number,
                                    'constructorId': constructor.get('constructorId', None),
                                    'driverId': driver_id,
                                    'position': result.get('position', None),
                                    'points': total_points,
                                    'grid': result.get('grid', None),
                                    'laps': result.get('laps', None),
                                    'status': result.get('status', None),
                                    'time': time_info.get('time', None),
                                    'millis': time_info.get('millis', None),
                                    'fastestLap': fastest_lap_info.get('lap', None),
                                    'fastestLapTime': fastest_lap_info.get('Time', {}).get('time', None),
                                    'fastestLapSpeed': fastest_lap_speed_info.get('speed', None),
                                    'rank': fastest_lap_info.get('rank', None),
                                }
                                result_data.append(result_frame)

    print(f"Collected {len(result_data)} rows of data.")

    try:
        df = pd.DataFrame(result_data)
    except Exception as e:
        print(f"Error creating DataFrame: {e}")
        return None
    
    df = df.drop_duplicates(subset=["driverId", "season", "round"], keep="first")
    df = df.replace(r'\N', None)
    print(f"Processed session data:\n{df.head()}")
    
    return df




@task
def process_session_data():
    specific_key = "races"
    records = consumer.poll(timeout_ms=1000)  
    
    # print(f"MESSAGES : {records.items()}")
    
    if not records:
        print("No records were consumed.")
        return None

    race_data = []
    for tp, msgs in records.items(): 
        for msg in msgs:
            for item in msg.value:
                key = msg.key.decode('utf-8') if msg.key else None
                if key == specific_key:
                    #print(f"PORUKA RACE JE: {item['FirstPractice']['date']}")
                    race_frame = {
                        'season': item.get('season', None),
                        'raceName': item.get('raceName', None),
                        'fp1_date': item.get('FirstPractice', {}).get('date', None),
                        'fp1_time': item.get('FirstPractice', {}).get('time', None),
                        'fp2_date': item.get('SecondPractice', {}).get('date', None),
                        'fp2_time': item.get('SecondPractice', {}).get('time', None),
                        'fp3_date': item.get('ThirdPractice', {}).get('date', None),
                        'fp3_time': item.get('ThirdPractice', {}).get('time', None),
                        'quali_date': item.get('Qualifying', {}).get('date', None),
                        'quali_time': item.get('Qualifying', {}).get('time', None),
                        'sprint_date': item.get('Sprint', {}).get('date', None),
                        'sprint_time': item.get('Sprint', {}).get('time', None)
                    }
                    race_data.append(race_frame)

    
    
    try:
        df = pd.DataFrame(race_data)
    except Exception as e:
        print(f"Error creating DataFrame: {e}")
        return None
    
    df = df.drop_duplicates(subset=["season", "raceName"], keep="first")
    df = df.replace(r'\N', None)
    print(f"Processed session data:\n{df.head()}")
    
    return df



@task
def process_race_data():
    messages = []  
    desired_key = "races" 

    while True:
        
        records = consumer.poll(timeout_ms=1000) 
        if not records:
            break  
 
        for tp, msgs in records.items():
            for msg in msgs:
                key = msg.key.decode('utf-8') if msg.key else None
                if key == desired_key:
                    value = msg.value if isinstance(msg.value, (dict, list)) else json.loads(msg.value)
                    messages.append(value)

    if not messages:
        print("No valid messages with the desired key were consumed.")
        return None

    print(f"Processed {len(messages)} messages with the key '{desired_key}'.")
    flattened_messages = [item for sublist in messages for item in sublist] if isinstance(messages[0], list) else messages

    try:
        race_df = pd.DataFrame(flattened_messages)
        print(f"DataFrame columns before any processing: {race_df.columns}")
        print(f"First few rows of DataFrame:\n{race_df.head()}")
    except Exception as e:
        print(f"Error creating DataFrame: {e}")
        return None

   
    race_df = race_df.drop_duplicates(subset=["season", "round"], keep="first")
    race_df = race_df.replace(r'\N', None)

    print(f"Processed race data:\n{race_df.head()}")
    return race_df


@task
def load_driver_data(df: pd.DataFrame):
    data_dict = df.to_dict(orient="records")
    
    try:
        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        conn.autocommit = True
        cursor = conn.cursor()
    except Exception as e:
        print(f"Error: Failed to connect to the database: {e}")
        return 

    select_sql = '''
        SELECT "driverRef" FROM driver_dim
    '''
    existing_drivers = pd.read_sql(select_sql, conn).to_dict(orient="records")
    print("Existing drivers data:", existing_drivers)
    existing_driver_refs = {driver['driverRef'] for driver in existing_drivers}

    new_drivers = [
        driver for driver in data_dict if driver['driverId'] not in existing_driver_refs
    ]
    
    if not new_drivers:
        print("No new drivers to insert.")
        return

  
    select_last_id_sql = '''SELECT MAX("driverId") FROM driver_dim '''
    cursor.execute(select_last_id_sql)
    last_driver_id = cursor.fetchone()[0] or 0  
    print("last_driver_id:", last_driver_id)

    insert_sql = '''
        INSERT INTO driver_dim ("driverId", "driverRef", "forename", "surname", "dob", "code", "url", "number", "nationality")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ("driverId") DO UPDATE
        SET "driverRef" = EXCLUDED."driverRef",
            "forename" = EXCLUDED."forename",
            "surname" = EXCLUDED."surname",
            "dob" = EXCLUDED."dob",
            "code" = EXCLUDED."code",
            "url" = EXCLUDED."url",
            "number" = EXCLUDED."number",
            "nationality" = EXCLUDED."nationality";
    '''

   
    data_tuples = [
        (int(last_driver_id) + i + 1,  
         record["driverId"],
         record["givenName"],
         record["familyName"],
         record["dateOfBirth"],
         record["code"],
         record["url"],
         record["permanentNumber"],
         record["nationality"])
        for i, record in enumerate(new_drivers)
    ]
    
    try:
        execute_batch(cursor, insert_sql, data_tuples, page_size=10000)
        print(f"Data successfully inserted into driver_dim table.")
    except Exception as e:
        print(f"Error occurred while inserting data: {e}")
    finally:
        cursor.close()
        conn.close()

@task
def load_circuit_data(df: pd.DataFrame):
    data_dict = df.to_dict(orient="records")
    
    try:
        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        conn.autocommit = True
        cursor = conn.cursor()
    except Exception as e:
        print(f"Error: Failed to connect to the database: {e}")
        return 

    select_sql = '''
        SELECT "circuitRef" FROM circuit_dim
    '''
    existing_circuits = pd.read_sql(select_sql, conn).to_dict(orient="records")
    print("Existing circuits data:", existing_circuits)
    existing_circuit_refs = {circuit['circuitRef'] for circuit in existing_circuits}

    new_circuits = [
        circuit for circuit in data_dict if circuit['circuitId'] not in existing_circuit_refs
    ]
    
    if not new_circuits:
        print("No new circuits to insert.")
        return

  
    select_last_id_sql = '''SELECT MAX("circuitId") FROM circuit_dim '''
    cursor.execute(select_last_id_sql)
    last_id = cursor.fetchone()[0] or 0  
    print("last_circuit_id:", last_id)

    insert_sql = '''
        INSERT INTO circuit_dim ("circuitId", "circuitRef", "name_y", "url_y", "location", "country", "lat", "lng")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ("circuitId") DO UPDATE
        SET "circuitRef" = EXCLUDED."circuitRef",
            "name_y" = EXCLUDED."name_y",
            "url_y" = EXCLUDED."url_y",
            "location" = EXCLUDED."location",
            "country" = EXCLUDED."country",
            "lat" = EXCLUDED."lat",
            "lng" = EXCLUDED."lng";
            

    '''
   
    data_tuples = [
        (int(last_id) + i + 1,  
         record["circuitId"],
         record["circuitName"],
         record["url"],
         record["Location"]["locality"],
         record["Location"]["country"],
         record["Location"]["lat"],
         record["Location"]["long"]
         )
        for i, record in enumerate(new_circuits)
    ]
    
    try:
        execute_batch(cursor, insert_sql, data_tuples, page_size=10000)
        print(f"Data successfully inserted into circuit_dim table.")
    except Exception as e:
        print(f"Error occurred while inserting data: {e}")
    finally:
        cursor.close()
        conn.close()

@task
def load_constructor_data(df: pd.DataFrame):
    data_dict = df.to_dict(orient="records")
    
    try:
        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        conn.autocommit = True
        cursor = conn.cursor()
    except Exception as e:
        print(f"Error: Failed to connect to the database: {e}")
        return 

    select_sql = '''
        SELECT "constructorRef" FROM constructor_dim
    '''
    existing_constructor = pd.read_sql(select_sql, conn).to_dict(orient="records")
    print("Existing constructor data:", existing_constructor)
    existing_constructor_refs = {constructor['constructorRef'] for constructor in existing_constructor}

    new_constructors = [
        constructor for constructor in data_dict if constructor['constructorId'] not in existing_constructor_refs
    ]
    
    if not new_constructors:
        print("No new constructors to insert.")
        return

  
    select_last_id_sql = '''SELECT MAX("constructorId") FROM constructor_dim '''
    cursor.execute(select_last_id_sql)
    last_id = cursor.fetchone()[0] or 0  
    print("last_constructor_id:", last_id)

    insert_sql = '''
        INSERT INTO constructor_dim ("constructorId", "constructorRef", "name", "nationality_constructors", "url_constructors")
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT ("constructorId") DO UPDATE
        SET "constructorRef" = EXCLUDED."constructorRef",
            "name" = EXCLUDED."name",
            "nationality_constructors" = EXCLUDED."nationality_constructors",
            "url_constructors" = EXCLUDED."url_constructors";

    '''

    data_tuples = [
        (int(last_id) + i + 1,  
         record["constructorId"],
         record["name"],
         record["nationality"],
         record["url"],
         )
        for i, record in enumerate(new_constructors)
    ]
    
    try:
        execute_batch(cursor, insert_sql, data_tuples, page_size=10000)
        print(f"Data successfully inserted into constructor_dim table.")
    except Exception as e:
        print(f"Error occurred while inserting data: {e}")
    finally:
        cursor.close()
        conn.close()

@task
def load_status_data(df: pd.DataFrame):
    data_dict = df.to_dict(orient="records")
    
    try:
        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        conn.autocommit = True
        cursor = conn.cursor()
    except Exception as e:
        print(f"Error: Failed to connect to the database: {e}")
        return 

    select_sql = '''
        SELECT "status" FROM status_dim
    '''
    existing_status = pd.read_sql(select_sql, conn).to_dict(orient="records")
    print("Existing status data:", existing_status)
    existing_status_refs = {status['status'] for status in existing_status}

    new_statuses = [
        status for status in data_dict if status['status'] not in existing_status_refs
    ]
    
    if not new_statuses:
        print("No new status to insert.")
        return

  
    select_last_id_sql = '''SELECT MAX("statusId") FROM status_dim '''
    cursor.execute(select_last_id_sql)
    last_id = cursor.fetchone()[0] or 0  
    print("last_status_id:", last_id)

    insert_sql = '''
        INSERT INTO status_dim ("statusId", "status")
        VALUES (%s, %s)
        ON CONFLICT ("statusId") DO UPDATE
        SET "status" = EXCLUDED."status";

    '''

    data_tuples = [
        (int(last_id) + i + 1,  
         record["status"],
         )
        for i, record in enumerate(new_statuses)
    ]
    
    try:
        execute_batch(cursor, insert_sql, data_tuples, page_size=10000)
        print(f"Data successfully inserted into status_dim table.")
    except Exception as e:
        print(f"Error occurred while inserting data: {e}")
    finally:
        cursor.close()
        conn.close()

@task
def load_driver_standings_data(df: pd.DataFrame):
    data_dict = df.to_dict(orient="records")
    print("Input Data Dict:", data_dict)
    
    try:
        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        conn.autocommit = True
        cursor = conn.cursor()
    except Exception as e:
        print(f"Error: Failed to connect to the database: {e}")
        return 

    select_sql = '''
        SELECT "driverId", "raceId"
        FROM driver_standings_dim
    '''
    existing_data = pd.read_sql(select_sql, conn).to_dict(orient="records")

    print("Existing driver standings data:", existing_data)

    existing_combinations = {
        (data['driverId'], data['raceId'])
        for data in existing_data
    }

    select_race_sql = '''
        SELECT "raceId" FROM race_dim WHERE "year" = %s AND "round" = %s 
    '''

    race_map_dict = {}
    for data in data_dict:
        race_year = data['season']
        race_round = data['round']

        cursor.execute(select_race_sql, (race_year,race_round,))
        race_id_result = cursor.fetchone()

        if race_id_result:
            race_map_dict[(race_year,race_round)] = race_id_result[0]  
        else:
            print(f"Warning: No raceId found for {race_year} and {race_round}")

    

    select_driver_sql = '''
        SELECT "driverId" FROM driver_dim WHERE "driverRef" = %s 
    '''

    driver_map_dict = {}
    for data in data_dict:
        driver_name = data['driverId']

        cursor.execute(select_driver_sql, (driver_name,))
        driver_id_result = cursor.fetchone()

        if driver_id_result:
            driver_map_dict[(driver_name)] = driver_id_result[0]  
        else:
            print(f"Warning: No driverId found for {driver_name}")

    
    
    new_combinations = [
    status for status in data_dict
    if (
        (driver_map_dict[status['driverId']], race_map_dict[(status['season'], status['round'])]) not in existing_combinations
    )
] 
    print("Driver Map Dict:", driver_map_dict)
    print("Race Map Dict:", race_map_dict)
    print(f"NEW COMBINATIONS: {new_combinations}")
    if not new_combinations:
        print("No new driver standings to insert.")
        return
    else:
        print(f"Found {len(new_combinations)} new driver standings to insert.")

    select_last_id_sql = '''SELECT MAX("driverStandingsId") FROM driver_standings_dim '''
    cursor.execute(select_last_id_sql)
    last_id = cursor.fetchone()[0] or 0  
    print("last_driverStandings_id:", last_id)

    insert_sql = '''
        INSERT INTO driver_standings_dim ("driverStandingsId","driverId", "raceId", "points_driverstandings", "wins", "position_driverstandings")
        VALUES (%s, %s, %s, %s,%s,%s)
        ON CONFLICT ("driverStandingsId") DO UPDATE
        SET "driverId" = EXCLUDED."driverId",
            "raceId" = EXCLUDED."raceId",
            "points_driverstandings" = EXCLUDED."points_driverstandings",
            "wins" = EXCLUDED."wins",
            "position_driverstandings" = EXCLUDED."position_driverstandings";

    '''

    data_tuples = [
        (int(last_id) + i + 1, 
         driver_map_dict.get(record['driverId']) ,
         race_map_dict.get((record['season'], record['round'])),
         record["points"],
         record["wins"],
         record["position"]
         )
        for i, record in enumerate(new_combinations)
    ]
    
    try:
        execute_batch(cursor, insert_sql, data_tuples, page_size=10000)
        print(f"Data successfully inserted into driver_standings_dim table.")
    except Exception as e:
        print(f"Error occurred while inserting data: {e}")
    finally:
        cursor.close()
        conn.close()

@task
def load_constructor_standings_data(df: pd.DataFrame):
    data_dict = df.to_dict(orient="records")
    
    try:
        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        conn.autocommit = True
        cursor = conn.cursor()
    except Exception as e:
        print(f"Error: Failed to connect to the database: {e}")
        return 

    select_sql = '''
        SELECT "constructorId", "raceId"
        FROM constructor_standings_dim
    '''
    existing_data = pd.read_sql(select_sql, conn).to_dict(orient="records")

    print("Existing constructor standings data:", existing_data)

    existing_combinations = {
        (data['constructorId'],data['raceId'])
        for data in existing_data
    }

    select_race_sql = '''
        SELECT "raceId" FROM race_dim WHERE "year" = %s AND "round" = %s 
    '''

    race_map_dict = {}
    for data in data_dict:
        race_year = data['season']
        race_round = data['round']

        cursor.execute(select_race_sql, (race_year,race_round,))
        race_id_result = cursor.fetchone()

        if race_id_result:
            race_map_dict[(race_year,race_round)] = race_id_result[0]  
        else:
            print(f"Warning: No raceId found for {race_year} and {race_round}")
    
    select_constructor_sql = '''
        SELECT "constructorId" FROM constructor_dim WHERE "constructorRef" = %s 
    '''

    constructor_map_dict = {}
    for data in data_dict:
        constructor_name = data['constructorId']

        
        cursor.execute(select_constructor_sql, (constructor_name,))
        constructor_id_result = cursor.fetchone()

        if constructor_id_result:
            constructor_map_dict[(constructor_name)] = constructor_id_result[0]  
        else:
            print(f"Warning: No constructorId found for {constructor_name}")

    new_combinations = [
        constructor for constructor in data_dict
        if (constructor_map_dict.get(constructor['constructorId']),race_map_dict.get((constructor['season'], constructor['round']))) not in existing_combinations
    ]

    if not new_combinations:
        print("No new constructor standings to insert.")
        return
    else:
        print(f"Found {len(new_combinations)} new constructor standings to insert.")

    
    
   
    select_last_id_sql = '''SELECT MAX("constructorStandingsId") FROM constructor_standings_dim '''
    cursor.execute(select_last_id_sql)
    last_id = cursor.fetchone()[0] or 0  
    print("last_constructor_standings_id:", last_id)

    insert_sql = '''
       INSERT INTO constructor_standings_dim ("constructorStandingsId","constructorId" , "raceId", "points_constructorstandings", "wins_constructorstandings", "position_constructorstandings")
        VALUES (%s, %s, %s, %s,%s,%s)
        ON CONFLICT ("constructorStandingsId") DO UPDATE
        SET "constructorId" = EXCLUDED."constructorId",
            "raceId" = EXCLUDED."raceId",
            "points_constructorstandings" = EXCLUDED."points_constructorstandings",
            "wins_constructorstandings" = EXCLUDED."wins_constructorstandings",
            "position_constructorstandings" = EXCLUDED."position_constructorstandings";

    '''

    data_tuples = [
        (int(last_id) + i + 1, 
         constructor_map_dict.get(record['constructorId']),
         race_map_dict.get((record['season'], record['round'])),
         record["points"],
         record["wins"],
         record["position"]
         )
        for i, record in enumerate(new_combinations)
    ]
    
    try:
        execute_batch(cursor, insert_sql, data_tuples, page_size=10000)
        print(f"Data successfully inserted into constructor_standings_dim table.")
    except Exception as e:
        print(f"Error occurred while inserting data: {e}")
    finally:
        cursor.close()
        conn.close()

@task
def load_result_data(df: pd.DataFrame):
    data_dict = df.to_dict(orient="records")
    
    try:
        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        conn.autocommit = True
        cursor = conn.cursor()
    except Exception as e:
        print(f"Error: Failed to connect to the database: {e}")
        return 
    select_sql = '''
        SELECT "driverId", "raceId" FROM result_fact
    '''
    existing_result = pd.read_sql(select_sql, conn).to_dict(orient="records")
    print("Existing result data:", existing_result)
    existing_combinations = {
        (data['driverId'], data['raceId'])
        for data in existing_result
    }

    select_driver_sql = '''
        SELECT "driverId" FROM driver_dim WHERE "driverRef" = %s 
    '''

    driver_map_dict = {}
    for data in data_dict:
        driver_name = data['driverId']
       
        cursor.execute(select_driver_sql, (driver_name,))
        driver_id_result = cursor.fetchone()

        if driver_id_result:
            driver_map_dict[(driver_name)] = driver_id_result[0]  
        else:
            print(f"Warning: No driverId found for {driver_name}")

    select_race_sql = '''
        SELECT "raceId" FROM race_dim WHERE "year" = %s AND "round" = %s 
    '''

    race_map_dict = {}
    for data in data_dict:
        race_year = data['season']
        race_round = data['round']

        cursor.execute(select_race_sql, (race_year,race_round,))
        race_id_result = cursor.fetchone()

        if race_id_result:
            race_map_dict[(race_year,race_round)] = race_id_result[0]  
        else:
            print(f"Warning: No raceId found for {race_year} and {race_round}")
    
    new_combinations = [
        result for result in data_dict
        if (driver_map_dict.get(result['driverId']) , race_map_dict.get((result['season'], result['round'])),) not in existing_combinations
    ]

    if not new_combinations:
        print("No new results to insert.")
        return
    else:
        print(f"Found {len(new_combinations)} new results to insert.")

    select_constructor_sql = '''
        SELECT "constructorId" FROM constructor_dim WHERE "constructorRef" = %s 
    '''

    constructor_map_dict = {}
    for data in data_dict:
        constructor_name = data['constructorId']
       
        cursor.execute(select_constructor_sql, (constructor_name,))
        constructor_id_result = cursor.fetchone()

        if constructor_id_result:
            constructor_map_dict[(constructor_name)] = constructor_id_result[0]  
        else:
            print(f"Warning: No constructorId found for {constructor_name}")

    select_status_sql = '''
        SELECT "statusId" FROM status_dim WHERE "status" = %s 
    '''

    status_map_dict = {}
    for data in data_dict:
        status_name = data['status']
       
        cursor.execute(select_status_sql, (status_name,))
        status_id_result = cursor.fetchone()

        if status_id_result:
            status_map_dict[(status_name)] = status_id_result[0]  
        else:
            print(f"Warning: No statusId found for {status_name}")

    select_constructor_standings_sql = '''
        SELECT "constructorStandingsId" FROM constructor_standings_dim WHERE "constructorId" = %s AND "raceId" = %s
'''
    constructor_standings_map_dict = {}
    for data in data_dict:
        constructor_id = constructor_map_dict.get(data['constructorId'])
        race_id = race_map_dict.get((data['season'], data['round']))
        cursor.execute(select_constructor_standings_sql, (constructor_id,race_id,))
        constructor_standings_id_result = cursor.fetchone()

        if constructor_standings_id_result:
            constructor_standings_map_dict[(constructor_id,race_id)] = constructor_standings_id_result[0]  
        else:
            print(f"Warning: No constructor_standingsId found for {constructor_id}")

    select_driver_standings_sql = '''
        SELECT "driverStandingsId" FROM driver_standings_dim WHERE "driverId" = %s AND "raceId" = %s
'''
    driver_standings_map_dict = {}
    for data in data_dict:
        driver_id = driver_map_dict.get(data['driverId'])
        race_id = race_map_dict.get((data['season'], data['round']))
        cursor.execute(select_driver_standings_sql, (driver_id,race_id,))
        driver_standings_id_result = cursor.fetchone()

        if driver_standings_id_result:
            driver_standings_map_dict[(driver_id,race_id)] = driver_standings_id_result[0]  
        else:
            print(f"Warning: No driver_standingsId found for {driver_id}")

    select_last_id_sql = '''SELECT MAX("resultId") FROM result_fact'''
    cursor.execute(select_last_id_sql)
    last_id = cursor.fetchone()[0] or 0  
    print("last_result_id:", last_id)
    print(f"DRIVER STANDINGS {driver_standings_map_dict}")
    print(f"RACE  {race_map_dict}")
    data_tuples = []
    for i, record in enumerate(new_combinations):
        driver_id = driver_map_dict.get(record['driverId'])
        constructor_id = constructor_map_dict.get(record['constructorId'])
        race_id = race_map_dict.get((record['season'], record['round']))
        status_id = status_map_dict.get(record['status'])
        driver_standings_id = driver_standings_map_dict.get((driver_id, race_id))
        constructor_standings_id = constructor_standings_map_dict.get((constructor_id, race_id))

        if None in (driver_id, constructor_id, race_id, status_id, driver_standings_id, constructor_standings_id):
            print(f"Skipping invalid record: {record}")
            continue

        result_id = int(last_id) + i + 1

        data_tuples.append((
            result_id, driver_id, constructor_id, race_id, status_id,
            driver_standings_id, constructor_standings_id, record["grid"],
            record["position"], record["points"], record['time'], record['rank'],
            record['millis'], record['fastestLapSpeed'], record['fastestLapTime'],
            record['fastestLap'], record['laps']
        ))

    if not data_tuples:
        print("No valid data to insert.")
        return

    insert_sql = '''
       INSERT INTO result_fact ("resultId", "driverId", "constructorId", "raceId", "statusId", "driverStandingsId", "constructorStandingsId", "grid", "positionOrder",
         "points", "time", "rank", "milliseconds", "fastestLapSpeed", "fastestLapTime", "fastestLap", "laps")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ("resultId") DO UPDATE
        SET "driverId" = EXCLUDED."driverId",
            "constructorId" = EXCLUDED."constructorId",
            "raceId" = EXCLUDED."raceId",
            "statusId" = EXCLUDED."statusId",
            "driverStandingsId" = EXCLUDED."driverStandingsId",
            "constructorStandingsId" = EXCLUDED."constructorStandingsId",
            "grid" = EXCLUDED."grid",
            "positionOrder" = EXCLUDED."positionOrder",
            "points" = EXCLUDED."points",
            "time" = EXCLUDED."time",
            "rank" = EXCLUDED."rank",
            "milliseconds" = EXCLUDED."milliseconds",
            "fastestLapSpeed" = EXCLUDED."fastestLapSpeed",
            "fastestLapTime" = EXCLUDED."fastestLapTime",
            "fastestLap" = EXCLUDED."fastestLap",
            "laps" = EXCLUDED."laps";

    '''

    try:
        execute_batch(cursor, insert_sql, data_tuples, page_size=10000)
        print(f"{len(data_tuples)} records successfully inserted into result table.")
    except Exception as e:
        print(f"Error occurred while inserting data: {e}")
    finally:
        cursor.close()
        conn.close()

@task
def load_race_data(df: pd.DataFrame):
    data_dict = df.to_dict(orient="records")
    
    try:
        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        conn.autocommit = True
        cursor = conn.cursor()
    except Exception as e:
        print(f"Error: Failed to connect to the database: {e}")
        return 

    select_sql = '''
        SELECT "year", "round" FROM race_dim
    '''
    existing_race = pd.read_sql(select_sql, conn).to_dict(orient="records")
    print("Existing race data:", existing_race)
    existing_combinations = {
        (data['year'], data['round'])
        for data in existing_race
    }

    new_combinations = [
        race for race in data_dict
        if (race['season'], race['round']) not in existing_combinations
    ]

    if not new_combinations:
        print("No new races to insert.")
        return
    else:
        print(f"Found {len(new_combinations)} new races to insert.")
    
    select_last_id_sql = '''SELECT MAX("raceId") FROM race_dim'''
    cursor.execute(select_last_id_sql)
    last_id = cursor.fetchone()[0] or 0  
    print("last_race_id:", last_id)

    
    select_circuit_map_sql = '''
    SELECT "circuitRef", "circuitId" FROM circuit_dim
'''
    circuit_map = pd.read_sql(select_circuit_map_sql, conn)

   
    circuit_map_dict = circuit_map.set_index('circuitRef')['circuitId'].to_dict()

    data_tuples = []
    for i, record in enumerate(new_combinations):
        circuit_ref = record.get("Circuit").get("circuitId")  

        circuit_id = circuit_map_dict.get(circuit_ref)

        if not circuit_id:
            print(f"Warning: No circuitId found for circuitRef '{circuit_ref}'. Skipping race.")
            continue

        
        race_id = int(last_id) + i + 1
        data_tuples.append((
            race_id,
            circuit_id,
            record["season"],
            record["round"],
            record["raceName"],
            record["date"],
            record["time"],
            record["url"]
        ))

    if not data_tuples:
        print("No valid data to insert.")
        return

    insert_sql = '''
        INSERT INTO race_dim ("raceId", "circuitId", "year", "round", "name_x", "date", "time_races", "url_x")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ("raceId") DO UPDATE
        SET "circuitId" = EXCLUDED."circuitId",
            "year" = EXCLUDED."year",
            "round" = EXCLUDED."round",
            "name_x" = EXCLUDED."name_x",
            "date" = EXCLUDED."date",
            "time_races" = EXCLUDED."time_races",
            "url_x" = EXCLUDED."url_x";
    '''

    try:
        execute_batch(cursor, insert_sql, data_tuples, page_size=10000)
        print(f"{len(data_tuples)} records successfully inserted into race_dim table.")
    except Exception as e:
        print(f"Error occurred while inserting data: {e}")
    finally:
        cursor.close()
        conn.close()

@task
def load_lap_data(df: pd.DataFrame):
    data_dict = df.to_dict(orient="records")
    
    try:
        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        conn.autocommit = True
        cursor = conn.cursor()
    except Exception as e:
        print(f"Error: Failed to connect to the database: {e}")
        return 

    select_sql = '''
        SELECT "lap", "raceId", "driverId" FROM lap_dim
    '''
    existing_race = pd.read_sql(select_sql, conn).to_dict(orient="records")
    existing_combinations = {
        (data['lap'], data['raceId'], data['driverId']) for data in existing_race
    }

    select_race_sql = '''
        SELECT "raceId" FROM race_dim WHERE "round" = %s AND "year" = %s
    '''

    race_map_dict = {}
    for data in data_dict:
        race_name = data['round']
        season = data['season']
        
        cursor.execute(select_race_sql, (race_name, season))
        race_id_result = cursor.fetchone()

        if race_id_result:
            race_map_dict[(race_name, season)] = race_id_result[0]  
        else:
            print(f"Warning: No raceId found for {race_name} in {season}")


    select_driver_sql = '''
        SELECT "driverId" FROM driver_dim WHERE "driverRef" = %s
    '''

    driver_map_dict = {}

    for data in data_dict:

        driver_ref = data['driverId']
                
        cursor.execute(select_driver_sql, (driver_ref,))
        driver_id_result = cursor.fetchone()

        if driver_id_result:
            driver_map_dict[driver_ref] = driver_id_result[0] 
        else:
            print(f"Warning: No driverId found for {driver_ref} in race {data['round']}")

    new_combinations = []
    for data in data_dict:
        race_id = race_map_dict.get((data['round'], data['season']))

        lap_number = data['lap']


        driver_ref = data['driverId']
        position = data['position']
        time = data['time']
        milliseconds = utils.time_to_milliseconds(time)
                
        driver_id = driver_map_dict.get(driver_ref)
                
        if race_id and driver_id:
                    new_combinations.append(
                        (lap_number, race_id, driver_id, position, time, milliseconds)
                    )

    new_combinations = [
        combo for combo in new_combinations
        if (combo[0], combo[1], combo[2]) not in existing_combinations
    ]

    if not new_combinations:
        print("No new laps to insert.")
        return
    else:
        print(f"Found {len(new_combinations)} new laps to insert.")

    insert_sql = '''
        INSERT INTO lap_dim ("lap", "raceId", "driverId", "position_laptimes", "time_laptimes", "milliseconds_laptimes")
        VALUES (%s, %s, %s, %s, %s, %s)
    '''

    try:
        execute_batch(cursor, insert_sql, new_combinations, page_size=10000)
        print(f"{len(new_combinations)} records successfully inserted into lap_dim table.")
    except Exception as e:
        print(f"Error occurred while inserting data: {e}")
    finally:
        cursor.close()
        conn.close()


@task
def load_stop_data(df: pd.DataFrame):
    data_dict = df.to_dict(orient="records")
    
    try:
        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        conn.autocommit = True
        cursor = conn.cursor()
    except Exception as e:
        print(f"Error: Failed to connect to the database: {e}")
        return 

    select_sql = '''
        SELECT "stop", "raceId", "driverId" FROM stop_dim
    '''
    existing_stop = pd.read_sql(select_sql, conn).to_dict(orient="records")
    existing_combinations = {
        (int(data['stop']), int(data['raceId']), int(data['driverId'])) for data in existing_stop
    }

    select_race_sql = '''
        SELECT "raceId" FROM race_dim WHERE "round" = %s AND "year" = %s
    '''

    race_map_dict = {}
    for data in data_dict:
        race_name = data['round']
        season = data['season']
        
        cursor.execute(select_race_sql, (race_name, season))
        race_id_result = cursor.fetchone()

        if race_id_result:
            race_map_dict[(race_name, season)] = race_id_result[0]  
        else:
            print(f"Warning: No raceId found for {race_name} in {season}")


    select_driver_sql = '''
        SELECT "driverId" FROM driver_dim WHERE "driverRef" = %s
    '''

    driver_map_dict = {}

    for data in data_dict:
        

        driver_ref = data['driverId']
                
        cursor.execute(select_driver_sql, (driver_ref,))
        driver_id_result = cursor.fetchone()

        if driver_id_result:
            driver_map_dict[driver_ref] = driver_id_result[0] 
        else:
            print(f"Warning: No driverId found for {driver_ref} in race {data['round']}")

    new_combinations = []
    for data in data_dict:
        race_id = race_map_dict.get((data['round'], data['season'])) 
        
        lap_number = data['lap']
        stop = data['stop']
        driver_ref = data['driverId']
        duration = utils.convert_duration(data['duration']) 
        time = data['time']
        milliseconds = utils.duration_to_milliseconds(duration)
                    
        driver_id = driver_map_dict.get(driver_ref)
                    
        if race_id and driver_id:
                new_combinations.append(
                    (stop, race_id, driver_id, lap_number, time, duration,  milliseconds)
                )
    print("New combinations before filtering:", new_combinations)
    new_combinations = [
        combo for combo in new_combinations
        if (combo[0], combo[1], combo[2]) not in existing_combinations
    ]

    if not new_combinations:
        print("No new stops to insert.")
        return
    else:
        print(f"Found {len(new_combinations)} new stops to insert.")

    insert_sql = '''
        INSERT INTO stop_dim ("stop", "raceId", "driverId", "lap_pitstops", "time_pitstops", "duration", "milliseconds_pitstops")
        VALUES (%s, %s, %s, %s, %s, %s, %s)

    '''

    try:
        execute_batch(cursor, insert_sql, new_combinations, page_size=10000)
        print(f"{len(new_combinations)} records successfully inserted into stop_dim table.")
    except Exception as e:
        print(f"Error occurred while inserting data: {e}")
    finally:
        cursor.close()
        conn.close()


@task
def load_session_data(df: pd.DataFrame):
    data_dict = df.to_dict(orient="records")
    
    try:
        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        conn.autocommit = True
        cursor = conn.cursor()
    except Exception as e:
        print(f"Error: Failed to connect to the database: {e}")
        return
    select_race_sql = '''
        SELECT "raceId" FROM race_dim WHERE "name_x" = %s AND "year" = %s
    '''
    race_map_dict = {}
    for data in data_dict:
        race_name = data['raceName']
        season = data['season']
        
        cursor.execute(select_race_sql, (race_name, season))
        race_id_result = cursor.fetchone()

        if race_id_result:
            race_map_dict[(race_name, season)] = race_id_result[0]  
        else:
            print(f"Warning: No raceId found for {race_name} in {season}")

    new_combinations = []
    for data in data_dict:
        race_id = race_map_dict.get((data['raceName'], data['season'])) 
        if not race_id:
            continue  
        
        # Check if the session with the same raceId already exists in session_dim
        select_session_sql = '''
            SELECT 1 FROM session_dim WHERE "raceId" = %s 
            AND "fp1_date" = %s AND "fp1_time" = %s 
            AND "fp2_date" = %s AND "fp2_time" = %s
            AND "fp3_date" = %s AND "fp3_time" = %s 
            AND "quali_date" = %s AND "quali_time" = %s 
            AND "sprint_date" = %s AND "sprint_time" = %s
        '''
        
        cursor.execute(select_session_sql, (
            race_id, data['fp1_date'], data['fp1_time'],
            data['fp2_date'], data['fp2_time'],
            data['fp3_date'], data['fp3_time'],
            data['quali_date'], data['quali_time'],
            data['sprint_date'], data['sprint_time']
        ))
        
        session_exists = cursor.fetchone()
        
        if not session_exists:  
            new_combinations.append((
                race_id, data['fp1_date'], data['fp1_time'],
                data['fp2_date'], data['fp2_time'],
                data['fp3_date'], data['fp3_time'],
                data['quali_date'], data['quali_time'],
                data['sprint_date'], data['sprint_time']
            ))

    if not new_combinations:
        print("No new sessions to insert.")
        return
    else:
        print(f"Found {len(new_combinations)} new sessions to insert.")

    insert_sql = """
        INSERT INTO session_dim 
        ("raceId", "fp1_date", "fp1_time", "fp2_date", "fp2_time", "fp3_date", 
         "fp3_time", "quali_date", "quali_time", "sprint_date", "sprint_time")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        execute_batch(cursor, insert_sql, new_combinations, page_size=10000)
        print(f"{len(new_combinations)} records successfully inserted into session_dim table.")
    except Exception as e:
        print(f"Error occurred while inserting data: {e}")
    finally:
        cursor.close()
        conn.close()



@dag(start_date=datetime(2023, 11, 1), schedule="@daily", catchup=False, dag_id="kafka_consumer_dag")
def kafka_consumer_dag():
    start = DummyOperator(task_id='start_consuming')
    end = DummyOperator(task_id='end')

    process_driver_data_task = process_driver_data()
    process_circuit_data_task = process_circuit_data()
    process_constructor_data_task = process_constructor_data()
    process_status_data_task = process_status_data()
    process_driver_standings_data_task = process_driver_standings_data()
    process_constructor_standings_data_task = process_constructor_standings_data()
    process_race_data_task = process_race_data()
    process_session_data_task = process_session_data()
    process_lap_data_task = process_lap_data()
    process_stop_data_task = process_stop_data()
    process_result_data_task = process_result_data()

    load_driver_data_task = load_driver_data(process_driver_data_task)
    load_circuit_data_task = load_circuit_data(process_circuit_data_task)
    load_constructor_data_task = load_constructor_data(process_constructor_data_task)
    load_status_data_task = load_status_data(process_status_data_task)
    load_driver_standings_data_task = load_driver_standings_data(process_driver_standings_data_task)
    load_constructor_standings_data_task = load_constructor_standings_data(process_constructor_standings_data_task)
    load_race_data_task = load_race_data(process_race_data_task)
    load_session_data_task = load_session_data(process_session_data_task)
    load_lap_data_task = load_lap_data(process_lap_data_task)
    load_stop_data_task = load_stop_data(process_stop_data_task)
    load_result_data_task = load_result_data(process_result_data_task)

    start >> [
        process_driver_data_task,
        process_circuit_data_task,
        process_status_data_task,
        process_constructor_data_task,
        process_race_data_task,
        process_driver_standings_data_task,
        process_constructor_standings_data_task,
        process_lap_data_task,
        process_stop_data_task, process_session_data_task
    ]  >> process_result_data_task >> [
        load_driver_data_task,
        load_circuit_data_task,
        load_status_data_task,
        load_constructor_data_task
    ] >> load_race_data_task >> [
        load_session_data_task,
        load_driver_standings_data_task, 
        load_constructor_standings_data_task,
        load_lap_data_task,
        load_stop_data_task
    ] >> load_result_data_task >> end
     
  
kafka_consumer_dag()
