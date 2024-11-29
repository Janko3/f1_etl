import os
from airflow.decorators import task, dag
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch 
from dotenv import load_dotenv
import pyarrow.parquet as pq
import sys
sys.path.append('/opt/airflow/dags/programs/utils')
import utils
from airflow.operators.dummy import DummyOperator


load_dotenv(dotenv_path='.env')

file_path = os.getenv("FILE_PATH")
chunksize = int(os.getenv("CHUNKSIZE"))
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

    


@task
def extract_csv_in_chunks(file_path: str, chunksize: int):
    file_paths = [
        "./result.parquet", "./race.parquet", "./driver.parquet", "./circuit.parquet", "./status.parquet",
        "./constructor.parquet", "./session.parquet", "./lap.parquet", "./stop.parquet", 
        "./driver_standings.parquet", "./constructor_standings.parquet"
    ]
    utils.empty_files(file_paths)

    dtype_dict = {
    'position': 'string',
    'positionText': 'string',
    'milliseconds': 'string',
    'fastestLap': 'string',
    'fastestLapSpeed': 'string', 
    'alt': 'string',
    'number_drivers': 'string',
    'duration': 'string'
    
}

    chunk_iterator = pd.read_csv(file_path, chunksize=chunksize,dtype=dtype_dict)
    
 
    # Initialize lists to accumulate data
    result_data_list = []
    race_data_list = []
    driver_data_list = []
    circuit_data_list = []
    status_data_list = []
    constructor_data_list = []
    session_data_list = []
    lap_data_list = []
    stop_data_list = []
    driver_standings_data_list = []
    constructor_standings_data_list = []

    for chunk in chunk_iterator:
        # Accumulate data from each chunk
        
        result_data_list.append(chunk[["resultId", "driverId", "constructorId", "raceId", "statusId", "lap", "stop", 
                                       "driverStandingsId", "constructorStandingsId", "grid", "positionOrder", "positionText", 
                                       "position", "points", "time", "rank", "milliseconds", "fastestLapSpeed", "fastestLapTime", 
                                       "fastestLap","laps"]])
        race_data_list.append(chunk[["raceId", "circuitId", "year", "round", "name_x", "date", "time_races", "url_x" ]])
        driver_data_list.append(chunk[["driverId", "driverRef", "forename", "surname", "dob", "code", "url", "number","nationality","number_drivers"]])
        circuit_data_list.append(chunk[["circuitId", "circuitRef", "name_y", "url_y", "location", "country", "lat", "lng", "alt"]])
        status_data_list.append(chunk[["statusId", "status"]])
        constructor_data_list.append(chunk[["constructorId", "constructorRef", "name", "nationality_constructors", "url_constructors"]])
        session_data_list.append(chunk[["raceId", "fp1_date", "fp1_time", "fp2_date", "fp2_time", "fp3_date", "fp3_time", 
                                       "quali_date", "quali_time", "sprint_date", "sprint_time"]])
        lap_data_list.append(chunk[["lap", "raceId", "driverId", "position_laptimes", "time_laptimes", "milliseconds_laptimes"]])
        stop_data_list.append(chunk[["stop", "raceId", "driverId", "lap_pitstops", "time_pitstops", "duration", "milliseconds_pitstops"]])
        driver_standings_data_list.append(chunk[["driverStandingsId","driverId","raceId", "points_driverstandings", "wins", "position_driverstandings", 
                                                 "positionText_driverstandings"]])
        constructor_standings_data_list.append(chunk[["constructorStandingsId","constructorId", "raceId" , "points_constructorstandings", 
                                                     "wins_constructorstandings", "position_constructorstandings", 
                                                     "positionText_constructorstandings"]])

    dataframes = {
        "result": result_data_list,
        "race": race_data_list,
        "driver": driver_data_list,
        "circuit": circuit_data_list,
        "status": status_data_list,
        "constructor": constructor_data_list,
        "session": session_data_list,
        "lap": lap_data_list,
        "stop": stop_data_list,
        "driver_standings": driver_standings_data_list,
        "constructor_standings": constructor_standings_data_list
}


    for key, df_list in dataframes.items():
        combined_df = pd.concat(df_list)  
        file_path = f"./{key}.parquet"  
        combined_df.to_parquet(file_path, index=False, engine="pyarrow")
        print(f"Saved {key} to {file_path}")

@task
def process_race_data():
    file_path = './race.parquet'

    if os.path.exists(file_path):

        df = pd.read_parquet(file_path)

        df = df.drop_duplicates(subset=["raceId"], keep="first")
        df = df.replace(r'\N', None)

        with open(file_path, 'wb'):
            pass 

        df.to_parquet(file_path, index=False, engine="pyarrow")
    else:
        print(f"File {file_path} does not exist.")


@task
def process_result_data():
    file_path = './result.parquet'  
    if os.path.exists(file_path):

        df = pd.read_parquet(file_path)

        df= df.drop_duplicates(subset=["resultId"], keep="first")
        df = df.replace({r'\N': None, 'R': None})
        df = df.drop('positionText', axis=1)
        df = df.drop('position', axis=1)

        with open(file_path, 'wb'):
            pass 

        df.to_parquet(file_path, index=False, engine="pyarrow")
    else:
        print(f"File {file_path} does not exist.")

@task
def process_driver_data():
    file_path = './driver.parquet'  
    if os.path.exists(file_path):

        df = pd.read_parquet(file_path)

        df = df.drop_duplicates(subset=["driverId"], keep="first")
        df = df.replace(r'\N', None)

        with open(file_path, 'wb'):
            pass 

        df.to_parquet(file_path, index=False, engine="pyarrow")
    else:
        print(f"File {file_path} does not exist.")

@task
def process_circuit_data():
    file_path = './circuit.parquet'  
    if os.path.exists(file_path):

        df = pd.read_parquet(file_path)

        df = df.drop_duplicates(subset=["circuitId"], keep="first")
        df = df.replace(r'\N', None)

        with open(file_path, 'wb'):
            pass 

        df.to_parquet(file_path, index=False, engine="pyarrow")
    else:
        print(f"File {file_path} does not exist.")

@task
def process_constructor_data():
    file_path = './constructor.parquet'  
    if os.path.exists(file_path):

        df = pd.read_parquet(file_path)

        df = df.drop_duplicates(subset=["constructorId"], keep="first")
        df = df.replace(r'\N', None)

        with open(file_path, 'wb'):
            pass 

        df.to_parquet(file_path, index=False, engine="pyarrow")
    else:
        print(f"File {file_path} does not exist.")

@task
def process_status_data():
    file_path = './status.parquet'  
    if os.path.exists(file_path):

        df = pd.read_parquet(file_path)

        df = df.drop_duplicates(subset=["statusId"], keep="first")
        df = df.replace(r'\N', None)

        with open(file_path, 'wb'):
            pass 

        df.to_parquet(file_path, index=False, engine="pyarrow")
    else:
        print(f"File {file_path} does not exist.")

@task
def process_session_data():
    file_path = './session.parquet'  
    if os.path.exists(file_path):

        df = pd.read_parquet(file_path)

        df = df.drop_duplicates(subset=["raceId"], keep="first")
        df = df[~df.duplicated(subset=["fp1_date", "fp1_time", "fp2_date", "fp2_time", "fp3_date", "fp3_time", 
                               "quali_date", "quali_time", "sprint_date", "sprint_time"], keep=False)]

        df = df.replace(r'\N', None)

        with open(file_path, 'wb'):
            pass 

        df.to_parquet(file_path, index=False, engine="pyarrow")
    else:
        print(f"File {file_path} does not exist.")

@task
def process_lap_data():
    file_path = './lap.parquet'  
    if os.path.exists(file_path):

        df = pd.read_parquet(file_path)

        df = df.drop_duplicates(subset=["lap", "raceId", "driverId"], keep="first")
        df = df.replace(r'\N', None)

        with open(file_path, 'wb'):
            pass 

        df.to_parquet(file_path, index=False, engine="pyarrow")
    else:
        print(f"File {file_path} does not exist.")

@task
def process_stop_data():
    file_path = './stop.parquet'  
    if os.path.exists(file_path):

        df = pd.read_parquet(file_path)

        df = df.drop_duplicates(subset=["stop", "raceId", "driverId"], keep="first").replace('/N',None)
        df = df.replace(r'\N', None)
        df['duration'] = df['duration'].apply(utils.convert_duration)

        with open(file_path, 'wb'):
            pass 

        df.to_parquet(file_path, index=False, engine="pyarrow")
    else:
        print(f"File {file_path} does not exist.")

@task
def process_driver_standings_data():
    file_path = './driver_standings.parquet'  
    if os.path.exists(file_path):

        df = pd.read_parquet(file_path)

        df = df.drop_duplicates(subset=["driverStandingsId"], keep="first")
        df = df.drop('positionText_driverstandings', axis=1)
        df = df.replace(r'\N', None)

        with open(file_path, 'wb'):
            pass 

        df.to_parquet(file_path, index=False, engine="pyarrow")
    else:
        print(f"File {file_path} does not exist.")

@task
def process_constructor_standings_data():
    file_path = './constructor_standings.parquet'  
    if os.path.exists(file_path):

        df = pd.read_parquet(file_path)

        df = df.drop_duplicates(subset=["constructorStandingsId"], keep="first")
        df = df.drop('positionText_constructorstandings', axis=1)

        df = df.replace(r'\N', None)

        with open(file_path, 'wb'):
            pass 

        df.to_parquet(file_path, index=False, engine="pyarrow")
    else:
        print(f"File {file_path} does not exist.")

@task
def load_race_data():
   
    file_path = './race.parquet'  
    df = pd.read_parquet(file_path)
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

    data_tuples = [(record["raceId"], record["circuitId"], record["year"], record["round"], record["name_x"], 
                    record["date"], record["time_races"], record["url_x"]) 
                   for record in data_dict]

    try:
        execute_batch(cursor, insert_sql, data_tuples, page_size=10000)
        print(f"Data successfully inserted into race_dim table.")
    except Exception as e:
        print(f"Error occurred while inserting data: {e}")
    finally:
        cursor.close()
        conn.close()
@task
def load_driver_data():
    file_path = './driver.parquet'  
    df = pd.read_parquet(file_path)
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
    insert_sql = '''
        INSERT INTO driver_dim ("driverId", "driverRef", "forename", "surname", "dob", "code", "url", "number", "nationality", "number_drivers")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ("driverId") DO UPDATE
        SET "driverRef" = EXCLUDED."driverRef",
            "forename" = EXCLUDED."forename",
            "surname" = EXCLUDED."surname",
            "dob" = EXCLUDED."dob",
            "code" = EXCLUDED."code",
            "url" = EXCLUDED."url",
            "number" = EXCLUDED."number",
            "nationality" = EXCLUDED."nationality",
            "number_drivers" = EXCLUDED."number_drivers";
    '''

    data_tuples = [(record["driverId"], record["driverRef"], record["forename"], record["surname"], record["dob"], 
                    record["code"], record["url"], record["number"], record["nationality"], record["number_drivers"]) 
                   for record in data_dict]
    try:
        execute_batch(cursor, insert_sql, data_tuples, page_size=10000)
        print(f"Data successfully inserted into driver_dim table.")
    except Exception as e:
        print(f"Error occurred while inserting data: {e}")
    finally:
        cursor.close()
        conn.close()


@task
def load_result_data():
    file_path = './result.parquet'  
    df = pd.read_parquet(file_path)
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

    data_tuples = [(record["resultId"], record["driverId"], record["constructorId"], record["raceId"], record["statusId"], 
                        record["driverStandingsId"], record["constructorStandingsId"],record["grid"],record["positionOrder"],
                        record["points"], record["time"], record["rank"], record["milliseconds"], record["fastestLapSpeed"],record["fastestLapTime"],
                        record["fastestLap"], record["laps"]) 
                   for record in data_dict]
    try:
        execute_batch(cursor, insert_sql, data_tuples, page_size=10000)
        print(f"Data successfully inserted into result_fact table.")
    except Exception as e:
        print(f"Error occurred while inserting data: {e}")
    finally:
        cursor.close()
        conn.close()
@task
def load_circuit_data():
    file_path = './circuit.parquet'  
    df = pd.read_parquet(file_path)
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
    insert_sql = '''
        INSERT INTO circuit_dim ("circuitId", "circuitRef", "name_y", "url_y", "location", "country", "lat", "lng", "alt")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT ("circuitId") DO UPDATE
        SET "circuitRef" = EXCLUDED."circuitRef",
            "name_y" = EXCLUDED."name_y",
            "url_y" = EXCLUDED."url_y",
            "location" = EXCLUDED."location",
            "country" = EXCLUDED."country",
            "lat" = EXCLUDED."lat",
            "lng" = EXCLUDED."lng",
            "alt" = EXCLUDED."alt";

    '''

    data_tuples = [(record["circuitId"], record["circuitRef"], record["name_y"], record["url_y"], record["location"], 
                    record["country"], record["lat"], record["lng"], record["alt"]) 
                   for record in data_dict]
    try:
        execute_batch(cursor, insert_sql, data_tuples, page_size=10000)
        print(f"Data successfully inserted into circuit_dim table.")
    except Exception as e:
        print(f"Error occurred while inserting data: {e}")
    finally:
        cursor.close()
        conn.close()

@task
def load_status_data():
    file_path = './status.parquet'  
    df = pd.read_parquet(file_path)
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
    insert_sql = '''
        INSERT INTO status_dim ("statusId", "status")
        VALUES (%s, %s)
        ON CONFLICT ("statusId") DO UPDATE
        SET "status" = EXCLUDED."status";

    '''

    data_tuples = [(record["statusId"], record["status"]) 
                   for record in data_dict]
    try:
        execute_batch(cursor, insert_sql, data_tuples, page_size=10000)
        print(f"Data successfully inserted into status_dim table.")
    except Exception as e:
        print(f"Error occurred while inserting data: {e}")
    finally:
        cursor.close()
        conn.close()   
@task
def load_constructor_data():
    file_path = './constructor.parquet'  
    df = pd.read_parquet(file_path)
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
    insert_sql = '''
        INSERT INTO constructor_dim ("constructorId", "constructorRef", "name", "nationality_constructors", "url_constructors")
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT ("constructorId") DO UPDATE
        SET "constructorRef" = EXCLUDED."constructorRef",
            "name" = EXCLUDED."name",
            "nationality_constructors" = EXCLUDED."nationality_constructors",
            "url_constructors" = EXCLUDED."url_constructors";

    '''

    data_tuples = [(record["constructorId"], record["constructorRef"], record["name"], record["nationality_constructors"], record["url_constructors"]) 
                   for record in data_dict]
    try:
        execute_batch(cursor, insert_sql, data_tuples, page_size=10000)
        print(f"Data successfully inserted into constructor_dim table.")
    except Exception as e:
        print(f"Error occurred while inserting data: {e}")
    finally:
        cursor.close()
        conn.close() 
@task
def load_session_data():
    file_path = './session.parquet'  
    df = pd.read_parquet(file_path)
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
    insert_sql = '''
        INSERT INTO session_dim ("raceId", "fp1_date", "fp1_time", "fp2_date", "fp2_time", "fp3_date", "fp3_time", "quali_date", "quali_time", "sprint_date", "sprint_time")
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

    '''

    data_tuples = [(record["raceId"], record["fp1_date"], record["fp1_time"], record["fp2_date"], record["fp2_time"], 
                    record["fp3_date"], record["fp3_time"], record["quali_date"], record["quali_time"],record["sprint_date"],record["sprint_time"]) 
                   for record in data_dict]
    try:
        execute_batch(cursor, insert_sql, data_tuples, page_size=10000)
        print(f"Data successfully inserted into session_dim table.")
    except Exception as e:
        print(f"Error occurred while inserting data: {e}")
    finally:
        cursor.close()
        conn.close()
@task
def load_lap_data():
    file_path = './lap.parquet'
    temp_csv_file = 'temp_lap_data.csv'

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

    try:
        df = pd.read_parquet(file_path, engine='pyarrow')
        df.to_csv(temp_csv_file, index=False, header=False)  

        with open(temp_csv_file, 'r') as f:
            cursor.copy_expert("""
                COPY lap_dim ("lap", "raceId", "driverId", "position_laptimes", "time_laptimes", "milliseconds_laptimes")
                FROM STDIN WITH CSV DELIMITER ',' 
            """, f)
        print(f"Data successfully loaded into lap_dim table.")

    except Exception as e:
        print(f"Error while reading the Parquet file or loading data: {e}")
    finally:
        os.remove(temp_csv_file)  
        cursor.close()
        conn.close()

@task
def load_stop_data():
    file_path = './stop.parquet'  
    df = pd.read_parquet(file_path)
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
    insert_sql = '''
        INSERT INTO stop_dim ("stop", "raceId", "driverId", "lap_pitstops", "time_pitstops", "duration", "milliseconds_pitstops")
        VALUES (%s, %s, %s, %s, %s, %s, %s)

    '''

    data_tuples = [
        (record["stop"], record["raceId"], record["driverId"], record["lap_pitstops"], record["time_pitstops"], record["duration"], record["milliseconds_pitstops"])
        for record in data_dict
    ]
    try:
        execute_batch(cursor, insert_sql, data_tuples, page_size=10000)
        print(f"Data successfully inserted into stop_dim table.")
    except Exception as e:
        print(f"Error occurred while inserting data: {e}")
    finally:
        cursor.close()
        conn.close() 
@task
def load_driver_standings_data():
    file_path = './driver_standings.parquet'  
    df = pd.read_parquet(file_path)
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
    insert_sql = '''
        INSERT INTO driver_standings_dim ("driverStandingsId","driverId", "raceId", "points_driverstandings", "wins", "position_driverstandings")
        VALUES (%s, %s, %s, %s,%s, %s)
        ON CONFLICT ("driverStandingsId") DO UPDATE
        SET "driverId" = EXCLUDED."driverId",
            "raceId" = EXCLUDED."raceId",
            "points_driverstandings" = EXCLUDED."points_driverstandings",
            "wins" = EXCLUDED."wins",
            "position_driverstandings" = EXCLUDED."position_driverstandings";

    '''

    data_tuples = [(record["driverStandingsId"],record["driverId"],record["raceId"], record["points_driverstandings"], record["wins"], record["position_driverstandings"]) 
                   for record in data_dict]
    try:
        execute_batch(cursor, insert_sql, data_tuples, page_size=10000)
        print(f"Data successfully inserted into driver_standings_dim table.")
    except Exception as e:
        print(f"Error occurred while inserting data: {e}")
    finally:
        cursor.close()
        conn.close()
@task
def load_constructor_standings_data():
    file_path = './constructor_standings.parquet'  
    df = pd.read_parquet(file_path)
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
    insert_sql = '''
       INSERT INTO constructor_standings_dim ("constructorStandingsId","constructorId","raceId", "points_constructorstandings", "wins_constructorstandings", "position_constructorstandings")
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT ("constructorStandingsId") DO UPDATE
        SET "constructorId" = EXCLUDED."constructorId",
            "raceId" = EXCLUDED."raceId",
            "points_constructorstandings" = EXCLUDED."points_constructorstandings",
            "wins_constructorstandings" = EXCLUDED."wins_constructorstandings",
            "position_constructorstandings" = EXCLUDED."position_constructorstandings";

    '''

    data_tuples = [(record["constructorStandingsId"],record["constructorId"],record["raceId"], record["points_constructorstandings"], record["wins_constructorstandings"], record["position_constructorstandings"]) 
                   for record in data_dict]
    try:
        execute_batch(cursor, insert_sql, data_tuples, page_size=10000)
        print(f"Data successfully inserted into constructor_standings_dim table.")
    except Exception as e:
        print(f"Error occurred while inserting data: {e}")
    finally:
        cursor.close()
        conn.close()


@dag(start_date=datetime(2023, 11, 1), schedule="@daily", catchup=False,dag_id ='etl_dag')
def dag_pipeline_etl():
    start = DummyOperator(task_id='start')
    
    extract_csv_task =extract_csv_in_chunks(file_path, chunksize)
    with TaskGroup("process_data") as process_group:
        process_circuit_task = process_circuit_data()
        process_constructor_task = process_constructor_data()
        process_constructor_standings_task = process_constructor_standings_data()
        process_driver_task = process_driver_data()
        process_driver_standings_task = process_driver_standings_data()
        process_lap_task = process_lap_data()
        process_session_task = process_session_data()
        process_stop_task = process_stop_data()
        process_result_task = process_result_data()
        process_race_task = process_race_data()
        process_status_task = process_status_data()

    with TaskGroup("load_data") as load_group:
        load_status_task = load_status_data()
        load_constructor_task = load_constructor_data()
        load_driver_task = load_driver_data()
        load_constructor_standings_task = load_constructor_standings_data()
        load_driver_standings_task = load_driver_standings_data()
        load_circuit_task = load_circuit_data()
    end = DummyOperator(task_id='end')

    load_race_task = load_race_data()
    load_session_task = load_session_data()
    load_stop_task = load_stop_data()
    load_lap_task = load_lap_data()
    load_result_task = load_result_data()    

    # Define task dependencies with task groups
    start >> extract_csv_task >> process_group >> load_group

    # Parallel loading of independent dimensions
    
    load_group >> load_stop_task
    load_group >> load_lap_task

    # Load the race and constructor data
    load_group >> load_race_task
    load_race_task >> load_stop_task
    load_race_task >> load_lap_task
    load_race_task >> load_session_task
    # Ensure the proper order for fact table loading
    load_race_task >> load_result_task
    load_status_task >> load_result_task
    load_constructor_task >> load_result_task
    load_driver_task >> load_result_task
    load_driver_standings_task >> load_result_task
    load_constructor_standings_task >> load_result_task
    
    [load_stop_task, load_lap_task, load_session_task, load_result_task] >> end
    
dag_pipeline_etl = dag_pipeline_etl()
