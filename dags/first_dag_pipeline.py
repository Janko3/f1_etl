from datetime import datetime
import os
from airflow.decorators import task, dag
import psycopg2
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dotenv import load_dotenv

load_dotenv(dotenv_path='.env')

file_path = os.getenv("FILE_PATH")
chunksize = int(os.getenv("CHUNKSIZE"))
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

@task
def create_tables():
    conn = psycopg2.connect(
        host=db_host,
        dbname=db_name,
        user=db_user,
        password=db_password
    )
    print('DB connected successfully')
    with open('create_tables.sql', 'r') as file:
        create_sql = file.read()

    tables = ["race_dim", "circuit_dim", "session_dim", "constructor_standings_dim", "driver_standings_dim", "driver_dim",
               "constructor_dim", "status_dim", "lap_dim", "stop_dim", "result_fact"] 

    cursor = conn.cursor()

    for table in tables:
        cursor.execute(f"""
            SELECT to_regclass('{table}');
        """)
        result = cursor.fetchone()
        if result[0] is not None:
            print(f"Table {table} exists, dropping it.")
            cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")

    conn.commit()
    
    cursor.execute(create_sql)
    conn.commit()

    print('Tables created successfully')
    cursor.close()

@dag(
    dag_id='create_tables_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['create_tables']
)
def create_tables_dag():
    start = DummyOperator(task_id='start')
    create = create_tables()
    trigger_second_dag = TriggerDagRunOperator(
        task_id='trigger_second_dag',
        trigger_dag_id='etl_dag',  
        wait_for_completion=True  
    )

    start >> create >> trigger_second_dag

create_tables_dag()