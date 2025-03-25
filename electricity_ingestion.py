from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from hdfs import InsecureClient
import pandas as pd
from sqlalchemy import create_engine
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

HDFS_URL = 'http://namenode:50070'
HDFS_LANDING = '/data/electricity/landing'
HDFS_FORMATTED = '/data/electricity/formatted'
POSTGRES_CONN = 'postgresql://airflow:airflow@postgres:5432/electricitydb'

def ingest_electricity():
    client = InsecureClient(HDFS_URL, user='airflow')

    # Asegurar directorios en HDFS
    client.makedirs(HDFS_LANDING, permission=755)
    client.makedirs(HDFS_FORMATTED, permission=755)

    # Buscar archivo JSON en la landing zone
    files = client.list(HDFS_LANDING)
    json_files = [f for f in files if f.endswith('.json')]

    if not json_files:
        raise ValueError("No hay archivos JSON en la landing zone.")

    latest_json = json_files[-1]  # Coge el archivo más reciente
    json_path = f"{HDFS_LANDING}/{latest_json}"

    # Leer JSON desde HDFS
    with client.read(json_path) as reader:
        data = json.load(reader)

    df = pd.DataFrame(data)

    # Guardar como CSV en la formatted zone
    csv_path = f"{HDFS_FORMATTED}/{latest_json.replace('.json', '.csv')}"
    with client.write(csv_path, overwrite=True) as writer:
        df.to_csv(writer, index=False)

    print(f"Convertido {latest_json} a CSV en {csv_path}")

def transform_load_electricity():
    client = InsecureClient(HDFS_URL, user='airflow')

    # Leer el archivo CSV desde la formatted zone
    files = client.list(HDFS_FORMATTED)
    csv_files = [f for f in files if f.endswith('.csv')]

    if not csv_files:
        raise ValueError("No hay archivos CSV en la formatted zone.")

    latest_csv = csv_files[-1]
    csv_path = f"{HDFS_FORMATTED}/{latest_csv}"

    with client.read(csv_path) as reader:
        df = pd.read_csv(reader)

    # Transformaciones necesarias
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

    # Cargar en PostgreSQL
    engine = create_engine(POSTGRES_CONN)
    df.to_sql('electricity_data', engine, if_exists='append', index=False)

    print(f"Cargado {latest_csv} en PostgreSQL.")

with DAG('electricity_etl',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_electricity',
        python_callable=ingest_electricity
    )

    transform_load_task = PythonOperator(
        task_id='transform_load_electricity',
        python_callable=transform_load_electricity
    )

    ingest_task >> transform_load_task
