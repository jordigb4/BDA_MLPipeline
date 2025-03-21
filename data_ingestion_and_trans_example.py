from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from hdfs import InsecureClient
import pandas as pd
from sqlalchemy import create_engine

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}


def ingest_weather():
    client = InsecureClient('http://namenode:50070', user='airflow')
    client.makedirs('/data/weather', permission=755)

    # Sample data generation
    data = pd.DataFrame({
        'timestamp': [datetime.now()],
        'temperature': [25.5],
        'humidity': [60]
    })

    with client.write('/data/weather/raw.csv', overwrite=True) as writer:
        data.to_csv(writer, index=False)


def transform_load_weather():
    client = InsecureClient('http://namenode:50070', user='airflow')

    with client.read('/data/weather/raw.csv') as reader:
        df = pd.read_csv(reader)

    # Perform transformations
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Load to PostgreSQL
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/weatherdb')
    df.to_sql('weather_data', engine, if_exists='append', index=False)


with DAG('weather_etl',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    ingest_task = PythonOperator(
        task_id='ingest_weather',
        python_callable=ingest_weather
    )

    transform_load_task = PythonOperator(
        task_id='transform_load_weather',
        python_callable=transform_load_weather
    )

    ingest_task >> transform_load_task