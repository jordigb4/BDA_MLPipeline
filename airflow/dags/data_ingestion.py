from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator # type:ignore

from utils import (load_data_weather,
                   load_traffic_acc_data,
                   load_data_air,
                   load_data_electricity,
                   WeatherStationId,
                   AirStationId,
                   HDFSManager
                   )

from dotenv import load_dotenv

load_dotenv(dotenv_path='/opt/airflow/.env')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

hdfs_manager = HDFSManager()

with DAG('Data_Processing_Pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    ingest_weather_task = PythonOperator(
        task_id='ingest_weather',
        python_callable=load_data_weather,
        op_kwargs={
            'hdfs_manager':hdfs_manager,
        }
    )
    ingest_traffic_task = PythonOperator(
        task_id='ingest_traffic',
        python_callable=load_traffic_acc_data,
        op_kwargs={
            'area':'Central',
            'start_date':'2019-01-01',
            'end_date':'2019-01-31',
            'output_dir':'./traffic_data',
            'hdfs_manager':hdfs_manager,
        }
    )
    ingest_air_task = PythonOperator(
        task_id='ingest_air',
        python_callable=load_data_air,
        op_kwargs={
            'station_id': AirStationId.RESEDA,
            'start_date': '2019-01-01',
            'end_date': '2019-03-31',
            'output_dir': './air_data',
            'hdfs_manager': hdfs_manager,
        }
    )
    ingest_electricity_task = PythonOperator(
        task_id='ingest_electricity',
        python_callable=load_data_electricity,
        op_kwargs={
            'start_date':'2019-01-01T00',
            'end_date':'2024-03-31T00',
            'output_dir': './air_data',
            'hdfs_manager': hdfs_manager,
        }
    )

    [ingest_weather_task, ingest_traffic_task, ingest_air_task,ingest_electricity_task]