from airflow.operators.python import PythonOperator # type:ignore
from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow import DAG
from utils import *

load_dotenv(dotenv_path='/opt/airflow/.env')

default_args = {
    'owner': 'airflow', # Specifies the owner of the DAG
    'start_date': datetime(2023, 1, 1), # Defines when the DAG should first start running
    'retries': 2,  # Number of retries before failing permanently
    'retry_delay': timedelta(minutes=5),  # Wait 5 minutes between retries
    'execution_timeout': timedelta(minutes=30),  # Max allowed runtime per task
    #'email': ['your_team@domain.com'],  # Notification email
    #'email_on_failure': True,  # Send email on task failure
}

# Initialize the Client that connect to NameNode of the launched HDFS
hdfs_manager = HDFSManager()

with DAG(
    'Data_Processing_Pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['data_processing'],
    default_view='graph',  # Default DAG display view
) as dag:

    ingest_weather_task = PythonOperator(
        task_id='ingest_weather',
        python_callable=load_data_weather,
        op_kwargs={
            'start_date': '2019-01-01',
            'end_date': '2024-03-31',
            'hdfs_manager': hdfs_manager}
    )

    ingest_traffic_task = PythonOperator(
        task_id='ingest_traffic',
        python_callable=load_data_traffic_acc,
        op_kwargs={
            'start_date': '2019-01-01',
            'end_date': '2024-03-31',
            'hdfs_manager': hdfs_manager,
        }
    )

    ingest_air_task = PythonOperator(
        task_id='ingest_air',
        python_callable=load_data_air,
        op_kwargs={
            'start_date': '2019-01-01',
            'end_date': '2024-03-31',
            'hdfs_manager': hdfs_manager,
        }
    )

    ingest_electricity_task = PythonOperator(
        task_id='ingest_electricity',
        python_callable=load_data_electricity,
        op_kwargs={
            'start_date': '2019-01-01T00',
            'end_date': '2024-03-31T00',
            'hdfs_manager': hdfs_manager,
        }
    )

    [ingest_weather_task, ingest_traffic_task, ingest_air_task, ingest_electricity_task]