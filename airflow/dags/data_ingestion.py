from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow import DAG
import utils.landing_utils as landing_tasks
import utils.formatting_utils as formatting_tasks

load_dotenv(dotenv_path='/opt/airflow/.env')

default_args = {
    'owner': 'airflow', # Specifies the owner of the DAG
    'start_date': datetime(2023, 1, 1), # Defines when the DAG should first start running
    'retries': 2,  # Number of retries before failing permanently
    'retry_delay': timedelta(minutes=5),  # Wait 5 minutes between retries
    'execution_timeout': timedelta(minutes=30),  # Max allowed runtime per task

}

with DAG(
    'Data_Processing_Pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['data_processing'],
    default_view='graph',  # Default DAG display view
) as dag:

    # Ingestion tasks (save in HDFS)
    ingest_weather, ingest_traffic, ingest_air, ingest_electricity = landing_tasks.create_tasks(dag)

    # Formatting tasks
    format_weather = formatting_tasks.create_tasks(dag)


    [ingest_weather >> format_weather,ingest_traffic,ingest_air,ingest_electricity]