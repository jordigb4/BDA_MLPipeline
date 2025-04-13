from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow import DAG
import landing as landing_tasks
import formatting as formatting_tasks
import quality as quality_tasks
import exploitation as exploitation_tasks

load_dotenv(dotenv_path='/opt/airflow/.env')

default_args = {
    'owner': 'airflow', # Specifies the owner of the DAG
    'start_date': datetime(2023, 1, 1), # Defines when the DAG should first start running
    'retries': 2,  # Number of retries before failing permanently
    'retry_delay': timedelta(minutes=2),  # Wait 5 minutes between retries
    'execution_timeout': timedelta(minutes=30),  # Max allowed runtime per task

}

with DAG(
    'Machine_Learning_Pipeline',
    default_args=default_args,
    schedule_interval=None, # Ideally, we would put it in daily!
    catchup=False,
    tags=['mlpipeline'],
    default_view='graph',  # Default DAG display view
) as dag:

    # Ingestion tasks (save in HDFS)
    ingest_weather, ingest_traffic, ingest_air, ingest_electricity = landing_tasks.create_tasks(dag)
    #ingest_weather,ingest_electricity= landing_tasks.create_tasks(dag)

    # Formatting tasks
    #format_weather,format_electricity = formatting_tasks.create_tasks(dag)
    format_weather, format_air, format_traffic, format_electricity = formatting_tasks.create_tasks(dag)

    # Quality tasks
    #quality_weather, quality_electricity = quality_tasks.create_tasks(dag)
    quality_weather, quality_air, quality_traffic, quality_electricity = quality_tasks.create_tasks(dag)

    # Exploitation tasks
    weather_electricity, air_electricity_weather, trafficAcc_weather = exploitation_tasks.create_tasks(dag)

    ingest_weather >> format_weather >> quality_weather
    ingest_traffic >> format_traffic >> quality_traffic
    ingest_air >> format_air >> quality_air
    ingest_electricity >> format_electricity >> quality_electricity

    # Set all downstream tasks to run in parallel AFTER all quality tasks complete
    [quality_weather, quality_electricity] >> weather_electricity
    [quality_weather, quality_air, quality_electricity] >> air_electricity_weather
    [quality_weather, quality_traffic] >> trafficAcc_weather