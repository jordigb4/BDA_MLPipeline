from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow import DAG
import landing as landing_tasks
import formatting as formatting_tasks
import quality as quality_tasks
import exploitation as exploitation_tasks
import data_analysis as data_analysis_tasks

load_dotenv(dotenv_path='/opt/airflow/.env')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=30),

}

with DAG(
    'Machine_Learning_Pipeline',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
    tags=['mlpipeline'],
    default_view='graph',
) as dag:

    # Template for airflow to complete at runtime
    start_date_daily_template = "{{ data_interval_start.strftime('%Y-%m-%d') }}"
    end_date_daily_template = "{{ (data_interval_end - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"

    start_date_hourly_template = "{{ data_interval_start.strftime('%Y-%m-%dT%H') }}"
    end_date_hourly_template = "{{ data_interval_end.strftime('%Y-%m-%dT%H') }}"

    # Ingestion tasks -> HDFS
    ingest_weather, ingest_traffic, ingest_air, ingest_electricity = landing_tasks.create_tasks(
        dag=dag,
        weather_start_date=start_date_daily_template,
        weather_end_date=end_date_daily_template,
        traffic_start_date=start_date_daily_template,
        traffic_end_date=end_date_daily_template,
        air_start_date=start_date_daily_template,
        air_end_date=end_date_daily_template,
        electricity_start_date=start_date_hourly_template,
        electricity_end_date=end_date_hourly_template,
    )

    # ingest_weather, ingest_traffic, ingest_air, ingest_electricity = landing_tasks.create_tasks(
    #     dag=dag,
    #     weather_start_date='2019-01-01',
    #     weather_end_date='2024-03-31',
    #     traffic_start_date='2019-01-01',
    #     traffic_end_date='2024-03-31',
    #     air_start_date='2019-01-01',
    #     air_end_date='2024-03-31',
    #     electricity_start_date='2019-01-01T00',
    #     electricity_end_date='2024-03-31T00',
    # )

    # Formatting tasks
    format_weather, format_air, format_traffic, format_electricity = formatting_tasks.create_tasks(dag)

    # Quality tasks
    quality_weather, quality_air, quality_traffic, quality_electricity = quality_tasks.create_tasks(dag)

    # Exploitation tasks
    weather_electricity, air_electricity_weather, trafficAcc_weather = exploitation_tasks.create_tasks(dag)

    # Data Anlysis Tasks
    data_analysis_1_task, data_analysis_2_task, data_analysis_3_task = data_analysis_tasks.create_tasks(dag)

    # Parallel ingestion to quality
    ingest_weather >> format_weather >> quality_weather
    ingest_traffic >> format_traffic >> quality_traffic
    ingest_air >> format_air >> quality_air
    ingest_electricity >> format_electricity >> quality_electricity

    # Set all downstream tasks to run in parallel AFTER all quality tasks complete
    [quality_weather, quality_electricity] >> weather_electricity
    [quality_weather, quality_air, quality_electricity] >> air_electricity_weather
    [quality_weather, quality_traffic] >> trafficAcc_weather

    # Do data analysis
    weather_electricity >> data_analysis_1_task
    air_electricity_weather >> data_analysis_2_task
    trafficAcc_weather >> data_analysis_3_task
