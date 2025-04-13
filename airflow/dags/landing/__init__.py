from airflow.operators.python import PythonOperator # type:ignore
from .electricity_DL import load_data_electricity
from .traffic_acc_DL import load_data_traffic_acc
from .air_quality_DL import load_data_air
from .weather_DL import load_data_weather

from dags.utils import HDFSManager
from airflow import DAG


hdfs_manager = HDFSManager()

def create_tasks(dag: DAG,
                 weather_start_date: str, weather_end_date: str,
                 traffic_start_date: str, traffic_end_date: str,
                 air_start_date: str, air_end_date: str,
                 electricity_start_date: str, electricity_end_date: str):

    ingest_weather_task = PythonOperator(
        task_id='ingest_weather',
        python_callable=load_data_weather,
        op_kwargs={
            'start_date': weather_start_date,
            'end_date': weather_end_date,
            'hdfs_manager': hdfs_manager
        },
        dag=dag
    )

    ingest_traffic_task = PythonOperator(
        task_id='ingest_traffic',
        python_callable=load_data_traffic_acc,
        op_kwargs={
            'start_date': traffic_start_date,
            'end_date': traffic_end_date,
            'hdfs_manager': hdfs_manager,
        },
        dag=dag
    )

    ingest_air_task = PythonOperator(
        task_id='ingest_air',
        python_callable=load_data_air,
        op_kwargs={
            'start_date': air_start_date,
            'end_date': air_end_date,
            'hdfs_manager': hdfs_manager,
        },
        dag=dag
    )

    ingest_electricity_task = PythonOperator(
        task_id='ingest_electricity',
        python_callable=load_data_electricity,
        op_kwargs={
            'start_date': electricity_start_date,
            'end_date': electricity_end_date,
            'hdfs_manager': hdfs_manager,
        },
        dag=dag
    )
    return ingest_weather_task, ingest_traffic_task, ingest_air_task, ingest_electricity_task