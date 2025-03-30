from airflow.operators.python import PythonOperator # type:ignore
from .electricity_DL import load_data_electricity
from .traffic_acc_DL import load_data_traffic_acc
from .air_quality_DL import load_data_air
from .weather_DL import load_data_weather

from dags.utils import HDFSManager
from airflow import DAG

# Initialize the Client that connect to NameNode of the launched HDFS
hdfs_manager = HDFSManager()

def create_tasks(dag):
    ingest_weather_task = PythonOperator(
        task_id='ingest_weather',
        python_callable=load_data_weather,
        op_kwargs={
            'start_date': '2019-01-01',
            'end_date': '2024-03-31',
            'hdfs_manager': hdfs_manager},
        dag=dag
    )

    ingest_traffic_task = PythonOperator(
        task_id='ingest_traffic',
        python_callable=load_data_traffic_acc,
        op_kwargs={
            'start_date': '2019-01-01',
            'end_date': '2024-03-31',
            'hdfs_manager': hdfs_manager,
        },
        dag=dag
    )

    ingest_air_task = PythonOperator(
        task_id='ingest_air',
        python_callable=load_data_air,
        op_kwargs={
            'start_date': '2019-01-01',
            'end_date': '2024-03-31',
            'hdfs_manager': hdfs_manager,
        },
        dag=dag
    )

    ingest_electricity_task = PythonOperator(
        task_id='ingest_electricity',
        python_callable=load_data_electricity,
        op_kwargs={
            'start_date': '2019-01-01T00',
            'end_date': '2024-03-31T00',
            'hdfs_manager': hdfs_manager,
        },
        dag=dag
    )
    return ingest_weather_task, ingest_traffic_task, ingest_air_task, ingest_electricity_task