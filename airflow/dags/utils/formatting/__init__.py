from airflow import DAG
from airflow.operators.python import PythonOperator # type:ignore
from .weather_FR import format_weather

def create_tasks(dag):

    format_weather_task  = PythonOperator(
        task_id='format_weather',
        python_callable=format_weather,
        dag=dag
    )

    return format_weather_task