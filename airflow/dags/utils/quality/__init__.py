from airflow import DAG
from airflow.operators.python import PythonOperator # type:ignore

from .weather_QL import quality_data
def create_tasks(dag):

    quality_weather_task  = PythonOperator(
        task_id='quality_weather',
        python_callable=quality_data,
        dag=dag
    )

    return quality_weather_task