from airflow.operators.python import PythonOperator # type:ignore
from dags.utils.postgres_utils import PostgresManager

from .weather_FR import format_weather
from .air_quality_FR import format_air_quality
from .traffic_acc_FR import format_traffic_acc
from .electricity_FR import format_electricity_data

# Initialize Postgres Manager
postgres_manager = PostgresManager()


def create_tasks(dag):
    format_weather_task = PythonOperator(
        task_id='format_weather',
        python_callable=format_weather,
        op_kwargs={
            'postgres_manager': postgres_manager},
        dag=dag
    )

    format_air_task = PythonOperator(
        task_id='format_air_quality',
        python_callable=format_air_quality,
        op_kwargs={
            'postgres_manager': postgres_manager},
        dag=dag
    )

    format_traffic_task = PythonOperator(
        task_id='format_traffic_acc',
        python_callable=format_traffic_acc,
        op_kwargs={
            'postgres_manager': postgres_manager},
        dag=dag
    )

    format_electricity_task = PythonOperator(
        task_id='format_electricity',
        python_callable=format_electricity_data,
        op_kwargs={
            'postgres_manager': postgres_manager},
        dag=dag
    )

    return format_weather_task, format_air_task, format_traffic_task, format_electricity_task