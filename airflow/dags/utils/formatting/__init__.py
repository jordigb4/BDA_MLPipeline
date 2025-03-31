from airflow.operators.python import PythonOperator # type:ignore
from dags.utils.postgres_utils import PostgresManager

from .weather_FR import format_weather

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

    return format_weather_task