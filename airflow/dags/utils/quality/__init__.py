from airflow.operators.python import PythonOperator
from dags.utils.postgres_utils import PostgresManager

from .weather_QL import quality_weather

# Initialize Postgres Manager
postgres_manager = PostgresManager()


def create_tasks(dag):
    quality_weather_task = PythonOperator(
        task_id='quality_weather',
        python_callable=quality_weather,
        op_kwargs={
            'postgres_manager': postgres_manager},
        dag=dag
    )

    return quality_weather_task
