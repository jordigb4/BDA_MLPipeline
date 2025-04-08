from airflow.operators.python import PythonOperator # type:ignore
from dags.utils.postgres_utils import PostgresManager

from .weather_QL import quality_weather
from .air_quality_QL import quality_air
from .traffic_acc_QL import quality_traffic

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

    quality_air_task = PythonOperator(
        task_id='quality_air',
        python_callable=quality_air,
        op_kwargs={
            'postgres_manager': postgres_manager},
        dag=dag
    )

    quality_traffic_task = PythonOperator(
        task_id='quality_traffic',
        python_callable=quality_traffic,
        op_kwargs={
            'postgres_manager': postgres_manager},
        dag=dag
    )

    return quality_weather_task, quality_air_task, quality_traffic_task