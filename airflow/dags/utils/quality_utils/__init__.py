from airflow import DAG
from airflow.operators.python import PythonOperator # type:ignore
#from .weather_QL import quality_data # type:ignore
"""
def create_tasks(dag):

    quality_data_task  = PythonOperator(
        task_id='quality_checking',
        python_callable=quality_data,
        dag=dag
    )

    return quality_data_task"""