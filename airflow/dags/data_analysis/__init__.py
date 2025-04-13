from airflow.operators.python import PythonOperator
from dags.utils.postgres_utils import PostgresManager
from dags.utils.hdfs_utils import HDFSManager
from .exp1_DA import data_analysis_1


# Initialize Postgres Manager
postgres_manager = PostgresManager()
hdfs_manager = HDFSManager()

def create_tasks(dag):

    data_analysis_1_task = PythonOperator(
        task_id='data_analysis_exp1',
        python_callable=data_analysis_1,
        op_kwargs={
            'hdfs_manager': hdfs_manager,
            'postgres_manager': postgres_manager},
        dag=dag
    )


    return data_analysis_1_task