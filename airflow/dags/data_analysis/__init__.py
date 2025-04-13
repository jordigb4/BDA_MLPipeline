from airflow.operators.python import PythonOperator # type:ignore
from dags.utils.postgres_utils import PostgresManager
from dags.utils.hdfs_utils import HDFSManager
from .exp1_DA import data_analysis_1
from .exp2_DA import data_analysis_2
from .exp3_DA import data_analysis_3

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

    data_analysis_2_task = PythonOperator(
        task_id='data_analysis_exp2',
        python_callable=data_analysis_2,
        op_kwargs={
            'hdfs_manager': hdfs_manager,
            'postgres_manager': postgres_manager},
        dag=dag
    )

    data_analysis_3_task = PythonOperator(
        task_id='data_analysis_exp3',
        python_callable=data_analysis_3,
        op_kwargs={
            'hdfs_manager': hdfs_manager,
            'postgres_manager': postgres_manager},
        dag=dag
    )

    return data_analysis_1_task, data_analysis_2_task, data_analysis_3_task