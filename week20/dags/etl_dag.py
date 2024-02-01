from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from src.extract_transform_load import extract_transform_load


default_args = {
    'owner': 'shaq',
    'start_date':datetime(2024,1,31)}

dag = DAG("etl-pipeline",
          description="This will extract transform and load the data",
          schedule_interval = "@daily",
          default_args=default_args
          )

start_dag = DummyOperator(task_id="start_dag", dag=dag)
etl = PythonOperator(task_id="etl_task",
               python_callable=extract_transform_load,
               dag=dag
               )

end_dag = DummyOperator(task_id="end_dag", dag=dag)

start_dag >> etl >> end_dag

