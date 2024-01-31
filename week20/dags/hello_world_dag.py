from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from src.hello_world import hello_world

dag = DAG("hello_world",
          description="This will print Hello World",
          schedule_interval = "28 11 * * *",
          start_date=datetime(2024,1,30),
          catchup=False
          )

start_dag = DummyOperator(task_id="start_dag", dag=dag)
print_hello = PythonOperator(task_id="hello_work_task",
               python_callable=hello_world,
               dag=dag
               )

end_dag = DummyOperator(task_id="end_dag", dag=dag)

start_dag >> print_hello >> end_dag


