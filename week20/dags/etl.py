import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from src.extract import extract_transactional_data
from src.transform import identify_and_remove_duplicates
from src.load_data_to_s3 import df_to_s3


def extract_transform_load():
    # reading the variables from the .env file
    dbname = os.getenv("dbname")
    port = os.getenv("port")
    host = os.getenv("host")
    user_name = os.getenv("user")
    password = os.getenv("password")
    # defining the variables that will connect to s3
    key = "etl-pipeline-airflow/sh_online_trans_transformed.csv"
    s3_bucket = "sep-bootcamp"
    aws_access_key_id = os.getenv("aws_access_key_id")
    aws_secret_access_key = os.getenv("aws_secret_access_key_id")

    # step 1: extract and transform the data
    print("\nExtracting the data from redshift...")
    ot_transformed = extract_transactional_data(dbname, host, port, user_name, password)

    # step 2: identify and remove duplicate
    print("\nIdentifying and removing duplicates")
    ot_wout_duplicates = identify_and_remove_duplicates(ot_transformed)

    # step 3: load data to s3
    print("\nLoading data to s3 bucket")
    df_to_s3(ot_wout_duplicates, key, s3_bucket, aws_access_key_id, aws_secret_access_key)

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

