# This code will not work for students :(
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

import os
import pandas as pd
from load_data_to_s3 import connect_to_s3
from extract import connect_to_redshift

from dotenv import load_dotenv
load_dotenv()

# import variables from .env file
dbname = os.getenv("dbname")
host = os.getenv("host")
port = os.getenv("port")
user = os.getenv("user")
password = os.getenv("password")
aws_access_key_id = os.getenv("aws_access_key_id")
aws_secret_access_key_id = os.getenv("aws_secret_access_key_id")

s3_bucket = "sep-bootcamp"
key="etl-pipeline/sh_online_trans_transformed.csv"

# load this file to redshift
# read the s3 file and store as a data frame
s3_client = connect_to_s3(aws_access_key_id, aws_secret_access_key_id)
response = s3_client.get_object(Bucket=s3_bucket, Key=key)

if key[-4:] == '.pkl':
    cleaned_data = pd.read_pickle(response.get("Body"))

if key[-4:] == '.csv':
    cleaned_data = pd.read_csv(response.get("Body"))

print(cleaned_data.head())

# create table in redshift
# connect to redshift
connect = connect_to_redshift(dbname, host, port, user, password)
cursor = connect.cursor()
print(cursor)

table_destination = "bootcamp.online_transactions_fixed"

# create the table in sql
query = f"""
CREATE TABLE {table_destination} ( 
            invoice VARCHAR(256),
            stock_code VARCHAR(12),
            description VARCHAR(42),
            quantity INT8,
            invoice_date DATETIME,
            price FLOAT8,
            customer_id VARCHAR(6),
            country VARCHAR(20),
            total_order_value FLOAT8)
            ;
"""

# execute this is creating a table for the first time
cursor.execute(query)
connect.commit()

def load_from_s3_to_redshift(cursor,
                             table_destination, s3_bucket, key, aws_access_key_id,
                             aws_secret_access_key_id):
    query = f"""
        COPY
        {table_destination}
        FROM
        's3://{s3_bucket}/{key}'
        with credentials
            'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key_id}'
        """

    if key[-4:] == '.csv':
        query = query + """FORMAT AS CSV
                           IGNOREHEADER 1;"""

    if key[-4:] == '.pkl':
        query = query + """FORMAT AS PARQUET;"""


    cursor.execute(query)
    connect.commit()

# load the data from s3 to redshift
load_from_s3_to_redshift(cursor,
                         table_destination, s3_bucket, key, aws_access_key_id,
                         aws_secret_access_key_id)
print("The data is loaded to redshift")
