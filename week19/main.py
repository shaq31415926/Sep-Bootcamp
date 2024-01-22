import os
from src.extract import extract_transactional_data

# this library is being used to read from the .env file
from dotenv import load_dotenv
load_dotenv()

# reading the variables from the .env file
dbname = os.getenv("dbname")
port = os.getenv("port")
host = os.getenv("host")
user_name = os.getenv("user")
password = os.getenv("password")

# step 1: extract data
online_trans_transformed = extract_transactional_data(dbname, host, port, user_name, password)

# step 2: transform data
# step 3: load data to s3
