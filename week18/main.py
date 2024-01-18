import os
from dotenv import load_dotenv
load_dotenv()

from src.extract import connect_to_redshift

dbname = os.getenv("dbname")
port = os.getenv("port")
host = os.getenv("host")
user_name = os.getenv("user")
password = os.getenv("password")
print(user_name)

connect_to_redshift(dbname, host, port, user_name, password)
