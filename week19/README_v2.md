# ETL Pipeline v1
by Sarah Haq

## Introduction
This code contains the steps to build an ETL pipeline that carries out the following tasks:
- Extracts 400k transactions from Redshift
- Identifies and removes duplicates
- Loads the transformed data to a s3 bucket

## Requirements
The minimum requirements:
- Docker

## Instructions on how to execute the code

1. Copy the `.env.copy` file to `.env`and fill out the environment variables.


2. Make sure you have Docker Desktop running. 

Create the image which carries out all the steps:
```
docker image build -t etl .
```

Run the etl pipeline using docker:
```
docker run --env-file .env etl
```