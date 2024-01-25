# ETL Pipeline v1
by Sarah Haq

## Introduction
This code contains the steps to build an ETL pipeline that carries out the following tasks:
- Extracts 400k transactions from Redshift
- Identifies and removes duplicates
- Loads the transformed data to a s3 bucket

## Requirements
 The minimum requirements:
- Docker for Mac: [Docker >= 20.10.2](https://docs.docker.com/docker-for-mac/install/)
- Docker for Windows: 
  - Installation: [Docker](https://docs.docker.com/desktop/install/windows-install/)
  - Manual installation steps for older WSL version: [Docker WSL 2](https://learn.microsoft.com/en-us/windows/wsl/install-manual#step-4---download-the-linux-kernel-update-package)


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