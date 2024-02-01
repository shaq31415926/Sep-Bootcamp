# Information about the image we are using can be found here: https://github.com/puckel/docker-airflow
FROM puckel/docker-airflow

# creating a working directory in your docker image
WORKDIR /usr/local/airflow/

# install libraries that are not in the Docker image e.g. boto3
COPY requirements.txt .
RUN pip3 install -r requirements.txt


# copying everything from the dags folder into the image
COPY dags/. dags/

# copying everything from the dags folder into the image
COPY src dags/src/