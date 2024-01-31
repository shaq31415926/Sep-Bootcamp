# Airflow Setup


## Requirements
The minimum requirements:
- Docker for Mac: [Docker >= 20.10.2](https://docs.docker.com/docker-for-mac/install/)
- Docker for Windows: 
  - Installation: [Docker](https://docs.docker.com/desktop/install/windows-install/)
  - Manual installation steps for older WSL version: [Docker WSL 2](https://learn.microsoft.com/en-us/windows/wsl/install-manual#step-4---download-the-linux-kernel-update-package)


## Instructions

1. Create a DockerFile which fetches a docker image to setup airflow on your local machine:

```FROM puckel/docker-airflow```

Information about the image we are using can be found here: https://github.com/puckel/docker-airflow

2. Build a docker image called `local-airflow` on your local machine:

```docker image build -t local-airflow .```

3. Run airflow on port number 8080:
```docker run -d -p 8080:8080 local-airflow webserver```


4. Access airflow webserver at http://localhost:8080