# Project Instruction

In this project, you will build a ETL pipeline to extract, transform, and load rocket launch event data under a setup that mimics the enterprise data infrastructure. You will fetch the data from an external API service and store the raw data, and then convert the raw data into a structural data format, and finally export this data for downstream usage.

# Prerequisites

> You can skip this section if you already have these software installed on your machine.

Before starting this project, you will need to have **Docker** and **Docker Compose** installed on your local machine. You can simply install **[Docker Desktop](https://www.docker.com/products/docker-desktop/)* if you’re on a Windows or a MacOS. If you are on a Windows machine, I would recommend you to install [Git](https://git-scm.com/downloads/win), which provides you Git Bash to run the setup scripts in this project.

# What needs to be done in this project?

You will need to follow this instruction and the `TODO` comments to complete the following Airflow DAG: `dags/student/student_rocket_launch_etl.py`. The `TODO` comment tells what need to be implemented, and you will need to modify the DAG code, to complete the implementation. **There are in total 9** `TODO` **comments in the DAG code**.

The working DAG has also been attached to the project: `dags/rocket_launch_etl.py`. You can refer to this DAG file if you get stuck during the implementation, but I strongly recommend you to first implement the pipeline. In the following sections of this project instruction, this working DAG will be used to demonstrate the expected process.

# Infrastructure Overview

This section is to provide you context about the infrastructure that the `docker-compose.yaml` file setup and run on your local machine, which mimics the enterprise data infrastructure. You are not required to understand all the concepts in this section.

## Airflow Setup

First, Airflow is the orchestrator to schedule and run the ETL pipeline. For more details about this setup, you can refer to [Airflow’s official documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#running-airflow-in-docker). The `docker-compose.yaml` file in this project built on top of the one officially provided by Airflow. In this case, the Airflow is set up to use CeleryExecutor and runs as a distributed services.

## Datalake Setup

Another important component is the **Datalake**. In this project, all the data will be stored under the datalake. The diagram below demonstrate the components in the infrastructure. We will use **Trino** as query engine and **MinIO** as the underlying data storage (in real-world, this can be object storage such as AWS S3, Google Cloud Storage, and Azure ADLS Gen2). A **Hive Metastore** connects the query engine with the underlying object storage (i.e., MinIO), and allows us to query the data using SQL.

# Project Setup

Now, let’s start setting up this infrastructure, and run all the required services for the development.

First, run the setup script to create necessary folders and store environment variables in `.env` file.

```bash
chmod +x setup-script.sh
./setup-script.sh
```

Before starting the Airflow services, you need to run database migrations and create the first user account. To do this, run.

```bash
docker compose up airflow-init
```

After initialization is complete, you should see a message like this:

```bash
de-project-airflow-etl-airflow-init-1  | User "airflow" created with role "Admin"
de-project-airflow-etl-airflow-init-1  | 2.10.4
de-project-airflow-etl-airflow-init-1 exited with code 0
```

To start Airflow services together with all other services (i.e., Trino, MinIO), run.

```bash
docker compose up
```

# Project Setup - Linux

> You can skip this section if you are not on a Linux machine.

First, run the setup script to create necessary folders and store environment variables in `.env` file.

```bash
chmod +x linux-setup-script.sh
./linux-setup-script.sh
```

Before starting the Airflow services, you need to run database migrations and create the first user account. To do this, run.

```bash
docker compose up airflow-init
```

After initialization is complete, you should see a message like this:

```bash
de-project-airflow-etl-airflow-init-1  | User "airflow" created with role "Admin"
de-project-airflow-etl-airflow-init-1  | 2.10.4
de-project-airflow-etl-airflow-init-1 exited with code 0
```

To start Airflow services together with all other services (i.e., Trino, MinIO), run.

```bash
docker compose up
```