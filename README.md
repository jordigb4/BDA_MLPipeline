# Large Scale Data Engineering Project for AI

## Description

A containerized data pipeline using Apache Airflow, PostgreSQL, Hadoop HDFS, and a Streamlit frontend for data visualization. Designed for orchestrating ETL workflows and managing data in a distributed environment.

```text
project-root/
│
├── airflow/                             # Airflow service setup and pipelines
│   ├── dags/                            # DAG definitions for data workflows
│   │   ├── landing/                     # Raw data ingestion
│   │   │   ├── __init__.py              # Creates tasks for every Data Loading function
│   │   │   ├── air_quality_DL.py
│   │   │   ├── electricity_DL.py
│   │   │   ├── traffic_acc_DL.py
│   │   │   ├── weather_DL.py
│   │   │   └── class_types.py           # Definition of common station properties
│   │   │
│   │   ├── formatting/                  # Data standardization
│   │   │   ├── __init__.py              # Creates tasks for every Formatting function
│   │   │   ├── air_quality_FR.py
│   │   │   ├── electricity_FR.py
│   │   │   ├── traffic_acc_FR.py
│   │   │   ├── weather_FR.py
│   │   │
│   │   ├── quality/                     # Data quality assurance
│   │   │   ├── __init__.py              # Creates tasks for every Quality function
│   │   │   ├── air_quality_QL.py
│   │   │   ├── electricity_QL.py
│   │   │   ├── traffic_acc_QL.py
│   │   │   ├── weather_QL.py
│   │   │   └── quality_utils.py
│   │   │
│   │   ├── exploitation/               # View creation for Data Analysis
│   │   │   ├── __init__.py
│   │   │   ├── air_electricity_weather.sql
│   │   │   ├── trafficAcc_weather.sql
│   │   │   └── weather_electricity.sql
│   │   │
│   │   ├── data_analysis/              # Data analysis tasks
│   │   │   ├── utils/                  # Creates tasks for every Data analysis functions
│   │   │   │   ├── __init__.py
│   │   │   │   ├── exp1_DA.py
│   │   │   │   ├── exp2_DA.py
│   │   │   │   └── exp3_DA.py          
│   │   │
│   │   ├── utils/                      # General utilities and orchestration DAGs
│   │   │   ├── __init__.py
│   │   │   └── mlpipeline.py           # *MAIN PIPELINE (DAG)*
│   │
│   ├── __init__.py
│   ├── .env                             # Environment variables and configs
│   ├── Dockerfile                       # Airflow service image
│   └── requirements.txt                 # Airflow Python dependencies
│
├── streamlit-app/                       # Interactive data dashboard
│   ├── app.py                           # Streamlit app entry point
│   ├── Dockerfile                       # Streamlit service image
│   ├── requirements.txt                 # Streamlit dependencies
│   └── utils.py                         # Dashboard utility functions
│
├── docker-compose.yaml                  # Project-wide service definitions
└── postgresql-42.7.3.jar                # JDBC driver for PostgreSQL
```
## Authors

-Alberto Jerez
-Jordi Granja
-Marta Carrión


