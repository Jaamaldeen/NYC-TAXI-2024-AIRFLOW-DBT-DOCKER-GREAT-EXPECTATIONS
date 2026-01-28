# NYC-TAXI-2024-AIRFLOW-DBT-DOCKER-GREAT-EXPECTATIONS 

This project builds a Lakehouse-style data pipeline for the NYC Taxi 2024 dataset using Apache Airflow for orchestration, dbt for transformations, Great Expectations for data quality, and Docker for reproducible execution.

## *Project Architecture.*

![project](https://github.com/user-attachments/assets/b7b11383-ecc6-4ed6-8d3c-7682296a15a1)


## *Project Structure.*

```text
project-root/
├── dags/
│   ├── nyc_taxi_pipeline.py
│   ├── failure_callbacks.py
│   └── validation_utils.py
├── dbt/
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   ├── tests/
│   └── dbt_project.yml
├── tests/
│   └── test_pipeline_logic.py
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```


## *Tech Stack*
  
  Orchestration: Apache Airflow 3.1.6
  
  Transformations: dbt
  
  Data Quality: Great Expectations + dbt tests
  
  Database: PostgreSQL
  
  Containerization: Docker & Docker Compose
  
  CI/CD: GitHub Actions
  
  Alerting: Slack

## *Key Features*
  
  Metadata-driven monthly incremental loading
  
  Automatic retries and failure handling with Airflow
  
  Slack alerts via on_failure_callback
  
  dbt tests and Great Expectations quality gates
  
  Fully containerized, reproducible setup
  
  CI/CD checks for pipeline reliability

## *Data Processing*

Bronze: Incremental monthly loads with dbt tests on critical columns

Silver: Cleaned and enriched data with dbt + Great Expectations validation

Gold: Aggregated, analytics-ready models for reporting

To read more about this project check this meduim post

