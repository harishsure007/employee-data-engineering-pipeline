# Employee Data Engineering Pipeline

End-to-end data engineering project demonstrating real-world ETL, data quality, Spark processing, and Airflow orchestration.

## 🔧 Tech Stack
- Python
- PostgreSQL
- Apache Spark (PySpark)
- Apache Airflow
- Docker
- Pandas

## 🏗 Architecture
Raw Data → Transform → Data Quality → Spark (Parquet Data Lake) → PostgreSQL (UPSERT) → Airflow Orchestration

## 📁 Project Structure

employee_data_engineering/
├── scripts/ # ETL, DQ, Spark, loaders
├── data/ # Raw & processed data
├── airflow_home/dags/ # Airflow DAGs
├── docker-compose.yaml # Dockerized Airflow
└── README.md

## 🚀 Key Features
- Step-2 data validation (profiling)
- Business rule transformations
- Automated data quality checks
- Incremental UPSERT into PostgreSQL
- Spark-based Parquet data lake
- Fully automated using Airflow (Dockerized)

## ▶ How to Run
1. Clone the repository
2. Start Airflow using Docker:
   ```bash
   docker compose up -d
