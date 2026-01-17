from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Change this if your project folder name/path is different
PROJECT_DIR = "/Users/harishkumarsure/Desktop/employee_data_engineering"

default_args = {"owner": "harish"}

with DAG(
    dag_id="employee_data_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,          # run manually for now
    catchup=False,
    tags=["employee", "etl", "spark", "postgres"],
) as dag:

    transform = BashOperator(
        task_id="transform_csv",
        bash_command=f"cd {PROJECT_DIR} && python scripts/transform.py",
    )

    dq_report = BashOperator(
        task_id="data_quality_report",
        bash_command=f"cd {PROJECT_DIR} && python scripts/data_quality_report.py",
    )

    dq_to_pg = BashOperator(
        task_id="dq_results_to_postgres",
        bash_command=f"cd {PROJECT_DIR} && python scripts/dq_to_postgres.py",
    )

    spark_transform = BashOperator(
        task_id="spark_transform_to_parquet",
        bash_command=f"cd {PROJECT_DIR} && python scripts/spark_transform.py",
    )

    spark_to_pg = BashOperator(
        task_id="spark_parquet_to_postgres_upsert",
        bash_command=f"cd {PROJECT_DIR} && python scripts/spark_to_postgres_incremental.py",
    )

    transform >> dq_report >> dq_to_pg >> spark_transform >> spark_to_pg
