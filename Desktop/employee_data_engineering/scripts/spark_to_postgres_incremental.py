from pyspark.sql import SparkSession
import psycopg2
from psycopg2.extras import execute_values

PARQUET_PATH = "data/processed/employees_spark_parquet"

PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "employee_dw",
    "user": "harishkumarsure",
    "password": ""
}

# Columns in Postgres table (snake_case)
COLS = [
    "emp_no","first_name","last_name","gender","start_date","years",
    "department","country","center","monthly_salary","annual_salary",
    "job_rate","sick_leaves","unpaid_leaves","overtime_hours",
    "full_name","tenure_bucket","salary_band"
]

UPSERT_SQL = f"""
INSERT INTO employees ({",".join(COLS)})
VALUES %s
ON CONFLICT (emp_no) DO UPDATE SET
    first_name     = EXCLUDED.first_name,
    last_name      = EXCLUDED.last_name,
    gender         = EXCLUDED.gender,
    start_date     = EXCLUDED.start_date,
    years          = EXCLUDED.years,
    department     = EXCLUDED.department,
    country        = EXCLUDED.country,
    center         = EXCLUDED.center,
    monthly_salary = EXCLUDED.monthly_salary,
    annual_salary  = EXCLUDED.annual_salary,
    job_rate       = EXCLUDED.job_rate,
    sick_leaves    = EXCLUDED.sick_leaves,
    unpaid_leaves  = EXCLUDED.unpaid_leaves,
    overtime_hours = EXCLUDED.overtime_hours,
    full_name      = EXCLUDED.full_name,
    tenure_bucket  = EXCLUDED.tenure_bucket,
    salary_band    = EXCLUDED.salary_band;
"""

def main():
    spark = (
        SparkSession.builder
        .appName("spark-parquet-to-postgres")
        .master("local[*]")
        .getOrCreate()
    )

    df = spark.read.parquet(PARQUET_PATH)

    # rename columns from original names to snake_case to match Postgres
    df = (df
        .withColumnRenamed("No","emp_no")
        .withColumnRenamed("First Name","first_name")
        .withColumnRenamed("Last Name","last_name")
        .withColumnRenamed("Gender","gender")
        .withColumnRenamed("Start Date","start_date")
        .withColumnRenamed("Years","years")
        .withColumnRenamed("Department","department")
        .withColumnRenamed("Country","country")
        .withColumnRenamed("Center","center")
        .withColumnRenamed("Monthly Salary","monthly_salary")
        .withColumnRenamed("Annual Salary","annual_salary")
        .withColumnRenamed("Job Rate","job_rate")
        .withColumnRenamed("Sick Leaves","sick_leaves")
        .withColumnRenamed("Unpaid Leaves","unpaid_leaves")
        .withColumnRenamed("Overtime Hours","overtime_hours")
    )

    # Collect to Python (689 rows only — safe). For big data we’d do JDBC writes.
    rows = df.select(*COLS).collect()
    data = [tuple(r[c] for c in COLS) for r in rows]

    spark.stop()

    conn = psycopg2.connect(**PG_CONFIG)
    try:
        with conn.cursor() as cur:
            execute_values(cur, UPSERT_SQL, data, page_size=1000)
        conn.commit()

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM employees;")
            total = cur.fetchone()[0]

        print(f"✅ Spark→Postgres UPSERT complete. Rows in employees: {total}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
