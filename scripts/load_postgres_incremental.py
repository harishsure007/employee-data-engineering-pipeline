import pandas as pd
import psycopg2
from io import StringIO

CSV_PATH = "data/processed/employees_clean.csv"

PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "employee_dw",
    "user": "harishkumarsure",
    "password": ""
}

rename_map = {
    "No": "emp_no",
    "First Name": "first_name",
    "Last Name": "last_name",
    "Gender": "gender",
    "Start Date": "start_date",
    "Years": "years",
    "Department": "department",
    "Country": "country",
    "Center": "center",
    "Monthly Salary": "monthly_salary",
    "Annual Salary": "annual_salary",
    "Job Rate": "job_rate",
    "Sick Leaves": "sick_leaves",
    "Unpaid Leaves": "unpaid_leaves",
    "Overtime Hours": "overtime_hours",
    "full_name": "full_name",
    "tenure_bucket": "tenure_bucket",
    "salary_band": "salary_band",
}

def copy_df(conn, df: pd.DataFrame, table: str):
    buf = StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)
    cols = ",".join(df.columns)
    sql = f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT CSV)"
    with conn.cursor() as cur:
        cur.copy_expert(sql, buf)

UPSERT_SQL = """
INSERT INTO employees AS tgt (
    emp_no, first_name, last_name, gender, start_date, years,
    department, country, center, monthly_salary, annual_salary,
    job_rate, sick_leaves, unpaid_leaves, overtime_hours,
    full_name, tenure_bucket, salary_band
)
SELECT
    emp_no, first_name, last_name, gender, start_date, years,
    department, country, center, monthly_salary, annual_salary,
    job_rate, sick_leaves, unpaid_leaves, overtime_hours,
    full_name, tenure_bucket, salary_band
FROM employees_staging
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
    df = pd.read_csv(CSV_PATH).rename(columns=rename_map)
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce").dt.date

    conn = psycopg2.connect(**PG_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE employees_staging;")
        conn.commit()

        copy_df(conn, df, "employees_staging")
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(UPSERT_SQL)
            affected = cur.rowcount
        conn.commit()

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM employees;")
            total = cur.fetchone()[0]

        print(f"✅ Incremental load complete. Upserted rows: {affected}. Total rows now: {total}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
