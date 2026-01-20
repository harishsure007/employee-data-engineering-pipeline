import pandas as pd
import psycopg2
from io import StringIO

CSV_PATH = "data/processed/employees_clean.csv"

# On Mac Homebrew Postgres, user is often your mac username
PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "employee_dw",
    "user": "harishkumarsure",
    "password": ""  # keep empty unless you explicitly set a password
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

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS employees (
    emp_no INT,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    start_date DATE,
    years INT,
    department TEXT,
    country TEXT,
    center TEXT,
    monthly_salary INT,
    annual_salary INT,
    job_rate DOUBLE PRECISION,
    sick_leaves INT,
    unpaid_leaves INT,
    overtime_hours INT,
    full_name TEXT,
    tenure_bucket TEXT,
    salary_band TEXT
);
"""

def copy_df(conn, df: pd.DataFrame, table: str):
    buf = StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)
    cols = ",".join(df.columns)
    sql = f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT CSV)"
    with conn.cursor() as cur:
        cur.copy_expert(sql, buf)

def main():
    df = pd.read_csv(CSV_PATH).rename(columns=rename_map)
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce").dt.date

    conn = psycopg2.connect(**PG_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE)
            cur.execute("TRUNCATE TABLE employees;")
        conn.commit()

        copy_df(conn, df, "employees")
        conn.commit()

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM employees;")
            print("✅ Loaded rows:", cur.fetchone()[0])
    finally:
        conn.close()

if __name__ == "__main__":
    main()
