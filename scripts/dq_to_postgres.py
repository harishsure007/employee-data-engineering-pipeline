import pandas as pd
import psycopg2
from datetime import datetime

PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "employee_dw",
    "user": "harishkumarsure",
    "password": ""
}

SUMMARY_CSV = "data/processed/dq_summary.csv"
RULES_CSV = "data/processed/dq_rules.csv"

CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS dq_runs (
    run_id BIGSERIAL PRIMARY KEY,
    run_ts TIMESTAMP NOT NULL,
    rows INT,
    cols INT,
    duplicate_key TEXT,
    duplicate_rows INT,
    start_date_min TIMESTAMP NULL,
    start_date_max TIMESTAMP NULL,
    monthly_salary_min INT,
    monthly_salary_max INT,
    annual_salary_min INT,
    annual_salary_max INT,
    overtime_hours_min INT,
    overtime_hours_max INT,
    sick_leaves_min INT,
    sick_leaves_max INT,
    unpaid_leaves_min INT,
    unpaid_leaves_max INT,
    job_rate_min DOUBLE PRECISION,
    job_rate_max DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS dq_rule_results (
    run_id BIGINT REFERENCES dq_runs(run_id),
    rule_name TEXT,
    violations INT,
    PRIMARY KEY (run_id, rule_name)
);
"""

def main():
    summary = pd.read_csv(SUMMARY_CSV).iloc[0].to_dict()
    rules = pd.read_csv(RULES_CSV)

    # parse timestamps (they are stored as strings in CSV)
    for k in ["start_date_min", "start_date_max"]:
        if k in summary and isinstance(summary[k], str) and summary[k].strip():
            summary[k] = pd.to_datetime(summary[k], errors="coerce")
        else:
            summary[k] = None

    conn = psycopg2.connect(**PG_CONFIG)
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLES)

            run_ts = datetime.now()

            cur.execute(
                """
                INSERT INTO dq_runs (
                    run_ts, rows, cols, duplicate_key, duplicate_rows,
                    start_date_min, start_date_max,
                    monthly_salary_min, monthly_salary_max,
                    annual_salary_min, annual_salary_max,
                    overtime_hours_min, overtime_hours_max,
                    sick_leaves_min, sick_leaves_max,
                    unpaid_leaves_min, unpaid_leaves_max,
                    job_rate_min, job_rate_max
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING run_id;
                """,
                (
                    run_ts,
                    int(summary["rows"]), int(summary["cols"]),
                    summary["duplicate_key"], int(summary["duplicate_rows"]),
                    summary["start_date_min"], summary["start_date_max"],
                    int(summary["monthly_salary_min"]), int(summary["monthly_salary_max"]),
                    int(summary["annual_salary_min"]), int(summary["annual_salary_max"]),
                    int(summary["overtime_hours_min"]), int(summary["overtime_hours_max"]),
                    int(summary["sick_leaves_min"]), int(summary["sick_leaves_max"]),
                    int(summary["unpaid_leaves_min"]), int(summary["unpaid_leaves_max"]),
                    float(summary["job_rate_min"]), float(summary["job_rate_max"]),
                )
            )
            run_id = cur.fetchone()[0]

            for _, r in rules.iterrows():
                cur.execute(
                    """
                    INSERT INTO dq_rule_results (run_id, rule_name, violations)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (run_id, rule_name) DO UPDATE
                    SET violations = EXCLUDED.violations;
                    """,
                    (run_id, r["rule_name"], int(r["violations"]))
                )

        conn.commit()
        print(f"✅ DQ results saved to Postgres. run_id={run_id}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
