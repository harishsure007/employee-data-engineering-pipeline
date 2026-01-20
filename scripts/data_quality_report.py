import pandas as pd
from pathlib import Path

INPUT_CSV = "data/processed/employees_clean.csv"
OUT_DIR = Path("data/processed")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_SUMMARY = OUT_DIR / "dq_summary.csv"
OUT_NULLS = OUT_DIR / "dq_nulls.csv"
OUT_RULES = OUT_DIR / "dq_rules.csv"
OUT_SAMPLE_BAD = OUT_DIR / "dq_bad_rows_sample.csv"

def main():
    df = pd.read_csv(INPUT_CSV)

    # --- Basic profiling ---
    total_rows = len(df)
    total_cols = df.shape[1]

    # Nulls
    null_counts = df.isna().sum().sort_values(ascending=False)
    null_pct = (null_counts / total_rows * 100).round(2)
    nulls_report = pd.DataFrame({"null_count": null_counts, "null_pct": null_pct})
    nulls_report.to_csv(OUT_NULLS, index=True)

    # Duplicates (based on emp_no if present, else full row)
    if "No" in df.columns:
        dup_key = "No"
        duplicate_rows = df.duplicated(subset=[dup_key]).sum()
    else:
        dup_key = "FULL_ROW"
        duplicate_rows = df.duplicated().sum()

    # Try parse dates if present
    start_date_min = start_date_max = None
    if "Start Date" in df.columns:
        start_dates = pd.to_datetime(df["Start Date"], errors="coerce")
        start_date_min = start_dates.min()
        start_date_max = start_dates.max()

    # Numeric ranges (only if columns exist)
    def minmax(col):
        if col in df.columns:
            return (df[col].min(), df[col].max())
        return (None, None)

    monthly_min, monthly_max = minmax("Monthly Salary")
    annual_min, annual_max = minmax("Annual Salary")
    overtime_min, overtime_max = minmax("Overtime Hours")
    sick_min, sick_max = minmax("Sick Leaves")
    unpaid_min, unpaid_max = minmax("Unpaid Leaves")
    jobrate_min, jobrate_max = minmax("Job Rate")

    summary = pd.DataFrame([{
        "rows": total_rows,
        "cols": total_cols,
        "duplicate_key": dup_key,
        "duplicate_rows": int(duplicate_rows),
        "start_date_min": str(start_date_min) if start_date_min is not None else "",
        "start_date_max": str(start_date_max) if start_date_max is not None else "",
        "monthly_salary_min": monthly_min,
        "monthly_salary_max": monthly_max,
        "annual_salary_min": annual_min,
        "annual_salary_max": annual_max,
        "overtime_hours_min": overtime_min,
        "overtime_hours_max": overtime_max,
        "sick_leaves_min": sick_min,
        "sick_leaves_max": sick_max,
        "unpaid_leaves_min": unpaid_min,
        "unpaid_leaves_max": unpaid_max,
        "job_rate_min": jobrate_min,
        "job_rate_max": jobrate_max,
    }])
    summary.to_csv(OUT_SUMMARY, index=False)

    # --- Rule checks (practical DE checks) ---
    rules = []

    # Rule: Monthly salary > 0
    if "Monthly Salary" in df.columns:
        bad = (df["Monthly Salary"] <= 0).sum()
        rules.append(("monthly_salary_positive", int(bad)))

    # Rule: Annual salary == Monthly*12
    if "Monthly Salary" in df.columns and "Annual Salary" in df.columns:
        bad = (df["Annual Salary"] != df["Monthly Salary"] * 12).sum()
        rules.append(("annual_salary_matches_monthly_x12", int(bad)))

    # Rule: Leaves and overtime not negative
    for col in ["Sick Leaves", "Unpaid Leaves", "Overtime Hours"]:
        if col in df.columns:
            bad = (df[col] < 0).sum()
            rules.append((f"{col.lower().replace(' ', '_')}_non_negative", int(bad)))

    # Rule: Job Rate between 0 and 5
    if "Job Rate" in df.columns:
        bad = ((df["Job Rate"] < 0) | (df["Job Rate"] > 5)).sum()
        rules.append(("job_rate_0_to_5", int(bad)))

    rules_df = pd.DataFrame(rules, columns=["rule_name", "violations"])
    rules_df.to_csv(OUT_RULES, index=False)

    # Save a sample of “bad rows” for quick review (if any rule violations exist)
    bad_mask = pd.Series(False, index=df.index)

    if "Monthly Salary" in df.columns:
        bad_mask |= (df["Monthly Salary"] <= 0)
    if "Monthly Salary" in df.columns and "Annual Salary" in df.columns:
        bad_mask |= (df["Annual Salary"] != df["Monthly Salary"] * 12)
    for col in ["Sick Leaves", "Unpaid Leaves", "Overtime Hours"]:
        if col in df.columns:
            bad_mask |= (df[col] < 0)
    if "Job Rate" in df.columns:
        bad_mask |= ((df["Job Rate"] < 0) | (df["Job Rate"] > 5))

    bad_rows = df[bad_mask].head(50)
    bad_rows.to_csv(OUT_SAMPLE_BAD, index=False)

    # --- Console output (quick) ---
    print("✅ Data Quality Report Generated")
    print("Saved:", OUT_SUMMARY)
    print("Saved:", OUT_NULLS)
    print("Saved:", OUT_RULES)
    print("Saved:", OUT_SAMPLE_BAD)
    print("\n--- Quick DQ Summary ---")
    print(summary.to_string(index=False))
    print("\n--- Rule Violations ---")
    print(rules_df.to_string(index=False))

if __name__ == "__main__":
    main()
