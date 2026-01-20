import pandas as pd
import numpy as np

RAW_PATH = "data/raw/Employees 2.xlsx"
OUT_CSV = "data/processed/employees_clean.csv"
OUT_PARQUET = "data/processed/employees_clean.parquet"

df = pd.read_excel(RAW_PATH)

# --- 1) Standardize text columns ---
for col in ["First Name", "Last Name", "Department", "Country", "Center", "Gender"]:
    df[col] = df[col].astype(str).str.strip()

df["First Name"] = df["First Name"].str.title()
df["Last Name"] = df["Last Name"].str.title()
df["Department"] = df["Department"].str.upper()
df["Country"] = df["Country"].str.upper()
df["Center"] = df["Center"].str.upper()
df["Gender"] = df["Gender"].str.title()

# --- 2) Create full name ---
df["full_name"] = df["First Name"] + " " + df["Last Name"]

# --- 3) Data quality checks / fixes ---
# Salary must be > 0
df = df[df["Monthly Salary"] > 0].copy()

# Annual salary should equal Monthly * 12 (fix mismatches)
expected_annual = df["Monthly Salary"] * 12
mismatch = df["Annual Salary"] != expected_annual
df.loc[mismatch, "Annual Salary"] = expected_annual[mismatch]

# No negative values for leaves / overtime (clip at 0)
for col in ["Sick Leaves", "Unpaid Leaves", "Overtime Hours"]:
    df[col] = df[col].clip(lower=0)

# Job Rate: keep in 0–5 range (clip) and fill missing
df["Job Rate"] = df["Job Rate"].fillna(0).clip(lower=0, upper=5)

# --- 4) Tenure bucket (from Years column) ---
df["tenure_bucket"] = pd.cut(
    df["Years"],
    bins=[-1, 0, 2, 5, 10, 100],
    labels=["0", "1-2", "3-5", "6-10", "10+"]
)

# --- 5) Salary band (from Monthly Salary) ---
df["salary_band"] = pd.cut(
    df["Monthly Salary"],
    bins=[0, 1500, 3000, 4500, 10**9],
    labels=["LOW", "MID", "HIGH", "VERY_HIGH"]
)

# --- 6) Save outputs ---
df.to_csv(OUT_CSV, index=False)

# Parquet is best for DE pipelines, but needs pyarrow
try:
    df.to_parquet(OUT_PARQUET, index=False)
    parquet_saved = True
except Exception as e:
    parquet_saved = False
    print("⚠️ Could not save Parquet (install pyarrow later). Reason:", e)

print("✅ Transform complete")
print("Rows, Cols after cleaning:", df.shape)
print("Saved:", OUT_CSV)
if parquet_saved:
    print("Saved:", OUT_PARQUET)
    