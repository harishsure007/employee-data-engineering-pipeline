import pandas as pd

PATH = "data/raw/Employees 2.xlsx"

df = pd.read_excel(PATH)

print("✅ Loaded file successfully")
print("Rows, Cols:", df.shape)
print("\nColumns:", list(df.columns))

print("\n--- Preview (first 5 rows) ---")
print(df.head())

print("\n--- Data types ---")
print(df.dtypes)
