from pyspark.sql import SparkSession

PARQUET_PATH = "data/processed/employees_spark_parquet"

spark = (
    SparkSession.builder
    .appName("employees-spark-sql-analytics")
    .master("local[*]")
    .getOrCreate()
)

df = spark.read.parquet(PARQUET_PATH)

print("✅ Parquet loaded")
print("✅ Row count:", df.count())

# Register as a SQL table/view
df.createOrReplaceTempView("employees")

# 1) Avg monthly salary by department (Top 5)
q1 = """
SELECT Department, ROUND(AVG(`Monthly Salary`), 2) AS avg_monthly_salary
FROM employees
GROUP BY Department
ORDER BY avg_monthly_salary DESC
LIMIT 5
"""
print("\n--- Top 5 departments by avg salary ---")
spark.sql(q1).show(truncate=False)

# 2) Total overtime by country
q2 = """
SELECT Country, SUM(`Overtime Hours`) AS total_overtime
FROM employees
GROUP BY Country
ORDER BY total_overtime DESC
"""
print("\n--- Overtime by country ---")
spark.sql(q2).show(truncate=False)

# 3) Salary band distribution
q3 = """
SELECT salary_band, COUNT(*) AS cnt
FROM employees
GROUP BY salary_band
ORDER BY cnt DESC
"""
print("\n--- Salary band distribution ---")
spark.sql(q3).show(truncate=False)

# 4) Employees with highest overtime (Top 10)
q4 = """
SELECT full_name, Department, `Overtime Hours` AS overtime_hours
FROM employees
ORDER BY overtime_hours DESC
LIMIT 10
"""
print("\n--- Top 10 overtime employees ---")
spark.sql(q4).show(truncate=False)

spark.stop()
