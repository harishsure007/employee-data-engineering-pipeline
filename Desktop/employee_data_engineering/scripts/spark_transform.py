from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat_ws, upper, initcap, to_date

RAW = "data/processed/employees_clean.csv"
OUT = "data/processed/employees_spark_parquet"

spark = (
    SparkSession.builder
    .appName("employees-spark-transform")
    .master("local[*]")
    .getOrCreate()
)

# ---- Read processed CSV directly with Spark ----
df = spark.read.option("header", True).option("inferSchema", True).csv(RAW)

# ---- Step-2 checks (profiling) ----
print("✅ Spark schema:")
df.printSchema()
print("✅ Row count:", df.count())

# ---- Transform (same rules) ----
df = (
    df
    .withColumn("First Name", initcap(col("First Name")))
    .withColumn("Last Name", initcap(col("Last Name")))
    .withColumn("Department", upper(col("Department")))
    .withColumn("Country", upper(col("Country")))
    .withColumn("Center", upper(col("Center")))
    .withColumn("Gender", initcap(col("Gender")))
    .withColumn("Start Date", to_date(col("Start Date")))
    .withColumn("full_name", concat_ws(" ", col("First Name"), col("Last Name")))
)

# Salary must be > 0
df = df.filter(col("Monthly Salary") > 0)

# Fix Annual Salary = Monthly*12 if mismatch
df = df.withColumn(
    "Annual Salary",
    when(col("Annual Salary") != col("Monthly Salary") * 12,
         col("Monthly Salary") * 12).otherwise(col("Annual Salary"))
)

# Leaves/overtime non-negative
for c in ["Sick Leaves", "Unpaid Leaves", "Overtime Hours"]:
    df = df.withColumn(c, when(col(c) < 0, 0).otherwise(col(c)))

# Job Rate between 0 and 5, fill null with 0
df = df.withColumn(
    "Job Rate",
    when(col("Job Rate").isNull(), 0)
    .when(col("Job Rate") < 0, 0)
    .when(col("Job Rate") > 5, 5)
    .otherwise(col("Job Rate"))
)

# tenure_bucket
df = df.withColumn(
    "tenure_bucket",
    when(col("Years") == 0, "0")
    .when(col("Years").between(1, 2), "1-2")
    .when(col("Years").between(3, 5), "3-5")
    .when(col("Years").between(6, 10), "6-10")
    .otherwise("10+")
)

# salary_band
df = df.withColumn(
    "salary_band",
    when(col("Monthly Salary") < 1500, "LOW")
    .when(col("Monthly Salary") < 3000, "MID")
    .when(col("Monthly Salary") < 4500, "HIGH")
    .otherwise("VERY_HIGH")
)

# ---- Write Parquet ----
(
    df
    .repartition(4)
    .write
    .mode("overwrite")
    .parquet(OUT)
)

print("✅ Spark transform complete. Parquet written to:", OUT)
spark.stop()
