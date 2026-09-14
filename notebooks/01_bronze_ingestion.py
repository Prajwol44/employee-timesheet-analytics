# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Bronze Ingestion
# MAGIC
# MAGIC This notebook ingests the raw employee and timesheet CSV files from the
# MAGIC Databricks Unity Catalog Volume into Bronze Delta tables.
# MAGIC
# MAGIC ## Objectives
# MAGIC
# MAGIC - Read employee data from the raw Volume
# MAGIC - Read all timesheet extracts from the raw Volume
# MAGIC - Handle the pipe (`|`) delimiter used by the source files
# MAGIC - Correctly handle quoted fields containing `|`
# MAGIC - Interpret `[NULL]` as a null value
# MAGIC - Add basic ingestion lineage metadata
# MAGIC - Write the data to Bronze Delta tables
# MAGIC - Perform basic ingestion validation

# COMMAND ----------

# DBTITLE 1,Define Paths
# Source paths
RAW_VOLUME = "/Volumes/workforce/raw/raw_data"

EMPLOYEE_PATH = f"{RAW_VOLUME}/employee_202510161125.csv"
TIMESHEET_PATH = f"{RAW_VOLUME}/timesheet_*.csv"

# Target tables
BRONZE_SCHEMA = "workforce.bronze"

EMPLOYEE_TABLE = f"{BRONZE_SCHEMA}.employee"
TIMESHEET_TABLE = f"{BRONZE_SCHEMA}.timesheet"

print("Raw volume:", RAW_VOLUME)
print("Employee source:", EMPLOYEE_PATH)
print("Timesheet source:", TIMESHEET_PATH)
print("Bronze schema:", BRONZE_SCHEMA)

# COMMAND ----------

# DBTITLE 1,Verify src files
display(dbutils.fs.ls(RAW_VOLUME))

# COMMAND ----------

# DBTITLE 1,pre defining csv options
CSV_OPTIONS = {
    "header": "true",
    "sep": "|",
    "quote": '"',
    "escape": '"',
    "nullValue": "[NULL]",
    "mode": "PERMISSIVE",
    "multiLine": "false",
    "inferSchema": "false"
}

# COMMAND ----------

# DBTITLE 1,read emp data
employee_raw = (
    spark.read
    .format("csv")
    .options(**CSV_OPTIONS)
    .load(EMPLOYEE_PATH)
)

print("Employee columns:", len(employee_raw.columns))
print("Employee rows:", employee_raw.count())

employee_raw.printSchema()

# COMMAND ----------

display(employee_raw.limit(10))

# COMMAND ----------

# DBTITLE 1,read all timesheet files
timesheet_raw = (
    spark.read
    .format("csv")
    .options(**CSV_OPTIONS)
    .load(TIMESHEET_PATH)
)

print("Timesheet columns:", len(timesheet_raw.columns))
print("Timesheet rows:", timesheet_raw.count())

timesheet_raw.printSchema()


# COMMAND ----------

# DBTITLE 1,display timesheet
display(timesheet_raw.limit(10))

# COMMAND ----------

# DBTITLE 1,adding medatata cols
# Add ingestion lineage metadata

from pyspark.sql import functions as F

batch_id = (
    spark.sql("SELECT current_timestamp() AS ts")
    .first()["ts"]
    .strftime("%Y%m%d_%H%M%S")
)

employee_bronze = (
    employee_raw
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_batch_id", F.lit(batch_id))
)

print("Employee Bronze rows:", employee_bronze.count())

display(employee_bronze.limit(10))

# COMMAND ----------

# Add ingestion lineage metadata to timesheet data

timesheet_bronze = (
    timesheet_raw
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_batch_id", F.lit(batch_id))
)

print("Timesheet Bronze rows:", timesheet_bronze.count())

display(timesheet_bronze.limit(10))

# COMMAND ----------

display(
    timesheet_bronze
    .groupBy("_source_file")
    .count()
    .orderBy("_source_file")
)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workforce.bronze")

# COMMAND ----------

# DBTITLE 1,save to schema
(
    employee_bronze
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(EMPLOYEE_TABLE)
)

# COMMAND ----------

# DBTITLE 1,save timesheet to schema
(
    timesheet_bronze
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TIMESHEET_TABLE)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

# DBTITLE 1,verify tables
spark.sql("SHOW TABLES IN workforce.bronze").show()

# COMMAND ----------

print("Employee Bronze count:")
spark.sql(f"SELECT COUNT(*) FROM {EMPLOYEE_TABLE}").show()

print("Timesheet Bronze count:")
spark.sql(f"SELECT COUNT(*) FROM {TIMESHEET_TABLE}").show()

# COMMAND ----------

# DBTITLE 1,verify lineage
display(
    spark.sql(f"""
        SELECT
            _source_file,
            COUNT(*) AS row_count,
            MIN(_ingested_at) AS first_ingested_at,
            MAX(_ingested_at) AS last_ingested_at
        FROM {TIMESHEET_TABLE}
        GROUP BY _source_file
        ORDER BY _source_file
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC created a basic bronze validation so dq checks are more robust

# COMMAND ----------

employee_count = spark.table(EMPLOYEE_TABLE).count()
timesheet_count = spark.table(TIMESHEET_TABLE).count()

print("=== Bronze Ingestion Validation ===")
print(f"Employee rows   : {employee_count}")
print(f"Timesheet rows  : {timesheet_count}")

assert employee_count > 0, "Employee Bronze table is empty"
assert timesheet_count > 0, "Timesheet Bronze table is empty"

print("Bronze ingestion validation PASSED")

# COMMAND ----------

# DBTITLE 1,check important columns
print("Employee columns:")
print(employee_bronze.columns)

print("\nTimesheet columns:")
print(timesheet_bronze.columns)

# COMMAND ----------

# DBTITLE 1,check pay_code parsing
display(
    timesheet_bronze
    .select("pay_code")
    .where(F.col("pay_code").contains("|"))
    .limit(20)
)

# COMMAND ----------

# DBTITLE 1,summary
print("BRONZE INGESTION COMPLETED \n")
print(f"Employee rows : {employee_count}")
print(f"Timesheet rows : {timesheet_count}")
print(f"Batch ID : {batch_id}")
print()
print("Tables created:")
print(f" {EMPLOYEE_TABLE}")
print(f" {TIMESHEET_TABLE}")