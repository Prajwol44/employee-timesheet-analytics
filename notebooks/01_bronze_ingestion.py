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

# DBTITLE 1,clean malformed data
# Clean malformed data before writing to Bronze
# Some source rows have timestamps in hours_worked instead of decimal values
# Use try_cast to convert valid values and set invalid values to NULL

print("=== Cleaning Malformed Data ===")

# Count rows with non-numeric hours_worked
malformed_hours = (
    timesheet_bronze
    .filter(
        F.col("hours_worked").isNotNull() &
        F.expr("try_cast(hours_worked AS DECIMAL(10,2))").isNull()
    )
    .count()
)

print(f"Rows with malformed hours_worked: {malformed_hours}")

if malformed_hours > 0:
    print("Sample malformed values:")
    display(
        timesheet_bronze
        .filter(
            F.col("hours_worked").isNotNull() &
            F.expr("try_cast(hours_worked AS DECIMAL(10,2))").isNull()
        )
        .select("client_employee_id", "hours_worked", "punch_in_datetime", "punch_out_datetime")
        .limit(10)
    )

# Apply try_cast to hours_worked to handle malformed values gracefully
timesheet_bronze = (
    timesheet_bronze
    .withColumn(
        "hours_worked",
        F.expr("try_cast(hours_worked AS DECIMAL(10,2))")
    )
    .withColumn(
        "hours_worked",
        F.col("hours_worked").cast("string")  # Convert back to string for Bronze layer
    )
)

print(f"Malformed hours_worked values converted to NULL")
print("Data cleaning complete")

# COMMAND ----------

# DBTITLE 1,filter invalid rows
# Filter out malformed rows where pay codes/comments ended up in client_employee_id column
# Valid employee IDs are numeric (e.g., "101005", "00025") or follow specific patterns

print("=== Filtering Invalid Rows ===")

initial_count = timesheet_bronze.count()

# Identify rows with invalid employee IDs (containing spaces, common keywords, etc.)
invalid_employee_ids = (
    timesheet_bronze
    .filter(
        # Employee ID contains spaces (not a valid ID format)
        F.col("client_employee_id").contains(" ") |
        # Employee ID is NULL or empty
        F.col("client_employee_id").isNull() |
        (F.trim(F.col("client_employee_id")) == "") |
        # Employee ID contains common pay code/comment keywords
        F.lower(F.col("client_employee_id")).contains("meal") |
        F.lower(F.col("client_employee_id")).contains("missed") |
        F.lower(F.col("client_employee_id")).contains("tardy") |
        F.lower(F.col("client_employee_id")).contains("late") |
        F.lower(F.col("client_employee_id")).contains("note") |
        F.lower(F.col("client_employee_id")).contains("voluntary") |
        F.lower(F.col("client_employee_id")).contains("punch") |
        F.lower(F.col("client_employee_id")).contains("traffic") |
        F.lower(F.col("client_employee_id")).contains("mbta") |
        F.lower(F.col("client_employee_id")).contains("adj for")
    )
)

invalid_count = invalid_employee_ids.count()

if invalid_count > 0:
    print(f"Found {invalid_count} rows with invalid employee IDs")
    print("\nSample invalid rows:")
    display(
        invalid_employee_ids
        .select(
            "client_employee_id",
            "pay_code",
            "hours_worked",
            "punch_in_datetime",
            "punch_out_datetime"
        )
        .limit(20)
    )

# Filter out invalid rows
timesheet_bronze = (
    timesheet_bronze
    .filter(
        # Keep only rows with valid employee IDs
        ~(
            F.col("client_employee_id").contains(" ") |
            F.col("client_employee_id").isNull() |
            (F.trim(F.col("client_employee_id")) == "") |
            F.lower(F.col("client_employee_id")).contains("meal") |
            F.lower(F.col("client_employee_id")).contains("missed") |
            F.lower(F.col("client_employee_id")).contains("tardy") |
            F.lower(F.col("client_employee_id")).contains("late") |
            F.lower(F.col("client_employee_id")).contains("note") |
            F.lower(F.col("client_employee_id")).contains("voluntary") |
            F.lower(F.col("client_employee_id")).contains("punch") |
            F.lower(F.col("client_employee_id")).contains("traffic") |
            F.lower(F.col("client_employee_id")).contains("mbta") |
            F.lower(F.col("client_employee_id")).contains("adj for")
        )
    )
)

final_count = timesheet_bronze.count()
filtered_out = initial_count - final_count

print(f"\nRows before filtering: {initial_count}")
print(f"Rows after filtering: {final_count}")
print(f"Rows filtered out: {filtered_out}")
print("Invalid row filtering complete")

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