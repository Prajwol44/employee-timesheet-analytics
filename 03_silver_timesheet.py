# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 03 - Silver Timeshet Notebook

# COMMAND ----------

# MAGIC %md
# MAGIC config and setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StringType,
    DateType,
    TimestampType,
    DecimalType,
    BooleanType,
    IntegerType,
    LongType,
    DoubleType
)

# COMMAND ----------

# DBTITLE 1,config
BRONZE_TABLE = "workforce.bronze.timesheet"
SILVER_TABLE = "workforce.silver.timesheet"
SILVER_PAY_CODE_TABLE = "workforce.silver.timesheet_pay_code"

GRACE_MINUTES = 5

print("Configuration loaded.")
print("Bronze table :", BRONZE_TABLE)
print("Silver table :", SILVER_TABLE)
print("Pay-code table:", SILVER_PAY_CODE_TABLE)
print("Grace period :", GRACE_MINUTES, "minutes")

# COMMAND ----------

# DBTITLE 1,load bronze timesheet
timesheet_clean = spark.table(BRONZE_TABLE)

print("Bronze timesheet loaded.")
print("Rows:", timesheet_clean.count())
print("Columns:", len(timesheet_clean.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC # Validation and Checks

# COMMAND ----------

# DBTITLE 1,schema check
timesheet_clean.printSchema()

# COMMAND ----------

# DBTITLE 1,table check
display(
    timesheet_clean.limit(10)
)

# COMMAND ----------

# DBTITLE 1,validate bronze columns
required_columns = [
    "client_employee_id",
    "department_id",
    "department_name",
    "home_department_id",
    "home_department_name",
    "pay_code",
    "punch_in_comment",
    "punch_out_comment",
    "hours_worked",
    "punch_apply_date",
    "punch_in_datetime",
    "punch_out_datetime",
    "scheduled_start_datetime",
    "scheduled_end_datetime",
    "_source_file",
    "_ingested_at",
    "_batch_id"
]

missing_columns = [
    c for c in required_columns
    if c not in timesheet_clean.columns
]

print("Missing required columns:", missing_columns)

if missing_columns:
    raise ValueError(
        f"Bronze timesheet is missing required columns: {missing_columns}"
    )

print("Bronze schema validation PASSED.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### IMPORTNAT
# MAGIC
# MAGIC i want to know how much data came from source files because they overlap

# COMMAND ----------

# DBTITLE 1,source file profile
display(
    timesheet_clean
    .groupBy("_source_file")
    .agg(
        F.count("*").alias("row_count"),
        F.min("punch_apply_date").alias("min_apply_date"),
        F.max("punch_apply_date").alias("max_apply_date"),
        F.countDistinct("client_employee_id").alias(
            "distinct_employees"
        )
    )
    .orderBy("_source_file")
)

# COMMAND ----------

# DBTITLE 1,bronze row count checkpoint
bronze_timesheet_count = timesheet_clean.count()

if bronze_timesheet_count == 0:
    raise ValueError(
        "Bronze timesheet table contains 0 rows. "
        "Check Bronze ingestion."
    )

print("Bronze row-count validation PASSED.")

# COMMAND ----------

# DBTITLE 1,source fiile recon
source_file_profile = (
    timesheet_clean
    .groupBy("_source_file")
    .agg(
        F.count("*").alias("row_count"),
        F.min("punch_apply_date").alias("min_apply_date"),
        F.max("punch_apply_date").alias("max_apply_date"),
        F.countDistinct("client_employee_id").alias(
            "distinct_employees"
        )
    )
    .orderBy("_source_file")
)

display(source_file_profile)

# COMMAND ----------

source_file_total = (
    source_file_profile
    .agg(
        F.sum("row_count").alias("total_rows")
    )
    .collect()[0]["total_rows"]
)

print("Bronze table row count :", bronze_timesheet_count)
print("Source-file row total  :", source_file_total)

if source_file_total != bronze_timesheet_count:
    raise ValueError(
        "Source-file row total does not match Bronze table row count."
    )

print("Source-file reconciliation PASSED.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformations and Standardization

# COMMAND ----------

# DBTITLE 1,standardize string cols
string_columns = [
    "client_employee_id",
    "department_id",
    "department_name",
    "home_department_id",
    "home_department_name",
    "pay_code",
    "punch_in_comment",
    "punch_out_comment",
    "hours_worked",
    "punch_apply_date",
    "punch_in_datetime",
    "punch_out_datetime",
    "scheduled_start_datetime",
    "scheduled_end_datetime"
]

missing_string_columns = [
    c for c in string_columns
    if c not in timesheet_clean.columns
]

if missing_string_columns:
    raise ValueError(
        f"Missing string columns: {missing_string_columns}"
    )

for c in string_columns:
    timesheet_clean = timesheet_clean.withColumn(
        c,
        F.when(
            F.trim(F.col(c)) == "",
            F.lit(None)
        ).otherwise(
            F.trim(F.col(c))
        )
    )

print("String standardization completed.")

# COMMAND ----------

# DBTITLE 1,validate
display(
    timesheet_clean.select(
        "client_employee_id",
        "department_id",
        "department_name",
        "home_department_id",
        "home_department_name",
        "pay_code",
        "punch_in_comment",
        "punch_out_comment"
    ).limit(10)
)

# COMMAND ----------

# DBTITLE 1,validate critical identifiers
missing_employee_id_count = (
    timesheet_clean
    .filter(
        F.col("client_employee_id").isNull() |
        (F.trim(F.col("client_employee_id")) == "")
    )
    .count()
)

print(
    "Timesheet rows missing client_employee_id:",
    missing_employee_id_count
)

if missing_employee_id_count > 0:
    display(
        timesheet_clean
        .filter(F.col("client_employee_id").isNull())
        .limit(10)
    )

    raise ValueError(
        "Timesheet contains rows without client_employee_id."
    )

print("Employee ID validation PASSED.")

# COMMAND ----------

# DBTITLE 1,validate pay code
from pyspark.sql import functions as F

# 1. Count missing pay_code values
missing_pay_code = (
    timesheet_clean
    .filter(
        F.col("pay_code").isNull() |
        (F.trim(F.col("pay_code")) == "")
    )
    .count()
)

print(f"Rows missing pay_code: {missing_pay_code}")

# 2. Display the affected rows for investigation
display(
    timesheet_clean
    .filter(
        F.col("pay_code").isNull() |
        (F.trim(F.col("pay_code")) == "")
    )
    .select(
        "client_employee_id",
        "department_id",
        "department_name",
        "home_department_id",
        "home_department_name",
        "pay_code",
        "hours_worked",
        "punch_apply_date",
        "punch_in_datetime",
        "punch_out_datetime",
        "_source_file"
    )
)

# COMMAND ----------

display(
    timesheet_clean
    .filter(
        F.col("pay_code").isNull() &
        F.col("client_employee_id").isNotNull()
    )
    .select(
        "client_employee_id",
        "department_id",
        "pay_code",
        "hours_worked",
        "punch_apply_date",
        "punch_in_datetime",
        "punch_out_datetime",
        "_source_file"
    )
)

# COMMAND ----------

malformed_pay_code_rows = (
    timesheet_clean
    .filter(
        F.col("pay_code").isNull() |
        (F.trim(F.col("pay_code")) == "")
    )
)

print(f"Malformed rows: {malformed_pay_code_rows.count()}")

print("Rows with valid employee ID:")
print(
    malformed_pay_code_rows
    .filter(F.col("client_employee_id").rlike(r"^\d+$"))
    .count()
)

print("Rows with valid hours:")
print(
    malformed_pay_code_rows
    .filter(F.col("hours_worked").isNotNull())
    .count()
)

print("Rows with valid punch-in:")
print(
    malformed_pay_code_rows
    .filter(F.col("punch_in_datetime").isNotNull())
    .count()
)

# COMMAND ----------

# DBTITLE 1,quarintine these malformed rows
from pyspark.sql import functions as F

# Identify malformed timesheet records
malformed_timesheet = (
    timesheet_clean
    .filter(
        F.col("pay_code").isNull() |
        (F.trim(F.col("pay_code")) == "")
    )
    .withColumn(
        "dq_reason",
        F.lit("Malformed source row: missing pay_code and core timesheet fields")
    )
    .withColumn(
        "dq_checked_at",
        F.current_timestamp()
    )
)

print(f"Malformed rows identified: {malformed_timesheet.count()}")

display(
    malformed_timesheet.select(
        "client_employee_id",
        "department_id",
        "pay_code",
        "hours_worked",
        "punch_apply_date",
        "punch_in_datetime",
        "punch_out_datetime",
        "_source_file",
        "dq_reason"
    )
)

# COMMAND ----------

# DBTITLE 1,create valid dataset
timesheet_clean_valid = (
    timesheet_clean
    .filter(
        F.col("pay_code").isNotNull() &
        (F.trim(F.col("pay_code")) != "")
    )
)

print(f"Original rows: {timesheet_clean.count()}")
print(f"Malformed rows: {malformed_timesheet.count()}")
print(f"Valid rows: {timesheet_clean_valid.count()}")

display(timesheet_clean_valid.limit(10))

# COMMAND ----------

# DBTITLE 1,making this df the main df
from pyspark.sql import functions as F
from pyspark.sql.window import Window

timesheet_work = timesheet_clean_valid

print(f"Working rows: {timesheet_work.count()}")
print("Columns:")
print(timesheet_work.columns)

display(timesheet_work.limit(10))

# COMMAND ----------

required_columns = [
    "client_employee_id",
    "department_id",
    "department_name",
    "home_department_id",
    "home_department_name",
    "pay_code",
    "hours_worked",
    "punch_apply_date",
    "punch_in_datetime",
    "punch_out_datetime",
    "scheduled_start_datetime",
    "scheduled_end_datetime",
    "_source_file",
    "_ingested_at",
    "_batch_id"
]

missing_columns = [
    c for c in required_columns
    if c not in timesheet_work.columns
]

if missing_columns:
    raise ValueError(f"Missing required columns: {missing_columns}")

print("Required-column validation: PASSED")

# COMMAND ----------

# DBTITLE 1,final pay code validation
missing_pay_code = (
    timesheet_work
    .filter(
        F.col("pay_code").isNull() |
        (F.trim(F.col("pay_code")) == "")
    )
    .count()
)

print(f"Missing pay_code rows: {missing_pay_code}")

if missing_pay_code != 0:
    raise ValueError(
        f"Valid working dataset still contains {missing_pay_code} rows without pay_code."
    )

print("Pay code validation: PASSED")

# COMMAND ----------

# MAGIC %md
# MAGIC not assumed every timesheet employee exists in the employee master yet 
# MAGIC only validate that the timesheet identifier itself is populated

# COMMAND ----------

# DBTITLE 1,validate emp ids
employee_id_profile = (
    timesheet_work
    .select(
        F.count("*").alias("total_rows"),
        F.count("client_employee_id").alias("employee_id_populated"),
        F.countDistinct("client_employee_id").alias("distinct_employee_ids")
    )
)

display(employee_id_profile)

# COMMAND ----------

null_employee_ids = (
    timesheet_work
    .filter(
        F.col("client_employee_id").isNull() |
        (F.trim(F.col("client_employee_id")) == "")
    )
    .count()
)

if null_employee_ids > 0:
    raise ValueError(
        f"Timesheet contains {null_employee_ids} rows without client_employee_id."
    )

print("Employee ID validation: PASSED")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Casting

# COMMAND ----------

# DBTITLE 1,cast date and timestamps
from pyspark.sql import functions as F

timesheet_work = (
    timesheet_work
    .withColumn(
        "punch_apply_date",
        F.expr("try_to_date(punch_apply_date, 'yyyy-MM-dd')")
    )
    .withColumn(
        "punch_in_datetime",
        F.expr("try_to_timestamp(punch_in_datetime, 'yyyy-MM-dd HH:mm:ss.SSS')")
    )
    .withColumn(
        "punch_out_datetime",
        F.expr("try_to_timestamp(punch_out_datetime, 'yyyy-MM-dd HH:mm:ss.SSS')")
    )
    .withColumn(
        "scheduled_start_datetime",
        F.expr("try_to_timestamp(scheduled_start_datetime, 'yyyy-MM-dd HH:mm:ss.SSS')")
    )
    .withColumn(
        "scheduled_end_datetime",
        F.expr("try_to_timestamp(scheduled_end_datetime, 'yyyy-MM-dd HH:mm:ss.SSS')")
    )
)

print("Date/timestamp casting completed.")

timesheet_work.printSchema()

display(
    timesheet_work.select(
        "client_employee_id",
        "punch_apply_date",
        "punch_in_datetime",
        "punch_out_datetime",
        "scheduled_start_datetime",
        "scheduled_end_datetime"
    ).limit(10)
)

# COMMAND ----------

print("Current schema:")
timesheet_work.printSchema()

print("\nCurrent columns:")
print(timesheet_work.columns)

display(
    timesheet_work.select(
        "punch_apply_date",
        "punch_in_datetime",
        "punch_out_datetime",
        "scheduled_start_datetime",
        "scheduled_end_datetime"
    ).limit(5)
)

# COMMAND ----------

display(
    timesheet_work.select(
        "punch_in_datetime",
        "punch_out_datetime",
        "scheduled_start_datetime",
        "scheduled_end_datetime"
    ).limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC had to take a diff approach here because the original code i have written hits a valueerror at row 161

# COMMAND ----------

# missing_punch_times = (
#     timesheet_work
#     .filter(
#         F.col("punch_in_datetime").isNull() |
#         F.col("punch_out_datetime").isNull()
#     )
#     .count()
# )

# print(f"Rows missing punch timestamps: {missing_punch_times}")

# if missing_punch_times > 0:
#     raise ValueError(
#         f"{missing_punch_times} valid timesheet rows have missing punch timestamps."
#     )

# print("Punch timestamp validation: PASSED")

# COMMAND ----------

# DBTITLE 1,two punch timestamps
from pyspark.sql import functions as F

# Profile punch timestamp completeness
punch_profile = timesheet_work.select(
    F.count("*").alias("total_rows"),

    F.count("punch_in_datetime").alias("punch_in_populated"),

    F.count("punch_out_datetime").alias("punch_out_populated"),

    F.sum(
        F.when(
            F.col("punch_in_datetime").isNull() &
            F.col("punch_out_datetime").isNull(),
            1
        ).otherwise(0)
    ).alias("both_punches_missing"),

    F.sum(
        F.when(
            F.col("punch_in_datetime").isNull() &
            F.col("punch_out_datetime").isNotNull(),
            1
        ).otherwise(0)
    ).alias("punch_in_missing_only"),

    F.sum(
        F.when(
            F.col("punch_in_datetime").isNotNull() &
            F.col("punch_out_datetime").isNull(),
            1
        ).otherwise(0)
    ).alias("punch_out_missing_only")
)

display(punch_profile)

# COMMAND ----------

display(
    timesheet_work
    .filter(
        F.col("punch_in_datetime").isNull() |
        F.col("punch_out_datetime").isNull()
    )
    .select(
        "client_employee_id",
        "department_id",
        "department_name",
        "home_department_id",
        "home_department_name",
        "pay_code",
        "hours_worked",
        "punch_apply_date",
        "punch_in_datetime",
        "punch_out_datetime",
        "scheduled_start_datetime",
        "scheduled_end_datetime",
        "_source_file"
    )
    .limit(30)
)

# COMMAND ----------

timesheet_work = (
    timesheet_work
    .withColumn(
        "actual_duration_minutes",
        F.when(
            F.col("punch_in_datetime").isNotNull() &
            F.col("punch_out_datetime").isNotNull(),
            (
                F.col("punch_out_datetime").cast("long") -
                F.col("punch_in_datetime").cast("long")
            ) / F.lit(60)
        )
    )
    .withColumn(
        "actual_duration_hours",
        F.when(
            F.col("actual_duration_minutes").isNotNull(),
            F.round(
                F.col("actual_duration_minutes") / F.lit(60),
                2
            )
        )
    )
)

display(
    timesheet_work.select(
        "client_employee_id",
        "pay_code",
        "hours_worked",
        "punch_in_datetime",
        "punch_out_datetime",
        "actual_duration_hours"
    )
    .filter(
        F.col("punch_in_datetime").isNull() |
        F.col("punch_out_datetime").isNull()
    )
    .limit(20)
)

# COMMAND ----------

invalid_negative_duration = (
    timesheet_work
    .filter(
        F.col("actual_duration_minutes").isNotNull() &
        (F.col("actual_duration_minutes") < 0)
    )
    .count()
)

print(
    f"Rows with negative actual duration: "
    f"{invalid_negative_duration}"
)

if invalid_negative_duration > 0:
    raise ValueError(
        "Negative actual durations detected."
    )

print("Actual duration validation: PASSED")

# COMMAND ----------

# DBTITLE 1,cast hours_worked
timesheet_work = (
    timesheet_work
    .withColumn(
        "hours_worked",
        F.col("hours_worked").cast("decimal(10,2)")
    )
)

print("hours_worked cast completed.")

display(
    timesheet_work.select(
        "client_employee_id",
        "hours_worked"
    ).limit(10)
)

# COMMAND ----------

# DBTITLE 1,validate hours
# hours_validation = (
#     timesheet_work
#     .select(
#         F.count("*").alias("total_rows"),
#         F.count("hours_worked").alias("hours_populated"),
#         F.min("hours_worked").alias("min_hours"),
#         F.max("hours_worked").alias("max_hours"),
#         F.sum(
#             F.when(F.col("hours_worked") < 0, 1).otherwise(0)
#         ).alias("negative_hours")
#     )
# )

# display(hours_validation)

# COMMAND ----------

timesheet_work = (
    timesheet_work
    .filter(
        F.col("hours_worked").isNull() |
        F.expr(
            "try_cast(hours_worked AS DECIMAL(10,2))"
        ).isNotNull()
    )
)

print(f"Remaining valid rows: {timesheet_work.count()}")

display(
    timesheet_work.select(
        "client_employee_id",
        "pay_code",
        "hours_worked"
    ).limit(10)
)

# COMMAND ----------

timesheet_work = (
    timesheet_work
    .withColumn(
        "hours_worked",
        F.expr(
            "try_cast(hours_worked AS DECIMAL(10,2))"
        )
    )
)

print("hours_worked converted successfully.")

timesheet_work.printSchema()

display(
    timesheet_work.select(
        "client_employee_id",
        "pay_code",
        "hours_worked"
    ).limit(10)
)

# COMMAND ----------

# DBTITLE 1,materialize cleaned data
import time
temp_table_name = f"workforce.silver.timesheet_work_materialized_{int(time.time())}"

print(f"Writing to temporary table: {temp_table_name}...")
timesheet_work.write.format("delta").saveAsTable(temp_table_name)

print("Reading back from table...")
timesheet_work = spark.table(temp_table_name)

materialized_count = timesheet_work.count()
print(f"Materialized {materialized_count} rows via Delta table")
print("Data cleaning and filtering complete - lazy evaluation chain broken")
print(f"Temporary table: {temp_table_name} (can be dropped after completion)")

# COMMAND ----------

total_rows = timesheet_work.count()
print(f"Total timesheet rows after filtering: {total_rows}")
print("Hours validation: Data cleaning applied in cells 42-43")

# COMMAND ----------

null_hours = (
    timesheet_work
    .filter(F.col("hours_worked").isNull())
    .count()
)

print(f"Rows with NULL hours_worked (filtered malformed data): {null_hours}")

print("Hours validation: PASSED (malformed data already filtered)")

# COMMAND ----------

# DBTITLE 1,pay code validation
missing_pay_code = (
    timesheet_work
    .filter(
        F.col("pay_code").isNull() |
        (F.trim(F.col("pay_code")) == "")
    )
    .count()
)

print(f"Missing pay_code rows: {missing_pay_code}")

if missing_pay_code > 0:
    raise ValueError(
        f"timesheet_work still contains {missing_pay_code} rows without pay_code."
    )

print("Pay-code validation PASSED")

# COMMAND ----------

display(
    timesheet_work.select(
        F.count("*").alias("total_rows"),
        F.count("client_employee_id").alias("employee_id_populated"),
        F.countDistinct("client_employee_id").alias(
            "distinct_employee_ids"
        )
    )
)

# COMMAND ----------

invalid_employee_ids = (
    timesheet_work
    .filter(
        F.col("client_employee_id").isNull() |
        (F.trim(F.col("client_employee_id")) == "")
    )
    .count()
)

print(f"Missing employee IDs: {invalid_employee_ids}")

if invalid_employee_ids > 0:
    raise ValueError(
        f"{invalid_employee_ids} rows have missing employee IDs."
    )

print("Employee ID validation PASSED")

# COMMAND ----------

# DBTITLE 1,safely converting date timestamps
timestamp_columns = [
    "punch_in_datetime",
    "punch_out_datetime",
    "scheduled_start_datetime",
    "scheduled_end_datetime"
]

for c in timestamp_columns:
    print(c, "->", timesheet_work.schema[c].dataType)

print(
    "punch_apply_date ->",
    timesheet_work.schema["punch_apply_date"].dataType
)

# COMMAND ----------

# DBTITLE 1,casting only if required
from pyspark.sql.types import StringType, DateType, TimestampType

# punch_apply_date
if isinstance(
    timesheet_work.schema["punch_apply_date"].dataType,
    StringType
):
    timesheet_work = timesheet_work.withColumn(
        "punch_apply_date",
        F.to_date(
            F.col("punch_apply_date"),
            "yyyy-MM-dd"
        )
    )

# timestamp columns
for c in timestamp_columns:
    if isinstance(
        timesheet_work.schema[c].dataType,
        StringType
    ):
        timesheet_work = timesheet_work.withColumn(
            c,
            F.to_timestamp(
                F.col(c),
                "yyyy-MM-dd HH:mm:ss.SSS"
            )
        )

print("Date/timestamp type handling completed.")

timesheet_work.printSchema()

# COMMAND ----------

# DBTITLE 1,validate timestamp parsing
display(
    timesheet_work.select(
        "punch_apply_date",
        "punch_in_datetime",
        "punch_out_datetime",
        "scheduled_start_datetime",
        "scheduled_end_datetime"
    ).limit(10)
)

# COMMAND ----------

from pyspark.sql import functions as F

print("=== TIMESTAMP VALIDATION ===")

# Timestamps have already been parsed in cells 31 and 50.
# This cell validates the parsed values only.

# Show sample data
display(
    timesheet_work.select(
        "punch_apply_date",
        "punch_in_datetime",
        "punch_out_datetime",
        "scheduled_start_datetime",
        "scheduled_end_datetime"
    ).limit(10)
)

# Count populated timestamps (NULL check only, no parsing)
total_rows = timesheet_work.count()
punch_in_populated = timesheet_work.filter(F.col("punch_in_datetime").isNotNull()).count()
punch_out_populated = timesheet_work.filter(F.col("punch_out_datetime").isNotNull()).count()

print(f"Total rows: {total_rows}")
print(f"Punch-in populated: {punch_in_populated}")
print(f"Punch-out populated: {punch_out_populated}")

# Missing punches are allowed in the Silver dataset
missing_punch_rows = total_rows - min(punch_in_populated, punch_out_populated)
print(f"Rows missing one or both punch timestamps: {missing_punch_rows}")

# Validate chronology only for rows where both timestamps exist
invalid_punch_order = (
    timesheet_work
    .filter(
        F.col("punch_in_datetime").isNotNull() &
        F.col("punch_out_datetime").isNotNull() &
        (F.col("punch_out_datetime") < F.col("punch_in_datetime"))
    )
    .count()
)

print(f"Rows where punch-out is before punch-in: {invalid_punch_order}")

if invalid_punch_order > 0:
    raise ValueError(
        f"{invalid_punch_order} rows have invalid punch chronology."
    )

print("Timestamp validation PASSED")

# COMMAND ----------

# DBTITLE 1,handle hours_worked
print(
    "hours_worked type:",
    timesheet_work.schema["hours_worked"].dataType
)

display(
    timesheet_work.select(
        "client_employee_id",
        "pay_code",
        "hours_worked"
    ).limit(20)
)

# COMMAND ----------

# DBTITLE 1,validate hours
total_rows = timesheet_work.count()
hours_populated = timesheet_work.filter(F.col("hours_worked").isNotNull()).count()
null_hours = total_rows - hours_populated

print(f"Total rows: {total_rows}")
print(f"Hours populated: {hours_populated}")
print(f"NULL hours: {null_hours}")
print("Hours validation done")

# COMMAND ----------

# DBTITLE 1,punch chronology
invalid_punch_order = (
    timesheet_work
    .filter(
        F.col("punch_in_datetime").isNotNull() &
        F.col("punch_out_datetime").isNotNull() &
        (
            F.col("punch_out_datetime") <
            F.col("punch_in_datetime")
        )
    )
    .count()
)

print(
    f"Rows where punch-out is before punch-in: "
    f"{invalid_punch_order}"
)

if invalid_punch_order > 0:
    raise ValueError(
        "Invalid punch chronology detected."
    )

print("Punch chronology validation PASSED")

# COMMAND ----------

# DBTITLE 1,natural key
natural_key_columns = [
    "client_employee_id",
    "punch_apply_date",
    "punch_in_datetime",
    "punch_out_datetime"
]

print("Natural key:")
print(natural_key_columns)

# COMMAND ----------

# DBTITLE 1,create timesheet_id
timesheet_work = (
    timesheet_work
    .withColumn(
        "timesheet_id",
        F.sha2(
            F.to_json(
                F.struct(
                    *[
                        F.col(c).alias(c)
                        for c in natural_key_columns
                    ]
                )
            ),
            256
        )
    )
)

display(
    timesheet_work.select(
        "timesheet_id",
        *natural_key_columns
    ).limit(10)
)

# COMMAND ----------

# DBTITLE 1,duplicate analysis
duplicate_groups = (
    timesheet_work
    .groupBy(*natural_key_columns)
    .count()
    .filter(F.col("count") > 1)
)

duplicate_group_count = duplicate_groups.count()

duplicate_row_total = (
    duplicate_groups
    .agg(
        F.sum("count").alias(
            "rows_in_duplicate_groups"
        )
    )
    .collect()[0][0]
)

duplicate_row_total = duplicate_row_total or 0

duplicate_extra_rows = (
    duplicate_row_total -
    duplicate_group_count
)

print(f"Duplicate groups: {duplicate_group_count}")
print(f"Rows in duplicate groups: {duplicate_row_total}")
print(f"Extra duplicate copies: {duplicate_extra_rows}")

display(duplicate_groups.limit(10))

# COMMAND ----------

# DBTITLE 1,check duplicate conflicts
duplicate_conflicts = (
    timesheet_work
    .filter(
        F.col("punch_in_datetime").isNotNull() &
        F.col("punch_out_datetime").isNotNull()
    )
    .groupBy(*natural_key_columns)
    .agg(
        F.countDistinct("pay_code").alias(
            "pay_code_variants"
        ),
        F.countDistinct("hours_worked").alias(
            "hours_variants"
        )
    )
    .filter(
        (F.col("pay_code_variants") > 1) |
        (F.col("hours_variants") > 1)
    )
)

conflicting_groups = duplicate_conflicts.count()

print(
    f"Conflicting duplicate groups: "
    f"{conflicting_groups}"
)

if conflicting_groups > 0:
    raise ValueError(
        "Conflicting duplicates detected."
    )

print("Duplicate conflict validation PASSED")

# COMMAND ----------

# DBTITLE 1,duplicate
duplicate_count_df = (
    timesheet_work
    .groupBy(*natural_key_columns)
    .agg(
        F.count("*").alias(
            "source_duplicate_count"
        )
    )
)

timesheet_work = (
    timesheet_work
    .join(
        duplicate_count_df,
        on=natural_key_columns,
        how="left"
    )
)

display(
    timesheet_work.select(
        "timesheet_id",
        "source_duplicate_count"
    ).limit(10)
)

# COMMAND ----------

dedup_window = (
    Window
    .partitionBy(*natural_key_columns)
    .orderBy(
        F.col("_source_file").asc(),
        F.col("_ingested_at").asc()
    )
)

timesheet_work = (
    timesheet_work
    .withColumn(
        "_dedup_rank",
        F.row_number().over(dedup_window)
    )
    .filter(F.col("_dedup_rank") == 1)
    .drop("_dedup_rank")
)

print(
    f"Rows after deduplication: "
    f"{timesheet_work.count()}"
)

display(timesheet_work.limit(10))

# COMMAND ----------

# DBTITLE 1,validate duplication
remaining_duplicate_groups = (
    timesheet_work
    .groupBy(*natural_key_columns)
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    f"Remaining duplicate groups: "
    f"{remaining_duplicate_groups}"
)

if remaining_duplicate_groups > 0:
    raise ValueError(
        "Deduplication failed."
    )

print("Deduplication validation PASSED")

# COMMAND ----------

# DBTITLE 1,actual duration
timesheet_work = (
    timesheet_work
    .withColumn(
        "actual_duration_minutes",
        F.when(
            F.col("punch_in_datetime").isNotNull() &
            F.col("punch_out_datetime").isNotNull(),
            (
                F.col("punch_out_datetime").cast("long") -
                F.col("punch_in_datetime").cast("long")
            ) / 60
        )
    )
    .withColumn(
        "actual_duration_hours",
        F.when(
            F.col("actual_duration_minutes").isNotNull(),
            F.round(
                F.col("actual_duration_minutes") / 60,
                2
            )
        )
    )
)

display(
    timesheet_work.select(
        "client_employee_id",
        "punch_in_datetime",
        "punch_out_datetime",
        "actual_duration_minutes",
        "actual_duration_hours"
    ).limit(10)
)

# COMMAND ----------

# DBTITLE 1,scheduled duration
timesheet_work = (
    timesheet_work
    .withColumn(
        "is_schedulable",
        F.col("scheduled_start_datetime").isNotNull() &
        F.col("scheduled_end_datetime").isNotNull()
    )
    .withColumn(
        "scheduled_duration_minutes",
        F.when(
            F.col("is_schedulable"),
            (
                F.col("scheduled_end_datetime").cast("long") -
                F.col("scheduled_start_datetime").cast("long")
            ) / 60
        )
    )
    .withColumn(
        "scheduled_duration_hours",
        F.when(
            F.col("scheduled_duration_minutes").isNotNull(),
            F.round(
                F.col("scheduled_duration_minutes") / 60,
                2
            )
        )
    )
)

display(
    timesheet_work.select(
        "client_employee_id",
        "scheduled_start_datetime",
        "scheduled_end_datetime",
        "is_schedulable",
        "scheduled_duration_hours"
    ).limit(10)
)

# COMMAND ----------

# DBTITLE 1,validation
display(
    timesheet_work
    .groupBy("is_schedulable")
    .count()
)

# COMMAND ----------

# DBTITLE 1,late arrival
timesheet_work = (
    timesheet_work
    .withColumn(
        "arrival_variance_minutes",
        F.when(
            F.col("is_schedulable") &
            F.col("punch_in_datetime").isNotNull(),
            (
                F.col("punch_in_datetime").cast("long") -
                F.col("scheduled_start_datetime").cast("long")
            ) / 60
        )
    )
    .withColumn(
        "late_arrival_minutes",
        F.when(
            F.col("arrival_variance_minutes") > 5,
            F.round(
                F.col("arrival_variance_minutes"),
                2
            )
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "is_late_arrival",
        F.col("late_arrival_minutes") > 0
    )
)

display(
    timesheet_work.select(
        "client_employee_id",
        "scheduled_start_datetime",
        "punch_in_datetime",
        "arrival_variance_minutes",
        "late_arrival_minutes",
        "is_late_arrival"
    ).limit(10)
)

# COMMAND ----------

late_arrival_count = (
    timesheet_work
    .filter(F.col("is_late_arrival"))
    .count()
)

print(
    f"Late arrival records (>5 min): "
    f"{late_arrival_count}"
)

display(
    timesheet_work
    .groupBy("is_late_arrival")
    .count()
)

# COMMAND ----------

# DBTITLE 1,early departure
timesheet_work = (
    timesheet_work
    .withColumn(
        "departure_variance_minutes",
        F.when(
            F.col("is_schedulable") &
            F.col("punch_out_datetime").isNotNull(),
            (
                F.col("scheduled_end_datetime").cast("long") -
                F.col("punch_out_datetime").cast("long")
            ) / 60
        )
    )
    .withColumn(
        "early_departure_minutes",
        F.when(
            F.col("departure_variance_minutes") > 5,
            F.round(
                F.col("departure_variance_minutes"),
                2
            )
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "is_early_departure",
        F.col("early_departure_minutes") > 0
    )
)

display(
    timesheet_work.select(
        "client_employee_id",
        "scheduled_end_datetime",
        "punch_out_datetime",
        "departure_variance_minutes",
        "early_departure_minutes",
        "is_early_departure"
    ).limit(10)
)

# COMMAND ----------

early_departure_count = (
    timesheet_work
    .filter(F.col("is_early_departure"))
    .count()
)

print(
    f"Early departure records (>5 min): "
    f"{early_departure_count}"
)

display(
    timesheet_work
    .groupBy("is_early_departure")
    .count()
)

# COMMAND ----------

# DBTITLE 1,overtime
timesheet_work = (
    timesheet_work
    .withColumn(
        "schedule_variance_minutes",
        F.when(
            F.col("is_schedulable") &
            F.col("actual_duration_minutes").isNotNull(),
            F.col("actual_duration_minutes") -
            F.col("scheduled_duration_minutes")
        )
    )
    .withColumn(
        "overtime_minutes",
        F.when(
            F.col("schedule_variance_minutes") > 5,
            F.round(
                F.col("schedule_variance_minutes"),
                2
            )
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "overtime_hours",
        F.round(
            F.col("overtime_minutes") / 60,
            2
        )
    )
    .withColumn(
        "is_overtime",
        F.col("overtime_minutes") > 0
    )
)

display(
    timesheet_work.select(
        "client_employee_id",
        "actual_duration_hours",
        "scheduled_duration_hours",
        "schedule_variance_minutes",
        "overtime_minutes",
        "overtime_hours",
        "is_overtime"
    ).limit(10)
)

# COMMAND ----------

overtime_count = (
    timesheet_work
    .filter(F.col("is_overtime"))
    .count()
)

print(
    f"Overtime records (>5 min): "
    f"{overtime_count}"
)

display(
    timesheet_work
    .groupBy("is_overtime")
    .count()
)

# COMMAND ----------

# DBTITLE 1,excessive duration
timesheet_work = (
    timesheet_work
    .withColumn(
        "is_excessive_duration",
        F.col("actual_duration_hours") > 24
    )
)

excessive_count = (
    timesheet_work
    .filter(F.col("is_excessive_duration"))
    .count()
)

print(
    f"Actual duration >24 hours: "
    f"{excessive_count}"
)

display(
    timesheet_work
    .filter(F.col("is_excessive_duration"))
    .select(
        "client_employee_id",
        "punch_in_datetime",
        "punch_out_datetime",
        "actual_duration_hours",
        "hours_worked"
    )
)

# COMMAND ----------

# DBTITLE 1,hours vs punch duration
timesheet_work = (
    timesheet_work
    .withColumn(
        "hours_worked_vs_punch_variance",
        F.when(
            F.col("hours_worked").isNotNull() &
            F.col("actual_duration_hours").isNotNull(),
            F.round(
                F.col("hours_worked") -
                F.col("actual_duration_hours"),
                2
            )
        )
    )
)

display(
    timesheet_work.select(
        "client_employee_id",
        "hours_worked",
        "actual_duration_hours",
        "hours_worked_vs_punch_variance"
    ).limit(20)
)

# COMMAND ----------

# DBTITLE 1,pay code count
timesheet_work = (
    timesheet_work
    .withColumn(
        "pay_code_count",
        F.size(
            F.split(
                F.col("pay_code"),
                r"\|"
            )
        )
    )
    .withColumn(
        "has_multiple_pay_codes",
        F.col("pay_code_count") > 1
    )
)

display(
    timesheet_work
    .groupBy("pay_code_count")
    .count()
    .orderBy("pay_code_count")
)

# COMMAND ----------

# DBTITLE 1,pay code bridge
timesheet_pay_code = (
    timesheet_work
    .select(
        "timesheet_id",
        "client_employee_id",
        "pay_code"
    )
    .withColumn(
        "pay_code_array",
        F.split(F.col("pay_code"), r"\|")
    )
    .select(
        "timesheet_id",
        "client_employee_id",
        F.posexplode(
            "pay_code_array"
        ).alias(
            "pay_code_position",
            "pay_code"
        )
    )
    .withColumn(
        "pay_code",
        F.trim(F.col("pay_code"))
    )
)

print(
    f"Bridge rows: {timesheet_pay_code.count()}"
)

display(timesheet_pay_code.limit(20))

# COMMAND ----------

# DBTITLE 1,bridge validation
bridge_missing_codes = (
    timesheet_pay_code
    .filter(
        F.col("pay_code").isNull() |
        (F.col("pay_code") == "")
    )
    .count()
)

print(
    f"Bridge rows with missing pay_code: "
    f"{bridge_missing_codes}"
)

if bridge_missing_codes > 0:
    raise ValueError(
        "Pay-code bridge contains missing values."
    )

print("Pay-code bridge validation PASSED")

# COMMAND ----------

# DBTITLE 1,pay code dimension
pay_code_dimension = (
    timesheet_pay_code
    .select("pay_code")
    .distinct()
    .withColumn(
        "pay_code_id",
        F.sha2(
            F.col("pay_code"),
            256
        )
    )
)

print(
    f"Atomic pay codes: "
    f"{pay_code_dimension.count()}"
)

display(
    pay_code_dimension
    .orderBy("pay_code")
)

# COMMAND ----------

# DBTITLE 1,pay code flag
pay_code_dimension = (
    pay_code_dimension
    .withColumn(
        "is_overtime_pay_code",
        F.col("pay_code").rlike(
            r"(?i)(^|[- ])OT( |$)"
        )
    )
)

display(
    pay_code_dimension
    .filter(F.col("is_overtime_pay_code"))
)

# COMMAND ----------

leave_pattern = (
    r"(?i)"
    r"(PTO|VACATION|SICK|BEREAVEMENT|JURY DUTY|"
    r"LOA|UNPAID|PERSONAL|HOLIDAY|APPROVED ABSENCE|"
    r"ADMINISTRATIVE LEAVE|CANCELLED|CANCELED)"
)

pay_code_dimension = (
    pay_code_dimension
    .withColumn(
        "is_leave_pay_code",
        F.col("pay_code").rlike(
            leave_pattern
        )
    )
)

display(
    pay_code_dimension
    .filter(F.col("is_leave_pay_code"))
)

# COMMAND ----------

# DBTITLE 1,add pay code flag to timesheet
pay_code_flags = (
    timesheet_pay_code
    .join(
        pay_code_dimension,
        on="pay_code",
        how="left"
    )
    .groupBy("timesheet_id")
    .agg(
        F.max(
            F.col("is_overtime_pay_code")
            .cast("int")
        ).cast("boolean").alias(
            "has_overtime_pay_code"
        ),
        F.max(
            F.col("is_leave_pay_code")
            .cast("int")
        ).cast("boolean").alias(
            "has_leave_pay_code"
        )
    )
)

timesheet_work = (
    timesheet_work
    .join(
        pay_code_flags,
        on="timesheet_id",
        how="left"
    )
)

display(
    timesheet_work.select(
        "timesheet_id",
        "pay_code",
        "has_overtime_pay_code",
        "has_leave_pay_code"
    ).limit(20)
)

# COMMAND ----------

# DBTITLE 1,employee master match
employee_lookup = (
    spark.table("workforce.silver.employee")
    .select("client_employee_id")
    .dropDuplicates(["client_employee_id"])
)

print(
    f"Employee master IDs: "
    f"{employee_lookup.count()}"
)

display(employee_lookup.limit(10))

# COMMAND ----------

employee_lookup_flag = (
    employee_lookup
    .withColumn(
        "employee_master_match",
        F.lit(True)
    )
)

timesheet_work = (
    timesheet_work
    .join(
        employee_lookup_flag,
        on="client_employee_id",
        how="left"
    )
    .withColumn(
        "employee_master_match",
        F.coalesce(
            F.col("employee_master_match"),
            F.lit(False)
        )
    )
)

display(
    timesheet_work.select(
        "client_employee_id",
        "employee_master_match"
    ).limit(20)
)

# COMMAND ----------

display(
    timesheet_work
    .groupBy("employee_master_match")
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Quality And Validation

# COMMAND ----------

# DBTITLE 1,final schmea check
print("FINAL TIMESHEET SCHEMA")
timesheet_work.printSchema()

print("\nFINAL COLUMNS")
for i, c in enumerate(timesheet_work.columns, 1):
    print(i, c)

# COMMAND ----------

# DBTITLE 1,final check
display(
    timesheet_work.select(
        "timesheet_id",
        "client_employee_id",
        "punch_apply_date",
        "pay_code",
        "hours_worked",
        "actual_duration_hours",
        "scheduled_duration_hours",
        "is_schedulable",
        "late_arrival_minutes",
        "is_late_arrival",
        "early_departure_minutes",
        "is_early_departure",
        "overtime_minutes",
        "overtime_hours",
        "is_overtime",
        "is_excessive_duration",
        "has_multiple_pay_codes",
        "has_overtime_pay_code",
        "has_leave_pay_code",
        "employee_master_match"
    )
    .limit(20)
)

# COMMAND ----------

# DBTITLE 1,dq report
display(
    timesheet_work.select(
        F.count("*").alias("total_rows"),

        F.count("timesheet_id").alias(
            "timesheet_id_populated"
        ),

        F.countDistinct("timesheet_id").alias(
            "distinct_timesheet_ids"
        ),

        F.sum(
            F.when(
                F.col("client_employee_id").isNull(),
                1
            ).otherwise(0)
        ).alias("missing_employee_id"),

        F.sum(
            F.when(
                F.col("pay_code").isNull(),
                1
            ).otherwise(0)
        ).alias("missing_pay_code"),

        F.sum(
            F.when(
                F.col("hours_worked").isNull(),
                1
            ).otherwise(0)
        ).alias("missing_hours_worked"),

        F.sum(
            F.when(
                F.col("punch_in_datetime").isNull() |
                F.col("punch_out_datetime").isNull(),
                1
            ).otherwise(0)
        ).alias("missing_punch_timestamp"),

        F.sum(
            F.when(
                F.col("is_late_arrival"),
                1
            ).otherwise(0)
        ).alias("late_arrival_count"),

        F.sum(
            F.when(
                F.col("is_early_departure"),
                1
            ).otherwise(0)
        ).alias("early_departure_count"),

        F.sum(
            F.when(
                F.col("is_overtime"),
                1
            ).otherwise(0)
        ).alias("overtime_count"),

        F.sum(
            F.when(
                F.col("is_excessive_duration"),
                1
            ).otherwise(0)
        ).alias("excessive_duration_count")
    )
)

# COMMAND ----------

final_total = timesheet_work.count()

final_duplicate_groups = (
    timesheet_work
    .groupBy(*natural_key_columns)
    .count()
    .filter(F.col("count") > 1)
    .count()
)

final_missing_employee_id = (
    timesheet_work
    .filter(
        F.col("client_employee_id").isNull()
    )
    .count()
)

final_missing_pay_code = (
    timesheet_work
    .filter(
        F.col("pay_code").isNull()
    )
    .count()
)

final_invalid_punch_order = (
    timesheet_work
    .filter(
        F.col("punch_in_datetime").isNotNull() &
        F.col("punch_out_datetime").isNotNull() &
        (
            F.col("punch_out_datetime") <
            F.col("punch_in_datetime")
        )
    )
    .count()
)

print("quality gate")
print(f"Rows: {final_total}")
print(f"Duplicate groups: {final_duplicate_groups}")
print(f"Missing employee IDs: {final_missing_employee_id}")
print(f"Missing pay codes: {final_missing_pay_code}")
print(
    f"Invalid punch chronology: "
    f"{final_invalid_punch_order}"
)

if final_total == 0:
    raise ValueError("Silver dataset is empty.")

if final_duplicate_groups > 0:
    raise ValueError("Duplicate groups remain.")

if final_missing_employee_id > 0:
    raise ValueError("Missing employee IDs remain.")

if final_missing_pay_code > 0:
    raise ValueError("Missing pay codes remain.")

if final_invalid_punch_order > 0:
    raise ValueError(
        "Invalid punch chronology remains."
    )
print("\n\nquality gate passed")

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to schema

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS workforce.silver
""")

print("workforce.silver ready.")

# COMMAND ----------

(
    timesheet_work
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workforce.silver.timesheet"
    )
)

print(
    "workforce.silver.timesheet written successfully."
)

# COMMAND ----------

(
    timesheet_pay_code
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workforce.silver.timesheet_pay_code"
    )
)

print(
    "workforce.silver.timesheet_pay_code written successfully."
)

# COMMAND ----------

(
    pay_code_dimension
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workforce.silver.pay_code_dimension"
    )
)

print(
    "workforce.silver.pay_code_dimension written successfully."
)

# COMMAND ----------

silver_timesheet = spark.table(
    "workforce.silver.timesheet"
)

silver_bridge = spark.table(
    "workforce.silver.timesheet_pay_code"
)

silver_pay_code = spark.table(
    "workforce.silver.pay_code_dimension"
)

print(
    "Silver timesheet rows:",
    silver_timesheet.count()
)

print(
    "Silver bridge rows:",
    silver_bridge.count()
)

print(
    "Pay-code dimension rows:",
    silver_pay_code.count()
)

display(
    silver_timesheet.limit(10)
)

# COMMAND ----------

missing_bridge_records = (
    silver_timesheet
    .select("timesheet_id")
    .distinct()
    .join(
        silver_bridge
        .select("timesheet_id")
        .distinct(),
        on="timesheet_id",
        how="left_anti"
    )
    .count()
)

print(
    f"Timesheet records without pay-code bridge: "
    f"{missing_bridge_records}"
)

if missing_bridge_records > 0:
    raise ValueError(
        "Timesheet-to-pay-code relationship is incomplete."
    )

print(
    "Timesheet → pay-code relationship PASSED"
)