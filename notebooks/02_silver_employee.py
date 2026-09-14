# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver Employee Transformation
# MAGIC
# MAGIC This notebook transforms the Bronze employee dataset into a cleaned and
# MAGIC analytics-ready Silver employee table.
# MAGIC
# MAGIC ## Objectives
# MAGIC
# MAGIC - Read employee data from the Bronze Delta table
# MAGIC - Standardize column names and string values
# MAGIC - Convert dates and numeric fields to appropriate data types
# MAGIC - Handle blank values
# MAGIC - Correct the mislabeled organization_id source field
# MAGIC - Split department code and department name
# MAGIC - Preserve employee and manager relationships
# MAGIC - Derive employee status and tenure attributes
# MAGIC - Validate employee records
# MAGIC - Write the resulting dataset to the Silver Delta layer

# COMMAND ----------

# DBTITLE 1,imports
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,config
BRONZE_TABLE = "workforce.bronze.employee"
SILVER_TABLE = "workforce.silver.employee"

print("Source:", BRONZE_TABLE)
print("Target:", SILVER_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC #####reading and profiling the table

# COMMAND ----------

# DBTITLE 1,read from bronze
employee_bronze = spark.table(BRONZE_TABLE)

print(f"Bronze row count: {employee_bronze.count()}")
print(f"Bronze column count: {len(employee_bronze.columns)}")

employee_bronze.printSchema()

# COMMAND ----------

display(employee_bronze.limit(10))

# COMMAND ----------

# DBTITLE 1,initial profiling
print("=== Employee Source Profile ===")

print(f"Rows: {employee_bronze.count()}")
print(f"Columns: {len(employee_bronze.columns)}")

print("\nDistinct employee IDs:")
print(employee_bronze.select("client_employee_id").distinct().count())

print("\nDistinct departments:")
print(employee_bronze.select("department_id").distinct().count())

print("\nDistinct organizations:")
print(employee_bronze.select("organization_name").distinct().count())

print("\nDistinct managers:")
print(employee_bronze.select("manager_employee_id").distinct().count())

# COMMAND ----------

# DBTITLE 1,check duplicate employees
duplicate_employees = (
    employee_bronze
    .groupBy("client_employee_id")
    .count()
    .filter(F.col("count") > 1)
)

display(duplicate_employees)

# COMMAND ----------

# DBTITLE 1,duplicate count
duplicate_count = duplicate_employees.count()

print(f"Duplicate employee IDs: {duplicate_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### transformations

# COMMAND ----------

# DBTITLE 1,standardize empty values
employee_clean = employee_bronze

for column_name in employee_bronze.columns:
    if column_name.startswith("_"):
        continue

    employee_clean = employee_clean.withColumn(
        column_name,
        F.when(
            F.trim(F.col(column_name)) == "",
            F.lit(None)
        ).otherwise(F.trim(F.col(column_name)))
    )

# COMMAND ----------

display(
    employee_clean.select(
        [
            F.count(
                F.when(F.col(c).isNull(), 1)
            ).alias(c)
            for c in employee_clean.columns
            if not c.startswith("_")
        ]
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC dropping organization_id because it contains names instead of id (duplicate of organization_name)

# COMMAND ----------

employee_clean = (
    employee_clean
    .drop("organization_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC splitting dept code and dept name
# MAGIC
# MAGIC values are like:
# MAGIC
# MAGIC department_id = 72800
# MAGIC
# MAGIC department_name = 72800-CT Scan
# MAGIC
# MAGIC but we want:
# MAGIC
# MAGIC department_id = 72800
# MAGIC
# MAGIC department_name = 72800-CT Scan

# COMMAND ----------

# DBTITLE 1,splitting
employee_clean = (
    employee_clean
    .withColumn(
        "department_code",
        F.regexp_extract(
            F.col("department_name"),
            r"^([^-]+)-",
            1
        )
    )
    .withColumn(
        "department_name_clean",
        F.regexp_extract(
            F.col("department_name"),
            r"^[^-]+-(.*)$",
            1
        )
    )
)

# COMMAND ----------

print("Department-related columns:")
print([
    c for c in employee_clean.columns
    if "department" in c.lower()
])

display(
    employee_clean.select(
        "department_id",
        "department_name",
        "department_code",
        "department_name_clean"
    ).limit(10)
)

# COMMAND ----------

employee_clean = employee_clean.withColumn(
    "department_name_final",
    F.when(
        F.col("department_name_clean").isNotNull() &
        (F.trim(F.col("department_name_clean")) != ""),
        F.trim(F.col("department_name_clean"))
    ).otherwise(
        F.col("department_name")
    )
)

# COMMAND ----------

# DBTITLE 1,transfromation dept
# Remove temporary columns

employee_clean = (
    employee_clean
    .drop("department_name", "department_name_clean")
    .withColumnRenamed(
        "department_name_final",
        "department_name"
    )
)

print("Department transformation finalized.")

# COMMAND ----------

# DBTITLE 1,validation for dept_name
print("Department columns:")

department_columns = [
    c for c in employee_clean.columns
    if "department" in c.lower()
]

print(department_columns)

display(
    employee_clean.select(
        "department_id",
        "department_code",
        "department_name"
    ).limit(10)
)

# COMMAND ----------

# DBTITLE 1,validating data
display(employee_clean.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC adding a checkpoint for in between validation

# COMMAND ----------

# DBTITLE 1,validation
print(f"Rows: {employee_clean.count()}")
print(f"Columns: {len(employee_clean.columns)}")

print("\nColumns:")
for i, column_name in enumerate(employee_clean.columns, 1):
    print(f"{i:02d}. {column_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardization

# COMMAND ----------

# DBTITLE 1,standardize string values
# Columns that should be treated as strings at this stage

string_columns = [
    "client_employee_id",
    "first_name",
    "middle_name",
    "last_name",
    "preferred_name",
    "job_code",
    "job_title",
    "job_start_date",
    "organization_name",
    "department_id",
    "department_code",
    "department_name",
    "dob",
    "hire_date",
    "recent_hire_date",
    "anniversary_date",
    "term_date",
    "years_of_experience",
    "work_email",
    "address",
    "city",
    "state",
    "zip",
    "country",
    "manager_employee_id",
    "manager_employee_name",
    "fte_status",
    "is_per_deim",
    "cell_phone",
    "work_phone",
    "scheduled_weekly_hour",
    "active_status",
    "termination_reason",
    "clinical_level"
]

existing_string_columns = [
    c for c in string_columns
    if c in employee_clean.columns
]

missing_string_columns = [
    c for c in string_columns
    if c not in employee_clean.columns
]

print("Columns to standardize:")
print(existing_string_columns)

print("\nColumns not present:")
print(missing_string_columns)

for c in existing_string_columns:
    employee_clean = employee_clean.withColumn(
        c,
        F.when(
            F.trim(F.col(c)) == "",
            F.lit(None)
        ).otherwise(
            F.trim(F.col(c))
        )
    )

print("\nString standardization completed.")

# COMMAND ----------

# DBTITLE 1,validation
display(
    employee_clean.select(
        "client_employee_id",
        "first_name",
        "last_name",
        "job_title",
        "organization_name",
        "department_name",
        "city",
        "state",
        "country"
    ).limit(10)
)

# COMMAND ----------

# DBTITLE 1,inspect values before cast
date_columns = [
    "job_start_date",
    "dob",
    "hire_date",
    "recent_hire_date",
    "anniversary_date",
    "term_date"
]

missing_date_columns = [
    c for c in date_columns
    if c not in employee_clean.columns
]

print("Date columns:")
print(date_columns)

print("\nMissing date columns:")
print(missing_date_columns)

if missing_date_columns:
    raise ValueError(
        f"Expected date columns are missing: {missing_date_columns}"
    )

# COMMAND ----------

display(
    employee_clean.select(
        *date_columns
    ).limit(10)
)

# COMMAND ----------

# DBTITLE 1,convert date cols
for c in date_columns:
    employee_clean = employee_clean.withColumn(
        c,
        F.to_date(F.col(c), "yyyy-MM-dd")
    )

# COMMAND ----------

# DBTITLE 1,validation
employee_clean.printSchema()

# COMMAND ----------

# DBTITLE 1,date conversion validation
invalid_hire_dates = (
    employee_clean
    .filter(F.col("hire_date").isNull())
)

print(
    "Employees with NULL hire_date:",
    invalid_hire_dates.count()
)

# COMMAND ----------

# DBTITLE 1,standardize identifier cols
identifier_columns = [
    "client_employee_id",
    "job_code",
    "department_id",
    "department_code",
    "manager_employee_id"
]

missing_identifier_columns = [
    c for c in identifier_columns
    if c not in employee_clean.columns
]

for c in identifier_columns:
    employee_clean = employee_clean.withColumn(
        c,
        F.col(c).cast("string")
    )

print("Identifier columns converted to string.")

# COMMAND ----------

# DBTITLE 1,convert scheduled weekly hours
required_column = "scheduled_weekly_hour"

employee_clean = employee_clean.withColumn(
    "scheduled_weekly_hour",
    F.col("scheduled_weekly_hour").cast("decimal(10,2)")
)

print("scheduled_weekly_hour converted to decimal.")

# COMMAND ----------

employee_clean.printSchema()

# COMMAND ----------

# DBTITLE 1,convert active status to boolean
employee_clean = employee_clean.withColumn(
    "active_status",
    F.when(
        F.col("active_status") == "1",
        F.lit(True)
    )
    .when(
        F.col("active_status") == "0",
        F.lit(False)
    )
    .otherwise(
        F.lit(None).cast("boolean")
    )
)

print("active_status converted to boolean.")

# COMMAND ----------

# DBTITLE 1,standardize fte status
employee_clean = employee_clean.withColumn(
    "fte_status",
    F.initcap(
        F.regexp_replace(
            F.col("fte_status"),
            "_",
            " "
        )
    )
)

print("fte_status standardized.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### cleaning termination reason
# MAGIC
# MAGIC i want to clean the termination reason so its easier to process
# MAGIC
# MAGIC from - Terminate_Employee_Voluntary
# MAGIC to - Voluntary

# COMMAND ----------

employee_clean = employee_clean.withColumn(
    "termination_reason",
    F.when(
        F.col("termination_reason").isNull(),
        F.lit(None)
    ).otherwise(
        F.regexp_replace(
            F.col("termination_reason"),
            r"^Terminate_Employee_",
            ""
        )
    )
)

# COMMAND ----------

# DBTITLE 1,create employment_status
employee_clean = employee_clean.withColumn(
    "employment_status",
    F.when(
        F.col("active_status") == True,
        F.lit("Active")
    )
    .when(
        F.col("active_status") == False,
        F.lit("Terminated")
    )
    .otherwise(
        F.lit("Unknown")
    )
)

# COMMAND ----------

# DBTITLE 1,create full name
employee_clean = employee_clean.withColumn(
    "full_name",
    F.concat_ws(
        " ",
        F.col("first_name"),
        F.col("last_name")
    )
)

# COMMAND ----------

# DBTITLE 1,validation
display(employee_clean.limit(10))

# COMMAND ----------

# DBTITLE 1,validate manager relationship
required_manager_columns = [
    "client_employee_id",
    "manager_employee_id"
]

missing_manager_columns = [
    c for c in required_manager_columns
    if c not in employee_clean.columns
]

if missing_manager_columns:
    raise ValueError(
        f"Missing manager columns: {missing_manager_columns}"
    )

manager_lookup = (
    employee_clean
    .select(
        F.col("client_employee_id")
        .alias("manager_employee_id_lookup")
    )
    .distinct()
)

manager_validation = (
    employee_clean.alias("employee")
    .join(
        manager_lookup.alias("manager"),
        F.col("employee.manager_employee_id") ==
        F.col("manager.manager_employee_id_lookup"),
        "left"
    )
    .select(
        F.col("employee.client_employee_id"),
        F.col("employee.manager_employee_id"),
        F.col("manager.manager_employee_id_lookup")
    )
)

print("Manager relationship validation created.")

# COMMAND ----------

display(
    manager_validation
    .filter(
        F.col("manager_employee_id").isNotNull() &
        F.col("manager_employee_id_lookup").isNull()
    )
)

# COMMAND ----------

# DBTITLE 1,add manager exists flag
manager_lookup = (
    employee_clean
    .select(
        F.col("client_employee_id")
        .alias("manager_lookup_id")
    )
    .distinct()
)

employee_clean = (
    employee_clean.alias("employee")
    .join(
        manager_lookup.alias("manager"),
        F.col("employee.manager_employee_id") ==
        F.col("manager.manager_lookup_id"),
        "left"
    )
    .withColumn(
        "manager_exists",
        F.when(
            F.col("employee.manager_employee_id").isNull(),
            F.lit(None).cast("boolean")
        )
        .otherwise(
            F.col("manager.manager_lookup_id").isNotNull()
        )
    )
    .drop("manager_lookup_id")
)

# COMMAND ----------

display(
    employee_clean.select(
        "client_employee_id",
        "manager_employee_id",
        "manager_exists"
    ).limit(10)
)

# COMMAND ----------

display(
    employee_clean
    .groupBy("manager_exists")
    .count()
)

# COMMAND ----------

# DBTITLE 1,validate employee uniqueness
if "client_employee_id" not in employee_clean.columns:
    raise ValueError(
        "client_employee_id is missing."
    )

duplicate_employee_ids = (
    employee_clean
    .groupBy("client_employee_id")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_count = duplicate_employee_ids.count()

print("Duplicate employee IDs:", duplicate_count)

# COMMAND ----------

# DBTITLE 1,validation
display(duplicate_employee_ids)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Create deterministic analysis date
# MAGIC For tenure calculations, I don't want to use current_date()
# MAGIC Why?
# MAGIC Because your result would change every time you rerun the notebook.
# MAGIC Our source data runs through October 16, 2025, so we'll use that as the analysis/reporting cutoff

# COMMAND ----------

AS_OF_DATE = "2025-10-16"

print("Silver employee analysis date:", AS_OF_DATE)

# COMMAND ----------

# DBTITLE 1,create tenure end date
required_tenure_columns = [
    "hire_date",
    "term_date"
]

missing_tenure_columns = [
    c for c in required_tenure_columns
    if c not in employee_clean.columns
]

if missing_tenure_columns:
    raise ValueError(
        f"Missing tenure columns: {missing_tenure_columns}"
    )

employee_clean = employee_clean.withColumn(
    "tenure_end_date",
    F.coalesce(
        F.col("term_date"),
        F.to_date(F.lit(AS_OF_DATE))
    )
)

print("tenure_end_date created.")

# COMMAND ----------

display(
    employee_clean.select(
        "client_employee_id",
        "hire_date",
        "term_date",
        "employment_status",
        "tenure_end_date"
    ).limit(10)
)

# COMMAND ----------

employee_clean = (
    employee_clean
    .withColumn(
        "tenure_days",
        F.datediff(
            F.col("tenure_end_date"),
            F.col("hire_date")
        )
    )
    .withColumn(
        "tenure_years",
        F.round(
            F.col("tenure_days") / F.lit(365.25),
            2
        )
    )
)

print("Tenure metrics created.")

# COMMAND ----------

display(
    employee_clean.select(
        "client_employee_id",
        "hire_date",
        "term_date",
        "tenure_end_date",
        "tenure_days",
        "tenure_years"
    ).limit(10)
)

# COMMAND ----------

# DBTITLE 1,validate tenure
invalid_tenure = (
    employee_clean
    .filter(
        F.col("tenure_days").isNull() |
        (F.col("tenure_days") < 0)
    )
)

invalid_tenure_count = invalid_tenure.count()

print(
    "Employees with invalid tenure:",
    invalid_tenure_count
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### create early attrition flag
# MAGIC Early attrition = termination within 90 days of hire.
# MAGIC
# MAGIC We calculate this at employee level in Silver. Gold can later aggregate it into the KPI.

# COMMAND ----------

employee_clean = employee_clean.withColumn(
    "days_to_termination",
    F.when(
        F.col("term_date").isNotNull(),
        F.datediff(
            F.col("term_date"),
            F.col("hire_date")
        )
    )
)

employee_clean = employee_clean.withColumn(
    "is_early_attrition",
    F.when(
        F.col("days_to_termination").isNull(),
        F.lit(False)
    )
    .when(
        (F.col("days_to_termination") >= 0) &
        (F.col("days_to_termination") <= 90),
        F.lit(True)
    )
    .otherwise(
        F.lit(False)
    )
)

print("Early attrition flag created.")

# COMMAND ----------

# DBTITLE 1,validate active/termination consistency
active_with_term_date = (
    employee_clean
    .filter(
        (F.col("active_status") == True) &
        F.col("term_date").isNotNull()
    )
)

terminated_without_term_date = (
    employee_clean
    .filter(
        (F.col("active_status") == False) &
        F.col("term_date").isNull()
    )
)

print(
    "Active employees with termination date:",
    active_with_term_date.count()
)

print(
    "Terminated employees without termination date:",
    terminated_without_term_date.count()
)

# COMMAND ----------

# DBTITLE 1,validate hire/termination date
invalid_employment_dates = (
    employee_clean
    .filter(
        F.col("term_date").isNotNull() &
        F.col("hire_date").isNotNull() &
        (
            F.col("term_date") < F.col("hire_date")
        )
    )
)

print(
    "Employees with term_date before hire_date:",
    invalid_employment_dates.count()
)

# COMMAND ----------

# DBTITLE 1,validation
display(
    invalid_employment_dates.select(
        "client_employee_id",
        "hire_date",
        "term_date"
    )
)

# COMMAND ----------

# DBTITLE 1,schedule weekly hours
invalid_weekly_hours = (
    employee_clean
    .filter(
        F.col("scheduled_weekly_hour").isNull() |
        (F.col("scheduled_weekly_hour") < 0)
    )
)

print(
    "Employees with invalid scheduled weekly hours:",
    invalid_weekly_hours.count()
)

# COMMAND ----------

# DBTITLE 1,validate
display(
    invalid_weekly_hours.select(
        "client_employee_id",
        "scheduled_weekly_hour"
    )
)

# COMMAND ----------

# DBTITLE 1,check reqd emp ids
missing_employee_ids = (
    employee_clean
    .filter(
        F.col("client_employee_id").isNull() |
        (F.trim(F.col("client_employee_id")) == "")
    )
)

missing_employee_id_count = missing_employee_ids.count()

print(
    "Employees missing employee ID:",
    missing_employee_id_count
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### remove cols that are completely null
# MAGIC
# MAGIC middle_name
# MAGIC years_of_experience
# MAGIC zip
# MAGIC manager_employee_name
# MAGIC is_per_deim
# MAGIC work_phone
# MAGIC clinical_level
# MAGIC
# MAGIC these cols have no usable data so removing them
# MAGIC
# MAGIC

# COMMAND ----------

columns_to_remove = [
    "middle_name",
    "years_of_experience",
    "zip",
    "manager_employee_name",
    "is_per_deim",
    "work_phone",
    "clinical_level"
]

missing_columns = [
    c for c in columns_to_remove
    if c not in employee_clean.columns
]

print("Columns scheduled for Silver removal:")
print(columns_to_remove)

print("\nColumns not present:")
print(missing_columns)

employee_silver = employee_clean.drop(
    *[
        c for c in columns_to_remove
        if c in employee_clean.columns
    ]
)

print("\nCompletely-null source columns removed from Silver.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

print("Silver columns:")
print(employee_silver.columns)

# COMMAND ----------

display(
    employee_silver.limit(10)
)

# COMMAND ----------

# DBTITLE 1,final silver schema checkpoint
print("FINAL SILVER EMPLOYEE SCHEMA CHECK")

print(f"Rows: {employee_silver.count()}")
print(f"Columns: {len(employee_silver.columns)}")

employee_silver.printSchema()

# COMMAND ----------

display(
    employee_silver.select(
        "client_employee_id",
        "first_name",
        "last_name",
        "full_name",
        "job_code",
        "job_title",
        "organization_name",
        "department_id",
        "department_code",
        "department_name",
        "hire_date",
        "term_date",
        "manager_employee_id",
        "manager_exists",
        "fte_status",
        "scheduled_weekly_hour",
        "active_status",
        "employment_status",
        "termination_reason",
        "tenure_days",
        "tenure_years",
        "is_early_attrition"
    ).limit(10)
)

# COMMAND ----------

total_records = employee_silver.count()

missing_employee_ids = (
    employee_silver
    .filter(F.col("client_employee_id").isNull())
    .count()
)

duplicate_employee_ids = (
    employee_silver
    .groupBy("client_employee_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

invalid_dates = (
    employee_silver
    .filter(
        F.col("term_date").isNotNull() &
        F.col("hire_date").isNotNull() &
        (F.col("term_date") < F.col("hire_date"))
    )
    .count()
)

active_with_term = (
    employee_silver
    .filter(
        (F.col("active_status") == True) &
        F.col("term_date").isNotNull()
    )
    .count()
)

terminated_without_term = (
    employee_silver
    .filter(
        (F.col("active_status") == False) &
        F.col("term_date").isNull()
    )
    .count()
)

invalid_hours = (
    employee_silver
    .filter(
        F.col("scheduled_weekly_hour").isNull() |
        (F.col("scheduled_weekly_hour") < 0)
    )
    .count()
)

invalid_tenure = (
    employee_silver
    .filter(
        F.col("tenure_days").isNull() |
        (F.col("tenure_days") < 0)
    )
    .count()
)

early_attrition_count = (
    employee_silver
    .filter(F.col("is_early_attrition") == True)
    .count()
)

print("SILVER EMPLOYEE QUALITY REPORT \n")

print(f"Total records: {total_records}")
print(f"Missing employee IDs: {missing_employee_ids}")
print(f"Duplicate employee IDs: {duplicate_employee_ids}")
print(f"Invalid hire/term dates: {invalid_dates}")
print(f"Active employees with term date: {active_with_term}")
print(f"Terminated without term date: {terminated_without_term}")
print(f"Invalid weekly hours: {invalid_hours}")
print(f"Invalid tenure: {invalid_tenure}")
print(f"Early attrition employees: {early_attrition_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC hard validation gate
# MAGIC
# MAGIC notebook will fail rather than putting incorrect data

# COMMAND ----------

assert total_records == 50, (
    f"Expected 50 employee records, found {total_records}"
)

assert missing_employee_ids == 0, (
    f"Found {missing_employee_ids} missing employee IDs"
)

assert duplicate_employee_ids == 0, (
    f"Found {duplicate_employee_ids} duplicate employee IDs"
)

assert invalid_dates == 0, (
    f"Found {invalid_dates} invalid hire/termination dates"
)

assert active_with_term == 0, (
    f"Found {active_with_term} active employees with term dates"
)

assert terminated_without_term == 0, (
    f"Found {terminated_without_term} terminated employees without term dates"
)

assert invalid_hours == 0, (
    f"Found {invalid_hours} employees with invalid weekly hours"
)

assert invalid_tenure == 0, (
    f"Found {invalid_tenure} employees with invalid tenure"
)

print("validation passed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to delta table

# COMMAND ----------

SILVER_TABLE = "workforce.silver.employee"

spark.sql("""
    CREATE SCHEMA IF NOT EXISTS workforce.silver
""")

(
    employee_silver
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print(f"Silver table written successfully: {SILVER_TABLE}")

# COMMAND ----------

# DBTITLE 1,verify table
employee_silver_table = spark.table(
    "workforce.silver.employee"
)

print(
    "Silver table row count:",
    employee_silver_table.count()
)

print(
    "Silver table column count:",
    len(employee_silver_table.columns)
)

employee_silver_table.printSchema()

# COMMAND ----------

# DBTITLE 1,validation
display(
    employee_silver_table
    .select(
        "client_employee_id",
        "full_name",
        "department_code",
        "department_name",
        "employment_status",
        "tenure_years",
        "is_early_attrition"
    )
    .orderBy("client_employee_id")
)

# COMMAND ----------

# DBTITLE 1,final bronze -> silver check
bronze_count = spark.table(
    "workforce.bronze.employee"
).count()

silver_count = spark.table(
    "workforce.silver.employee"
).count()

print("Bronze employee records:", bronze_count)
print("Silver employee records:", silver_count)

if bronze_count != silver_count:
    raise ValueError(
        f"Unexpected population change: "
        f"Bronze={bronze_count}, Silver={silver_count}"
    )

print("Bronze → Silver population check PASSED.")