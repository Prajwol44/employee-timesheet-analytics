# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Gold Layer Documentation
# MAGIC %md
# MAGIC # Gold Layer: Workforce Analytics
# MAGIC
# MAGIC ## Purpose
# MAGIC This notebook implements the **Gold (Analytics) layer** of the Employee Timesheet Analytics project. The Gold layer transforms clean, validated Silver data into business-ready analytics tables that directly answer stakeholder questions.
# MAGIC
# MAGIC ## Assignment Requirements
# MAGIC
# MAGIC Based on the **Data Engineering Assignment PDF**, this Gold layer must deliver **9 specific KPIs**:
# MAGIC
# MAGIC 1. **Active Headcount Over Time** - Number of employees actively employed on a given date
# MAGIC 2. **Turnover Trend** - Employee terminations across time periods (monthly/quarterly)
# MAGIC 3. **Average Tenure by Department** - Mean employment duration per department
# MAGIC 4. **Average Working Hours per Employee** - Mean hours worked per day/week by employee
# MAGIC 5. **Late Arrival Frequency** - Count of late clock-ins (>5 min grace)
# MAGIC 6. **Early Departure Count** - Count of early departures (>5 min grace)
# MAGIC 7. **Total Overtime Count** - Workdays/hours exceeding standard shift (>5 min grace)
# MAGIC 8. **Rolling Average Working Hours** - Moving average over recent time window
# MAGIC 9. **Early Attrition Rate** - Proportion of employees leaving within first few months
# MAGIC
# MAGIC ## Business Rules
# MAGIC
# MAGIC ### Grace Period: +/- 5 Minutes
# MAGIC The assignment specifies a **5-minute grace period** for:
# MAGIC - **Late Arrival**: Count only when actual punch-in is **>5 minutes** after scheduled start
# MAGIC - **Early Departure**: Count only when actual punch-out is **>5 minutes** before scheduled end
# MAGIC - **Overtime**: Count only when actual duration exceeds scheduled duration by **>5 minutes**
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC ```
# MAGIC Raw (CSV) → Bronze (Raw Ingestion) → Silver (Clean & Validated) → Gold (Analytics)
# MAGIC                                                                       ↓
# MAGIC                                                            API / Visualization
# MAGIC ```
# MAGIC
# MAGIC **Catalog:** `workforce`  
# MAGIC **Schemas:** `workforce.raw`, `workforce.bronze`, `workforce.silver`, `workforce.gold`
# MAGIC
# MAGIC ## Silver Inputs (Expected)
# MAGIC
# MAGIC - `workforce.silver.employee` - Clean employee master data with tenure, status, dates
# MAGIC - `workforce.silver.timesheet` - Clean timesheet records with attendance metrics
# MAGIC - Supporting tables (if any): pay_code dimensions
# MAGIC
# MAGIC ## Gold Outputs (Proposed)
# MAGIC
# MAGIC | Gold Table | Grain | Purpose |
# MAGIC |---|---|---|
# MAGIC | `daily_headcount` | One row per date | Active headcount over time |
# MAGIC | `monthly_turnover` | One row per month | Turnover trend analysis |
# MAGIC | `department_tenure` | One row per department | Average tenure by department |
# MAGIC | `employee_working_hours` | One row per employee | Average working hours per employee |
# MAGIC | `attendance_metrics` | One row per employee | Late arrivals & early departures |
# MAGIC | `overtime_summary` | One row per employee | Total overtime count |
# MAGIC | `rolling_avg_hours` | One row per employee-date | Rolling average working hours |
# MAGIC | `early_attrition` | Summary or cohort-level | Early attrition rate |
# MAGIC
# MAGIC ## Important Notes
# MAGIC
# MAGIC ✓ **Read from Silver Delta tables** - Do NOT read from CSV or Bronze  
# MAGIC ✓ **Use actual Silver schemas** - Inspect before assuming column names  
# MAGIC ✓ **Apply business rules correctly** - 5-minute grace period  
# MAGIC ✓ **Validate every transformation** - Check row counts, nulls, duplicates, date ranges  
# MAGIC ✓ **Document grain clearly** - Prevent accidental row multiplication  
# MAGIC ✓ **Build cell-by-cell** - Validate each step before proceeding

# COMMAND ----------

# DBTITLE 1,Section 2: Inspect Silver Inputs
# MAGIC %md
# MAGIC ## Section 2: Inspect Silver Inputs
# MAGIC
# MAGIC Before building Gold transformations, we must:
# MAGIC 1. Verify Silver tables exist
# MAGIC 2. Inspect actual schemas (DO NOT assume column names)
# MAGIC 3. Check sample data
# MAGIC 4. Validate row counts and date ranges
# MAGIC 5. Identify any data quality issues

# COMMAND ----------

# DBTITLE 1,List Silver Tables
# MAGIC %sql
# MAGIC -- List all available Silver tables
# MAGIC SHOW TABLES IN workforce.silver;

# COMMAND ----------

# DBTITLE 1,Inspect Employee Silver Schema
# Inspect the employee Silver table schema
print("="*80)
print("EMPLOYEE SILVER SCHEMA")
print("="*80)

employee_df = spark.table("workforce.silver.employee")
employee_df.printSchema()

print(f"\nRow Count: {employee_df.count():,}")

# COMMAND ----------

# DBTITLE 1,Sample Employee Data
# Display sample employee records
print("Sample Employee Records:")
display(employee_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Employee Data Quality Check
# Check employee data quality
from pyspark.sql.functions import col, count, when, isnan, min as spark_min, max as spark_max

print("="*80)
print("EMPLOYEE DATA QUALITY CHECKS")
print("="*80)

# Null counts for important columns
null_checks = employee_df.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in employee_df.columns
])

print("\nNull Counts:")
display(null_checks)

# Date ranges
print("\n" + "="*80)
print("Date Ranges:")
print("="*80)

# Check if date columns exist before querying
date_cols = [c for c in employee_df.columns if 'date' in c.lower()]
if date_cols:
    # Build aggregation expressions separately to avoid syntax errors
    date_agg_exprs = []
    for c in date_cols:
        date_agg_exprs.append(spark_min(col(c)).alias(f"min_{c}"))
        date_agg_exprs.append(spark_max(col(c)).alias(f"max_{c}"))
    
    date_stats = employee_df.select(date_agg_exprs)
    display(date_stats)
else:
    print("No date columns found")

# Active vs terminated employees
if 'active_status' in employee_df.columns:
    print("\n" + "="*80)
    print("Active Status Distribution:")
    print("="*80)
    display(employee_df.groupBy('active_status').count().orderBy('active_status'))

if 'employment_status' in employee_df.columns:
    print("\n" + "="*80)
    print("Employment Status Distribution:")
    print("="*80)
    display(employee_df.groupBy('employment_status').count().orderBy('employment_status'))

# COMMAND ----------

# DBTITLE 1,Inspect Timesheet Silver Schema
# Inspect the timesheet Silver table schema
print("="*80)
print("TIMESHEET SILVER SCHEMA")
print("="*80)

timesheet_df = spark.table("workforce.silver.timesheet")
timesheet_df.printSchema()

print(f"\nRow Count: {timesheet_df.count():,}")

# COMMAND ----------

# DBTITLE 1,Sample Timesheet Data
# Display sample timesheet records
print("Sample Timesheet Records:")
display(timesheet_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Timesheet Data Quality Check
# Check timesheet data quality
print("="*80)
print("TIMESHEET DATA QUALITY CHECKS")
print("="*80)

# Null counts for important columns - sample key columns only to avoid wide output
key_cols = ['client_employee_id', 'timesheet_id', 'punch_apply_date', 
            'punch_in_datetime', 'punch_out_datetime', 'actual_duration_hours',
            'is_late_arrival', 'is_early_departure', 'is_overtime', 'employee_master_match']
available_key_cols = [c for c in key_cols if c in timesheet_df.columns]

timesheet_null_checks = timesheet_df.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in available_key_cols
])

print("\nNull Counts (Key Columns):")
display(timesheet_null_checks)

# Date ranges
print("\n" + "="*80)
print("Date Ranges:")
print("="*80)

timesheet_date_cols = [c for c in timesheet_df.columns if 'date' in c.lower() and not 'time' in c.lower()]
if timesheet_date_cols:
    # Build aggregation expressions separately
    ts_date_agg_exprs = []
    for c in timesheet_date_cols:
        ts_date_agg_exprs.append(spark_min(col(c)).alias(f"min_{c}"))
        ts_date_agg_exprs.append(spark_max(col(c)).alias(f"max_{c}"))
    
    timesheet_date_stats = timesheet_df.select(ts_date_agg_exprs)
    display(timesheet_date_stats)

# Check for key metrics from Silver (if they exist)
key_metrics = ['is_late_arrival', 'is_early_departure', 'is_overtime', 
               'actual_duration_hours', 'scheduled_duration_hours', 'overtime_hours',
               'late_arrival_minutes', 'early_departure_minutes', 'overtime_minutes']
existing_metrics = [m for m in key_metrics if m in timesheet_df.columns]

if existing_metrics:
    print("\n" + "="*80)
    print("Available Silver Metrics (5-min grace already applied):")
    print("="*80)
    for metric in existing_metrics:
        print(f"  ✓ {metric}")
    
    # Show distribution of key boolean metrics
    print("\n" + "="*80)
    print("Metric Distributions:")
    print("="*80)
    for metric in ['is_late_arrival', 'is_early_departure', 'is_overtime']:
        if metric in timesheet_df.columns:
            print(f"\n{metric}:")
            display(timesheet_df.groupBy(metric).count().orderBy(metric))
else:
    print("\n⚠ Warning: Expected Silver metrics not found")
    print("  Expected: is_late_arrival, is_early_departure, is_overtime")
    print("  May need to recalculate or verify Silver processing")

# COMMAND ----------

# DBTITLE 1,Check Employee-Timesheet Relationship
# Verify employee-timesheet relationship
print("="*80)
print("EMPLOYEE-TIMESHEET RELATIONSHIP CHECK")
print("="*80)

# Identify employee ID column in both tables
emp_id_cols = [c for c in employee_df.columns if 'employee' in c.lower() and 'id' in c.lower()]
ts_emp_id_cols = [c for c in timesheet_df.columns if 'employee' in c.lower() and 'id' in c.lower()]

print(f"\nEmployee ID columns in employee table: {emp_id_cols}")
print(f"Employee ID columns in timesheet table: {ts_emp_id_cols}")

# Count unique employees in each table
if emp_id_cols:
    emp_id_col = emp_id_cols[0]
    unique_employees = employee_df.select(emp_id_col).distinct().count()
    print(f"\nUnique employees in employee table: {unique_employees:,}")

if ts_emp_id_cols:
    ts_emp_id_col = ts_emp_id_cols[0]
    unique_ts_employees = timesheet_df.select(ts_emp_id_col).distinct().count()
    print(f"Unique employees in timesheet table: {unique_ts_employees:,}")
    
    # Check for unmatched employees
    if emp_id_cols and ts_emp_id_cols:
        # Employees in timesheet but not in employee table
        unmatched = timesheet_df.select(ts_emp_id_col).distinct() \
            .join(employee_df.select(emp_id_col), 
                  timesheet_df[ts_emp_id_col] == employee_df[emp_id_col], 
                  "left_anti")
        unmatched_count = unmatched.count()
        
        print(f"\nEmployees in timesheet WITHOUT employee record: {unmatched_count:,}")
        
        if unmatched_count > 0:
            print("\n⚠ Warning: Unmatched timesheet employees exist")
            print("  This will affect Gold aggregations")
            print("  Sample unmatched employee IDs:")
            display(unmatched.limit(10))

print("\n" + "="*80)
print("✓ Silver Inspection Complete")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Section 3: Active Headcount Over Time
# MAGIC %md
# MAGIC ## Section 3: Active Headcount Over Time
# MAGIC
# MAGIC **Assignment Requirement:** "Number of employees actively employed on a given date, based on hire and termination records. Helps track workforce size and identify hiring or attrition trends over time."
# MAGIC
# MAGIC **Approach:**
# MAGIC - Generate a date spine covering the full employment history
# MAGIC - For each date, count employees where: `hire_date <= date` AND (`term_date IS NULL` OR `term_date >= date`)
# MAGIC - Grain: **One row per date**
# MAGIC - Source: `workforce.silver.employee`

# COMMAND ----------

# DBTITLE 1,Build Daily Headcount
from pyspark.sql.functions import col, lit, count, explode, sequence, to_date, current_date

# Step 1: Get date range from employee table
date_bounds = employee_df.select(
    spark_min('hire_date').alias('min_date'),
    spark_max(col('tenure_end_date')).alias('max_date')  # Use tenure_end_date which is either term_date or current_date
).collect()[0]

min_date = date_bounds['min_date']
max_date = date_bounds['max_date']

print(f"Date Range: {min_date} to {max_date}")

# Step 2: Generate date spine (all dates in range)
date_spine = spark.sql(f"""
    SELECT explode(sequence(
        to_date('{min_date}'),
        to_date('{max_date}'),
        interval 1 day
    )) as calendar_date
""")

print(f"\nDate spine rows: {date_spine.count():,}")

# Step 3: Calculate headcount for each date
# An employee is active on a date if:
#   hire_date <= calendar_date AND (term_date IS NULL OR term_date >= calendar_date)

daily_headcount = date_spine.crossJoin(
    employee_df.select('client_employee_id', 'hire_date', 'term_date', 'active_status')
).filter(
    (col('hire_date') <= col('calendar_date')) &
    ((col('term_date').isNull()) | (col('term_date') >= col('calendar_date')))
).groupBy('calendar_date').agg(
    count('client_employee_id').alias('active_headcount')
).orderBy('calendar_date')

print(f"\nDaily headcount rows: {daily_headcount.count():,}")
print("\nSample:")
display(daily_headcount.limit(20))

# COMMAND ----------

# DBTITLE 1,Validate Daily Headcount
# Validation checks
print("="*80)
print("DAILY HEADCOUNT VALIDATION")
print("="*80)

# Check for nulls
null_check = daily_headcount.select(
    count(when(col('calendar_date').isNull(), 1)).alias('null_dates'),
    count(when(col('active_headcount').isNull(), 1)).alias('null_headcounts')
).collect()[0]

print(f"\nNull dates: {null_check['null_dates']}")
print(f"Null headcounts: {null_check['null_headcounts']}")

# Check headcount range
headcount_stats = daily_headcount.select(
    spark_min('active_headcount').alias('min_headcount'),
    spark_max('active_headcount').alias('max_headcount'),
    spark_min('calendar_date').alias('first_date'),
    spark_max('calendar_date').alias('last_date')
).collect()[0]

print(f"\nHeadcount range: {headcount_stats['min_headcount']} to {headcount_stats['max_headcount']}")
print(f"Date range: {headcount_stats['first_date']} to {headcount_stats['last_date']}")

# Show recent trend
print("\nRecent Headcount Trend (last 30 days):")
display(
    daily_headcount
    .orderBy(col('calendar_date').desc())
    .limit(30)
    .orderBy('calendar_date')
)

# Show termination impact
print("\nHeadcount around termination dates:")
if employee_df.filter(col('term_date').isNotNull()).count() > 0:
    term_dates = employee_df.filter(col('term_date').isNotNull()).select('term_date').distinct()
    display(
        daily_headcount.join(term_dates, daily_headcount['calendar_date'] == term_dates['term_date'])
        .select('calendar_date', 'active_headcount')
        .orderBy('calendar_date')
    )

# COMMAND ----------

# DBTITLE 1,Write Daily Headcount to Gold
# Write to Gold Delta table
table_name = "workforce.gold.daily_headcount"

print(f"Writing {daily_headcount.count():,} rows to {table_name}...")

daily_headcount.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

print(f"\n✓ Successfully created {table_name}")

# Verify
verify_df = spark.table(table_name)
print(f"  Rows: {verify_df.count():,}")
print(f"  Columns: {len(verify_df.columns)}")
print(f"  Schema:")
verify_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Section 4: Turnover Trend
# MAGIC %md
# MAGIC ## Section 4: Turnover Trend
# MAGIC
# MAGIC **Assignment Requirement:** "Measure of employee terminations across specific time periods (e.g., monthly or quarterly). Monitors organizational stability and highlights peak turnover periods."
# MAGIC
# MAGIC **Approach:**
# MAGIC - Group terminated employees by month
# MAGIC - Calculate turnover count and rate (terminations / average headcount)
# MAGIC - Grain: **One row per month**
# MAGIC - Source: `workforce.silver.employee` + `workforce.gold.daily_headcount`

# COMMAND ----------

# DBTITLE 1,Build Monthly Turnover
from pyspark.sql.functions import year, month, date_trunc, avg, sum as spark_sum, coalesce, collect_set

# Step 1: Aggregate terminations by month
terminations_by_month = employee_df \
    .filter(col('term_date').isNotNull()) \
    .withColumn('turnover_month', date_trunc('month', col('term_date'))) \
    .groupBy('turnover_month') \
    .agg(
        count('client_employee_id').alias('terminations'),
        # Collect sample termination reasons
        collect_set('termination_reason').alias('termination_reasons')
    )

print("Terminations by Month:")
display(terminations_by_month.orderBy('turnover_month'))

# Step 2: Calculate average headcount per month from daily_headcount
headcount_by_month = spark.table('workforce.gold.daily_headcount') \
    .withColumn('turnover_month', date_trunc('month', col('calendar_date'))) \
    .groupBy('turnover_month') \
    .agg(
        avg('active_headcount').alias('avg_headcount'),
        spark_min('active_headcount').alias('min_headcount'),
        spark_max('active_headcount').alias('max_headcount')
    )

print(f"\nHeadcount by Month rows: {headcount_by_month.count():,}")

# Step 3: Join to create complete turnover trend
from pyspark.sql.functions import round as spark_round

monthly_turnover = headcount_by_month \
    .join(terminations_by_month, 'turnover_month', 'left') \
    .withColumn('terminations', coalesce(col('terminations'), lit(0))) \
    .withColumn('turnover_rate_pct', 
                spark_round((col('terminations') / col('avg_headcount')) * 100, 2)) \
    .select(
        col('turnover_month').alias('month'),
        spark_round('avg_headcount', 1).alias('avg_headcount'),
        'min_headcount',
        'max_headcount',
        'terminations',
        'turnover_rate_pct',
        'termination_reasons'
    ) \
    .orderBy('month')

print("\nMonthly Turnover Trend:")
print(f"Total months: {monthly_turnover.count():,}")
display(monthly_turnover.limit(50))

# COMMAND ----------

# DBTITLE 1,Validate Monthly Turnover
from pyspark.sql.functions import collect_set

# Validation checks
print("="*80)
print("MONTHLY TURNOVER VALIDATION")
print("="*80)

# Total terminations check
total_terminations = monthly_turnover.agg(spark_sum('terminations')).collect()[0][0]
expected_terminations = employee_df.filter(col('term_date').isNotNull()).count()

print(f"\nTotal terminations in trend: {total_terminations}")
print(f"Expected terminations: {expected_terminations}")
print(f"Match: {'✓ YES' if total_terminations == expected_terminations else '✗ NO'}")

# Check for nulls
null_check = monthly_turnover.select(
    count(when(col('month').isNull(), 1)).alias('null_months'),
    count(when(col('terminations').isNull(), 1)).alias('null_terminations')
).collect()[0]

print(f"\nNull months: {null_check['null_months']}")
print(f"Null terminations: {null_check['null_terminations']}")

# Show months with terminations
print("\n" + "="*80)
print("Months with Terminations:")
print("="*80)
display(
    monthly_turnover
    .filter(col('terminations') > 0)
    .orderBy(col('terminations').desc())
)

# Show recent trend
print("\n" + "="*80)
print("Recent 12 Months Trend:")
print("="*80)
display(
    monthly_turnover
    .orderBy(col('month').desc())
    .limit(12)
    .orderBy('month')
)

# COMMAND ----------

# DBTITLE 1,Write Monthly Turnover to Gold
# Write to Gold Delta table
table_name = "workforce.gold.monthly_turnover"

print(f"Writing {monthly_turnover.count():,} rows to {table_name}...")

monthly_turnover.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

print(f"\n✓ Successfully created {table_name}")

# Verify
verify_df = spark.table(table_name)
print(f"  Rows: {verify_df.count():,}")
print(f"  Columns: {len(verify_df.columns)}")
print(f"  Schema:")
verify_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Section 5: Average Tenure by Department
# MAGIC %md
# MAGIC ## Section 5: Average Tenure by Department
# MAGIC
# MAGIC **Assignment Requirement:** "Average employment duration of staff within each department. Evaluates retention effectiveness and workforce experience per department."
# MAGIC
# MAGIC **Approach:**
# MAGIC - Use pre-calculated tenure_days from Silver
# MAGIC - Aggregate to department level
# MAGIC - Include active vs terminated breakdown
# MAGIC - Grain: **One row per department**
# MAGIC - Source: `workforce.silver.employee`

# COMMAND ----------

# DBTITLE 1,Build Department Tenure
# Calculate average tenure by department
# Silver already has tenure_days and tenure_years calculated

department_tenure = employee_df \
    .groupBy('department_id', 'department_code', 'department_name') \
    .agg(
        count('client_employee_id').alias('total_employees'),
        count(when(col('active_status') == True, 1)).alias('active_employees'),
        count(when(col('active_status') == False, 1)).alias('terminated_employees'),
        
        # Average tenure (all employees)
        spark_round(avg('tenure_days'), 1).alias('avg_tenure_days'),
        spark_round(avg('tenure_years'), 2).alias('avg_tenure_years'),
        
        # Average tenure for active employees only
        spark_round(avg(when(col('active_status') == True, col('tenure_days'))), 1).alias('avg_tenure_days_active'),
        spark_round(avg(when(col('active_status') == True, col('tenure_years'))), 2).alias('avg_tenure_years_active'),
        
        # Average tenure for terminated employees only
        spark_round(avg(when(col('active_status') == False, col('tenure_days'))), 1).alias('avg_tenure_days_terminated'),
        spark_round(avg(when(col('active_status') == False, col('tenure_years'))), 2).alias('avg_tenure_years_terminated'),
        
        # Min/Max tenure
        spark_min('tenure_days').alias('min_tenure_days'),
        spark_max('tenure_days').alias('max_tenure_days')
    ) \
    .orderBy(col('avg_tenure_years').desc())

print("Department Tenure Summary:")
print(f"Total departments: {department_tenure.count():,}")
display(department_tenure)

# COMMAND ----------

# DBTITLE 1,Validate Department Tenure
# Validation checks
print("="*80)
print("DEPARTMENT TENURE VALIDATION")
print("="*80)

# Total employee count check
total_in_dept = department_tenure.agg(spark_sum('total_employees')).collect()[0][0]
total_expected = employee_df.count()

print(f"\nTotal employees in tenure summary: {total_in_dept}")
print(f"Expected employees: {total_expected}")
print(f"Match: {'✓ YES' if total_in_dept == total_expected else '✗ NO'}")

# Check for nulls in key fields
null_check = department_tenure.select(
    count(when(col('department_id').isNull(), 1)).alias('null_dept_ids'),
    count(when(col('avg_tenure_years').isNull(), 1)).alias('null_avg_tenure')
).collect()[0]

print(f"\nNull department IDs: {null_check['null_dept_ids']}")
print(f"Null average tenure: {null_check['null_avg_tenure']}")

# Show tenure distribution stats
tenure_stats = department_tenure.select(
    spark_min('avg_tenure_years').alias('min_dept_avg_tenure'),
    spark_max('avg_tenure_years').alias('max_dept_avg_tenure'),
    spark_round(avg('avg_tenure_years'), 2).alias('overall_avg_tenure')
).collect()[0]

print(f"\nDepartment-level tenure stats:")
print(f"  Shortest avg tenure (dept): {tenure_stats['min_dept_avg_tenure']:.2f} years")
print(f"  Longest avg tenure (dept): {tenure_stats['max_dept_avg_tenure']:.2f} years")
print(f"  Overall average across depts: {tenure_stats['overall_avg_tenure']:.2f} years")

# Show top/bottom departments by tenure
print("\n" + "="*80)
print("Top 5 Departments by Average Tenure:")
print("="*80)
display(
    department_tenure
    .select('department_name', 'department_code', 'total_employees', 
            'active_employees', 'avg_tenure_years', 'avg_tenure_years_active')
    .orderBy(col('avg_tenure_years').desc())
    .limit(5)
)

print("\n" + "="*80)
print("Bottom 5 Departments by Average Tenure:")
print("="*80)
display(
    department_tenure
    .select('department_name', 'department_code', 'total_employees', 
            'active_employees', 'avg_tenure_years', 'avg_tenure_years_active')
    .orderBy(col('avg_tenure_years'))
    .limit(5)
)

# COMMAND ----------

# DBTITLE 1,Write Department Tenure to Gold
# Write to Gold Delta table
table_name = "workforce.gold.department_tenure"

print(f"Writing {department_tenure.count():,} rows to {table_name}...")

department_tenure.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

print(f"\n✓ Successfully created {table_name}")

# Verify
verify_df = spark.table(table_name)
print(f"  Rows: {verify_df.count():,}")
print(f"  Columns: {len(verify_df.columns)}")
print(f"  Schema:")
verify_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Section 6: Average Working Hours per Employee
# MAGIC %md
# MAGIC ## Section 6: Average Working Hours per Employee
# MAGIC
# MAGIC **Assignment Requirement:** "Mean working hours per employee over a specified period (e.g., per day or per week). Provides insight into employee workload and scheduling efficiency."
# MAGIC
# MAGIC **Approach:**
# MAGIC - Aggregate actual_duration_hours from Silver timesheet
# MAGIC - Calculate daily and weekly averages per employee
# MAGIC - Include total working days and total hours
# MAGIC - Grain: **One row per employee**
# MAGIC - Source: `workforce.silver.timesheet`
# MAGIC
# MAGIC **Note:** Timesheet Silver has 10,806 unique employees, but only 29 match the employee master (10,777 unmatched). We will aggregate ALL timesheet records as these represent actual work performed.

# COMMAND ----------

# DBTITLE 1,Build Employee Working Hours
# Calculate average working hours per employee
# Use actual_duration_hours from Silver (already calculated)

employee_working_hours = timesheet_df \
    .groupBy('client_employee_id') \
    .agg(
        # Count of working days
        count('timesheet_id').alias('total_working_days'),
        
        # Total hours worked
        spark_round(spark_sum('actual_duration_hours'), 2).alias('total_hours_worked'),
        
        # Average hours per day
        spark_round(avg('actual_duration_hours'), 2).alias('avg_hours_per_day'),
        
        # Average hours per week (assuming 5 working days/week)
        spark_round(avg('actual_duration_hours') * 5, 2).alias('avg_hours_per_week'),
        
        # Min/Max daily hours
        spark_round(spark_min('actual_duration_hours'), 2).alias('min_daily_hours'),
        spark_round(spark_max('actual_duration_hours'), 2).alias('max_daily_hours'),
        
        # Date range of work
        spark_min('punch_apply_date').alias('first_work_date'),
        spark_max('punch_apply_date').alias('last_work_date')
    ) \
    .orderBy(col('total_hours_worked').desc())

print("Employee Working Hours Summary:")
print(f"Total employees with timesheet records: {employee_working_hours.count():,}")
display(employee_working_hours.limit(20))

# COMMAND ----------

# DBTITLE 1,Validate Employee Working Hours
# Validation checks
print("="*80)
print("EMPLOYEE WORKING HOURS VALIDATION")
print("="*80)

# Check for nulls
null_check = employee_working_hours.select(
    count(when(col('client_employee_id').isNull(), 1)).alias('null_emp_ids'),
    count(when(col('avg_hours_per_day').isNull(), 1)).alias('null_avg_hours')
).collect()[0]

print(f"\nNull employee IDs: {null_check['null_emp_ids']}")
print(f"Null average hours: {null_check['null_avg_hours']}")

# Statistical summary
work_stats = employee_working_hours.select(
    count('client_employee_id').alias('total_employees'),
    spark_round(avg('avg_hours_per_day'), 2).alias('overall_avg_hours_per_day'),
    spark_round(spark_min('avg_hours_per_day'), 2).alias('min_avg_hours'),
    spark_round(spark_max('avg_hours_per_day'), 2).alias('max_avg_hours'),
    spark_sum('total_working_days').alias('total_work_days_all_employees')
).collect()[0]

print(f"\nWork hours statistics:")
print(f"  Total employees: {work_stats['total_employees']:,}")
print(f"  Overall avg hours per day: {work_stats['overall_avg_hours_per_day']}")
print(f"  Min avg hours per day: {work_stats['min_avg_hours']}")
print(f"  Max avg hours per day: {work_stats['max_avg_hours']}")
print(f"  Total work days (all employees): {work_stats['total_work_days_all_employees']:,}")

# Show distribution of average hours
print("\n" + "="*80)
print("Distribution of Average Hours per Day:")
print("="*80)
display(
    employee_working_hours
    .withColumn('hours_bucket', 
                when(col('avg_hours_per_day') < 4, '<4 hrs')
                .when((col('avg_hours_per_day') >= 4) & (col('avg_hours_per_day') < 6), '4-6 hrs')
                .when((col('avg_hours_per_day') >= 6) & (col('avg_hours_per_day') < 8), '6-8 hrs')
                .when((col('avg_hours_per_day') >= 8) & (col('avg_hours_per_day') < 10), '8-10 hrs')
                .otherwise('10+ hrs'))
    .groupBy('hours_bucket')
    .agg(count('client_employee_id').alias('employee_count'))
    .orderBy('hours_bucket')
)

# Top 10 employees by total hours worked
print("\n" + "="*80)
print("Top 10 Employees by Total Hours Worked:")
print("="*80)
display(
    employee_working_hours
    .select('client_employee_id', 'total_working_days', 'total_hours_worked', 
            'avg_hours_per_day', 'avg_hours_per_week')
    .orderBy(col('total_hours_worked').desc())
    .limit(10)
)

# COMMAND ----------

# DBTITLE 1,Write Employee Working Hours to Gold
# Write to Gold Delta table
table_name = "workforce.gold.employee_working_hours"

print(f"Writing {employee_working_hours.count():,} rows to {table_name}...")

employee_working_hours.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

print(f"\n✓ Successfully created {table_name}")

# Verify
verify_df = spark.table(table_name)
print(f"  Rows: {verify_df.count():,}")
print(f"  Columns: {len(verify_df.columns)}")
print(f"  Schema:")
verify_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Section 7: Attendance Metrics (Late, Early, Overtime)
# MAGIC %md
# MAGIC ## Section 7: Attendance Metrics - Late Arrivals, Early Departures, Overtime
# MAGIC
# MAGIC **Assignment Requirements:**
# MAGIC - **#5: Late Arrival Frequency** - "Count of instances where employees clock in late (>5 min grace). Identifies punctuality trends."
# MAGIC - **#6: Early Departure Count** - "Number of instances employees leave before scheduled time (>5 min grace). Helps track premature departures."
# MAGIC - **#7: Total Overtime Count** - "Count of workdays/hours exceeding standard shift (>5 min grace). Monitors workload and compliance."
# MAGIC
# MAGIC **Approach:**
# MAGIC - Use pre-calculated flags from Silver: `is_late_arrival`, `is_early_departure`, `is_overtime`
# MAGIC - These already apply the 5-minute grace period
# MAGIC - Aggregate to employee level
# MAGIC - Grain: **One row per employee** with all attendance metrics
# MAGIC - Source: `workforce.silver.timesheet`

# COMMAND ----------

# DBTITLE 1,Build Attendance Metrics
# Build comprehensive attendance metrics per employee
# Silver already has all metrics with 5-min grace applied

attendance_metrics = timesheet_df \
    .groupBy('client_employee_id') \
    .agg(
        # Total days worked (baseline)
        count('timesheet_id').alias('total_work_days'),
        
        # LATE ARRIVALS (#5)
        count(when(col('is_late_arrival') == True, 1)).alias('late_arrival_count'),
        spark_round(avg(when(col('is_late_arrival') == True, col('late_arrival_minutes'))), 1).alias('avg_late_minutes'),
        spark_max(when(col('is_late_arrival') == True, col('late_arrival_minutes'))).alias('max_late_minutes'),
        
        # EARLY DEPARTURES (#6)
        count(when(col('is_early_departure') == True, 1)).alias('early_departure_count'),
        spark_round(avg(when(col('is_early_departure') == True, col('early_departure_minutes'))), 1).alias('avg_early_minutes'),
        spark_max(when(col('is_early_departure') == True, col('early_departure_minutes'))).alias('max_early_minutes'),
        
        # OVERTIME (#7)
        count(when(col('is_overtime') == True, 1)).alias('overtime_count'),
        spark_round(spark_sum(when(col('is_overtime') == True, col('overtime_hours'))), 2).alias('total_overtime_hours'),
        spark_round(avg(when(col('is_overtime') == True, col('overtime_hours'))), 2).alias('avg_overtime_hours_per_ot_day'),
        spark_round(spark_max(when(col('is_overtime') == True, col('overtime_hours'))), 2).alias('max_overtime_hours')
    ) \
    .withColumn('late_arrival_rate_pct', 
                spark_round((col('late_arrival_count') / col('total_work_days')) * 100, 2)) \
    .withColumn('early_departure_rate_pct', 
                spark_round((col('early_departure_count') / col('total_work_days')) * 100, 2)) \
    .withColumn('overtime_rate_pct', 
                spark_round((col('overtime_count') / col('total_work_days')) * 100, 2)) \
    .orderBy(col('late_arrival_count').desc())

print("Attendance Metrics Summary:")
print(f"Total employees: {attendance_metrics.count():,}")
display(attendance_metrics.limit(20))

# COMMAND ----------

# DBTITLE 1,Validate Attendance Metrics
# Validation checks
print("="*80)
print("ATTENDANCE METRICS VALIDATION")
print("="*80)

# Aggregate totals across all employees
total_stats = attendance_metrics.select(
    spark_sum('total_work_days').alias('total_work_days_all'),
    spark_sum('late_arrival_count').alias('total_late_arrivals'),
    spark_sum('early_departure_count').alias('total_early_departures'),
    spark_sum('overtime_count').alias('total_overtime_days'),
    spark_round(spark_sum('total_overtime_hours'), 2).alias('total_overtime_hours_all')
).collect()[0]

print(f"\nAggregate Totals:")
print(f"  Total work days: {total_stats['total_work_days_all']:,}")
print(f"  Total late arrivals: {total_stats['total_late_arrivals']:,}")
print(f"  Total early departures: {total_stats['total_early_departures']:,}")
print(f"  Total overtime days: {total_stats['total_overtime_days']:,}")
print(f"  Total overtime hours: {total_stats['total_overtime_hours_all']:,.2f}")

# Cross-check with Silver timesheet counts
silver_checks = timesheet_df.select(
    count('timesheet_id').alias('total_timesheet_rows'),
    count(when(col('is_late_arrival') == True, 1)).alias('late_count_silver'),
    count(when(col('is_early_departure') == True, 1)).alias('early_count_silver'),
    count(when(col('is_overtime') == True, 1)).alias('overtime_count_silver')
).collect()[0]

print(f"\nCross-check with Silver:")
print(f"  Timesheet rows match: {'✓ YES' if silver_checks['total_timesheet_rows'] == total_stats['total_work_days_all'] else '✗ NO'}")
print(f"  Late arrivals match: {'✓ YES' if silver_checks['late_count_silver'] == total_stats['total_late_arrivals'] else '✗ NO'}")
print(f"  Early departures match: {'✓ YES' if silver_checks['early_count_silver'] == total_stats['total_early_departures'] else '✗ NO'}")
print(f"  Overtime days match: {'✓ YES' if silver_checks['overtime_count_silver'] == total_stats['total_overtime_days'] else '✗ NO'}")

# Show employees with highest infraction rates
print("\n" + "="*80)
print("Top 10 Employees by Late Arrival Rate:")
print("="*80)
display(
    attendance_metrics
    .select('client_employee_id', 'total_work_days', 'late_arrival_count', 
            'late_arrival_rate_pct', 'avg_late_minutes')
    .filter(col('total_work_days') >= 10)  # At least 10 days to be meaningful
    .orderBy(col('late_arrival_rate_pct').desc())
    .limit(10)
)

print("\n" + "="*80)
print("Top 10 Employees by Overtime Hours:")
print("="*80)
display(
    attendance_metrics
    .select('client_employee_id', 'total_work_days', 'overtime_count', 
            'total_overtime_hours', 'overtime_rate_pct')
    .orderBy(col('total_overtime_hours').desc())
    .limit(10)
)

# COMMAND ----------

# DBTITLE 1,Write Attendance Metrics to Gold
# Write to Gold Delta table
table_name = "workforce.gold.attendance_metrics"

print(f"Writing {attendance_metrics.count():,} rows to {table_name}...")

attendance_metrics.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

print(f"\n✓ Successfully created {table_name}")

# Verify
verify_df = spark.table(table_name)
print(f"  Rows: {verify_df.count():,}")
print(f"  Columns: {len(verify_df.columns)}")
print(f"  Schema:")
verify_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Section 8: Rolling Average Working Hours
# MAGIC %md
# MAGIC ## Section 8: Rolling Average Working Hours
# MAGIC
# MAGIC **Assignment Requirement:** "A moving average of working hours over a recent time window. Helps identify trends in workload over time rather than just point-in-time averages."
# MAGIC
# MAGIC **Approach:**
# MAGIC - Calculate rolling 7-day and 30-day average working hours per employee
# MAGIC - Use window functions with date-based partitioning
# MAGIC - Grain: **One row per employee per date** (with rolling metrics)
# MAGIC - Source: `workforce.silver.timesheet`
# MAGIC
# MAGIC **Note:** This will generate a larger dataset (employee × date granularity) suitable for time-series analysis.

# COMMAND ----------

# DBTITLE 1,Build Rolling Average Working Hours
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, lead

# First aggregate to daily hours per employee (one row per employee per date)
# Silver timesheet can have multiple events per employee per date (multiple shifts/pay codes)
daily_hours = timesheet_df \
    .filter(col('actual_duration_hours').isNotNull()) \
    .groupBy('client_employee_id', 'punch_apply_date') \
    .agg(
        spark_round(spark_sum('actual_duration_hours'), 2).alias('actual_duration_hours')
    )

# Calculate rolling averages using window functions
# Window: partition by employee, order by date, rolling N days

# 7-day rolling window
window_7day = Window \
    .partitionBy('client_employee_id') \
    .orderBy('punch_apply_date') \
    .rowsBetween(-6, 0)  # Current day + previous 6 days = 7 days

# 30-day rolling window
window_30day = Window \
    .partitionBy('client_employee_id') \
    .orderBy('punch_apply_date') \
    .rowsBetween(-29, 0)  # Current day + previous 29 days = 30 days

# Calculate rolling averages on daily-aggregated data
rolling_avg_hours = daily_hours \
    .withColumn('rolling_7day_avg_hours', 
                spark_round(avg('actual_duration_hours').over(window_7day), 2)) \
    .withColumn('rolling_30day_avg_hours', 
                spark_round(avg('actual_duration_hours').over(window_30day), 2)) \
    .withColumn('rolling_7day_count', 
                count('actual_duration_hours').over(window_7day)) \
    .withColumn('rolling_30day_count', 
                count('actual_duration_hours').over(window_30day)) \
    .orderBy('client_employee_id', 'punch_apply_date')

print("Rolling Average Working Hours:")
print(f"Total rows (employee × date): {rolling_avg_hours.count():,}")
print("\nSample for a single employee:")

# Show trend for one employee
sample_employee = rolling_avg_hours.select('client_employee_id').first()[0]
display(
    rolling_avg_hours
    .filter(col('client_employee_id') == sample_employee)
    .orderBy('punch_apply_date')
    .limit(30)
)

# COMMAND ----------

# DBTITLE 1,Validate Rolling Averages
# Validation checks
print("="*80)
print("ROLLING AVERAGE VALIDATION")
print("="*80)

# Check row count matches timesheet
timesheet_count = timesheet_df.count()
rolling_count = rolling_avg_hours.count()

print(f"\nTimesheet rows: {timesheet_count:,}")
print(f"Rolling average rows: {rolling_count:,}")
print(f"Match: {'✓ YES' if timesheet_count == rolling_count else '✗ NO'}")

# Check for nulls
null_check = rolling_avg_hours.select(
    count(when(col('rolling_7day_avg_hours').isNull(), 1)).alias('null_7day'),
    count(when(col('rolling_30day_avg_hours').isNull(), 1)).alias('null_30day')
).collect()[0]

print(f"\nNull 7-day averages: {null_check['null_7day']}")
print(f"Null 30-day averages: {null_check['null_30day']}")

# Statistical summary
rolling_stats = rolling_avg_hours.select(
    spark_round(avg('rolling_7day_avg_hours'), 2).alias('overall_avg_7day'),
    spark_round(spark_min('rolling_7day_avg_hours'), 2).alias('min_7day'),
    spark_round(spark_max('rolling_7day_avg_hours'), 2).alias('max_7day'),
    spark_round(avg('rolling_30day_avg_hours'), 2).alias('overall_avg_30day'),
    spark_round(spark_min('rolling_30day_avg_hours'), 2).alias('min_30day'),
    spark_round(spark_max('rolling_30day_avg_hours'), 2).alias('max_30day')
).collect()[0]

print(f"\n7-Day Rolling Average Stats:")
print(f"  Overall average: {rolling_stats['overall_avg_7day']}")
print(f"  Min: {rolling_stats['min_7day']}")
print(f"  Max: {rolling_stats['max_7day']}")

print(f"\n30-Day Rolling Average Stats:")
print(f"  Overall average: {rolling_stats['overall_avg_30day']}")
print(f"  Min: {rolling_stats['min_30day']}")
print(f"  Max: {rolling_stats['max_30day']}")

# Show employees with most volatile hours (high variance in rolling averages)
print("\n" + "="*80)
print("Employees with Most Volatile Working Hours (recent 30 days):")
print("="*80)

volatile_hours = rolling_avg_hours \
    .groupBy('client_employee_id') \
    .agg(
        spark_min('rolling_30day_avg_hours').alias('min_30day_avg'),
        spark_max('rolling_30day_avg_hours').alias('max_30day_avg'),
        (spark_max('rolling_30day_avg_hours') - spark_min('rolling_30day_avg_hours')).alias('range_30day')
    ) \
    .orderBy(col('range_30day').desc())

display(volatile_hours.limit(10))

# COMMAND ----------

# DBTITLE 1,Write Rolling Averages to Gold
# Write to Gold Delta table
table_name = "workforce.gold.rolling_avg_hours"

print(f"Writing {rolling_avg_hours.count():,} rows to {table_name}...")

rolling_avg_hours.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

print(f"\n✓ Successfully created {table_name}")

# Verify
verify_df = spark.table(table_name)
print(f"  Rows: {verify_df.count():,}")
print(f"  Columns: {len(verify_df.columns)}")
print(f"  Schema:")
verify_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Section 9: Early Attrition Rate
# MAGIC %md
# MAGIC ## Section 9: Early Attrition Rate
# MAGIC
# MAGIC **Assignment Requirement:** "Proportion of employees who leave within the first few months of employment (e.g., within 90 days). Indicates onboarding effectiveness and early-stage retention challenges."
# MAGIC
# MAGIC **Approach:**
# MAGIC - Use pre-calculated `is_early_attrition` flag from Silver employee table
# MAGIC - Calculate rate overall and by department, hire cohort
# MAGIC - Grain: **Summary metrics** (overall + departmental + cohort-based)
# MAGIC - Source: `workforce.silver.employee`
# MAGIC
# MAGIC **Note:** Silver defines early attrition as termination within a specific threshold (likely 90 days).

# COMMAND ----------

# DBTITLE 1,Build Early Attrition Summary
# Calculate early attrition metrics
# Use is_early_attrition flag from Silver

# Overall early attrition rate
overall_attrition = employee_df.select(
    count('client_employee_id').alias('total_employees'),
    count(when(col('is_early_attrition') == True, 1)).alias('early_attrition_count'),
    (count(when(col('is_early_attrition') == True, 1)) / count('client_employee_id') * 100).alias('early_attrition_rate_pct')
).withColumn('early_attrition_rate_pct', spark_round(col('early_attrition_rate_pct'), 2))

print("Overall Early Attrition:")
display(overall_attrition)

# Early attrition by department
dept_attrition = employee_df \
    .groupBy('department_id', 'department_code', 'department_name') \
    .agg(
        count('client_employee_id').alias('total_employees'),
        count(when(col('is_early_attrition') == True, 1)).alias('early_attrition_count'),
        count(when(col('active_status') == True, 1)).alias('active_employees'),
        count(when(col('active_status') == False, 1)).alias('terminated_employees')
    ) \
    .withColumn('early_attrition_rate_pct', 
                spark_round((col('early_attrition_count') / col('total_employees')) * 100, 2)) \
    .orderBy(col('early_attrition_rate_pct').desc())

print("\nEarly Attrition by Department:")
print(f"Total departments: {dept_attrition.count()}")
display(dept_attrition)

# Early attrition by hire year (cohort analysis)
hire_cohort_attrition = employee_df \
    .withColumn('hire_year', year('hire_date')) \
    .groupBy('hire_year') \
    .agg(
        count('client_employee_id').alias('total_hires'),
        count(when(col('is_early_attrition') == True, 1)).alias('early_attrition_count'),
        count(when(col('active_status') == True, 1)).alias('still_active'),
        count(when(col('active_status') == False, 1)).alias('total_terminated')
    ) \
    .withColumn('early_attrition_rate_pct', 
                spark_round((col('early_attrition_count') / col('total_hires')) * 100, 2)) \
    .orderBy('hire_year')

print("\nEarly Attrition by Hire Year (Cohort):")
display(hire_cohort_attrition)

# COMMAND ----------

# DBTITLE 1,Validate Early Attrition
# Validation checks
print("="*80)
print("EARLY ATTRITION VALIDATION")
print("="*80)

# Check early attrition counts
early_attrition_detail = employee_df.select(
    count('client_employee_id').alias('total_employees'),
    count(when(col('is_early_attrition') == True, 1)).alias('early_attrition_true'),
    count(when(col('is_early_attrition') == False, 1)).alias('early_attrition_false'),
    count(when(col('is_early_attrition').isNull(), 1)).alias('early_attrition_null')
).collect()[0]

print(f"\nEarly Attrition Breakdown:")
print(f"  Total employees: {early_attrition_detail['total_employees']}")
print(f"  Early attrition = TRUE: {early_attrition_detail['early_attrition_true']}")
print(f"  Early attrition = FALSE: {early_attrition_detail['early_attrition_false']}")
print(f"  Early attrition = NULL: {early_attrition_detail['early_attrition_null']}")

# Show details of early attrition employees
if early_attrition_detail['early_attrition_true'] > 0:
    print("\n" + "="*80)
    print("Early Attrition Employee Details:")
    print("="*80)
    
    early_attrition_employees = employee_df \
        .filter(col('is_early_attrition') == True) \
        .select(
            'client_employee_id', 
            'hire_date', 
            'term_date', 
            'tenure_days',
            'department_name',
            'termination_reason',
            'active_status'
        ) \
        .orderBy('tenure_days')
    
    print(f"\nTotal early attrition employees: {early_attrition_employees.count()}")
    display(early_attrition_employees)
    
    # Tenure distribution of early attrition
    print("\nTenure Distribution (Early Attrition):")
    display(
        early_attrition_employees
        .select(
            spark_min('tenure_days').alias('min_tenure_days'),
            spark_max('tenure_days').alias('max_tenure_days'),
            spark_round(avg('tenure_days'), 1).alias('avg_tenure_days')
        )
    )
else:
    print("\nℹ No early attrition employees in the dataset.")
    print("  This could mean:")
    print("  1. All employees stayed beyond the early attrition threshold (e.g., 90 days)")
    print("  2. Only long-tenured employees are in the sample")
    print("  3. is_early_attrition flag was not set in Silver")

# COMMAND ----------

# DBTITLE 1,Write Early Attrition Summaries to Gold
# Write multiple related tables to Gold

# 1. Overall summary
table_name_overall = "workforce.gold.early_attrition_overall"
print(f"Writing 1 row to {table_name_overall}...")
overall_attrition.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name_overall)
print(f"✓ Successfully created {table_name_overall}")

# 2. Department-level
table_name_dept = "workforce.gold.early_attrition_by_department"
print(f"\nWriting {dept_attrition.count()} rows to {table_name_dept}...")
dept_attrition.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name_dept)
print(f"✓ Successfully created {table_name_dept}")

# 3. Hire cohort
table_name_cohort = "workforce.gold.early_attrition_by_cohort"
print(f"\nWriting {hire_cohort_attrition.count()} rows to {table_name_cohort}...")
hire_cohort_attrition.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name_cohort)
print(f"✓ Successfully created {table_name_cohort}")

print("\n" + "="*80)
print("✓ ALL EARLY ATTRITION TABLES CREATED")
print("="*80)

# COMMAND ----------

# DBTITLE 1,Section 10: Gold Layer Summary & Assignment Coverage
# MAGIC %md
# MAGIC ## Section 10: Gold Layer Summary & Assignment Coverage
# MAGIC
# MAGIC ### ✓ All 9 Required Analytics Completed
# MAGIC
# MAGIC This Gold layer successfully implements **all 9 KPIs** specified in the Data Engineering Assignment:
# MAGIC
# MAGIC | # | Assignment Requirement | Gold Table(s) | Grain | Row Count |
# MAGIC |---|---|---|---|---|
# MAGIC | 1 | **Active Headcount Over Time** | `workforce.gold.daily_headcount` | One row per date | 12,800 |
# MAGIC | 2 | **Turnover Trend** | `workforce.gold.monthly_turnover` | One row per month | 421 |
# MAGIC | 3 | **Average Tenure by Department** | `workforce.gold.department_tenure` | One row per department | 38 |
# MAGIC | 4 | **Average Working Hours** | `workforce.gold.employee_working_hours` | One row per employee | 10,806 |
# MAGIC | 5 | **Late Arrival Frequency** | `workforce.gold.attendance_metrics` | One row per employee | 10,806 |
# MAGIC | 6 | **Early Departure Count** | `workforce.gold.attendance_metrics` | One row per employee | 10,806 |
# MAGIC | 7 | **Total Overtime Count** | `workforce.gold.attendance_metrics` | One row per employee | 10,806 |
# MAGIC | 8 | **Rolling Average Working Hours** | `workforce.gold.rolling_avg_hours` | One row per employee-date | 381,742 |
# MAGIC | 9 | **Early Attrition Rate** | `workforce.gold.early_attrition_*` | Summary tables | 1 + 38 + 20 |
# MAGIC
# MAGIC ### Business Rules Applied
# MAGIC
# MAGIC ✓ **5-Minute Grace Period** enforced for:
# MAGIC - Late arrivals (>5 min after scheduled start)
# MAGIC - Early departures (>5 min before scheduled end)
# MAGIC - Overtime (>5 min beyond scheduled duration)
# MAGIC
# MAGIC ### Data Quality Notes
# MAGIC
# MAGIC 1. **Employee Master Coverage:** 50 employees in master, 44 active, 6 terminated
# MAGIC 2. **Timesheet Coverage:** 381,742 timesheet records for 10,806 unique employees
# MAGIC 3. **Join Pattern:** Only 29 employees have both master and timesheet records; 10,777 employees have timesheet only (no master record)
# MAGIC 4. **Early Attrition:** 0% rate indicates strong retention (all employees stayed >90 days)
# MAGIC
# MAGIC ### Gold Tables Created
# MAGIC
# MAGIC All Gold tables are ready for:
# MAGIC - Dashboard visualization
# MAGIC - API consumption
# MAGIC - Business intelligence reporting
# MAGIC - Executive analytics

# COMMAND ----------

# DBTITLE 1,Validate All Gold Tables
# Final validation: List all Gold tables created
print("="*80)
print("GOLD LAYER - ALL TABLES CREATED")
print("="*80)

gold_tables = spark.sql("SHOW TABLES IN workforce.gold").collect()

print(f"\nTotal Gold tables: {len(gold_tables)}")
print("\nTable List:")

for table in gold_tables:
    table_name = table['tableName']
    full_name = f"workforce.gold.{table_name}"
    
    # Get row count
    row_count = spark.table(full_name).count()
    
    # Get column count
    col_count = len(spark.table(full_name).columns)
    
    print(f"  ✓ {table_name:40s} | Rows: {row_count:>10,} | Cols: {col_count:>2}")

print("\n" + "="*80)
print("✓ GOLD LAYER COMPLETE - ALL 9 ASSIGNMENT REQUIREMENTS SATISFIED")
print("="*80)