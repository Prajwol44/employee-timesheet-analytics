-- Databricks notebook source
-- DBTITLE 1,SQL Analytics Notebook
-- MAGIC %md
-- MAGIC # Employee Timesheet Analytics - SQL Notebook
-- MAGIC
-- MAGIC Simple SQL analytics demonstrating key workforce and attendance metrics using existing Silver and Gold tables.

-- COMMAND ----------

-- DBTITLE 1,Verify Silver Tables
-- Verify Silver tables exist
SHOW TABLES IN workforce.silver

-- COMMAND ----------

-- DBTITLE 1,Verify Gold Tables
-- Verify Gold tables exist
SHOW TABLES IN workforce.gold

-- COMMAND ----------

-- DBTITLE 1,Employee Schema
-- Inspect Employee Silver table schema
DESCRIBE workforce.silver.employee

-- COMMAND ----------

-- DBTITLE 1,Timesheet Schema
-- Inspect Timesheet Silver table schema
DESCRIBE workforce.silver.timesheet

-- COMMAND ----------

-- DBTITLE 1,Create Temporary Views
-- Create simple temporary views for easier querying
CREATE OR REPLACE TEMPORARY VIEW employee AS
SELECT * FROM workforce.silver.employee;

CREATE OR REPLACE TEMPORARY VIEW timesheet AS
SELECT * FROM workforce.silver.timesheet;

CREATE OR REPLACE TEMPORARY VIEW employee_working_hours AS
SELECT * FROM workforce.gold.employee_working_hours;

CREATE OR REPLACE TEMPORARY VIEW attendance_metrics AS
SELECT * FROM workforce.gold.attendance_metrics;

CREATE OR REPLACE TEMPORARY VIEW rolling_avg_hours AS
SELECT * FROM workforce.gold.rolling_avg_hours;

SELECT 'Temporary views created successfully' AS status

-- COMMAND ----------

-- DBTITLE 1,Section: Active Headcount Over Time
-- MAGIC %md
-- MAGIC ## 1. Active Headcount Over Time
-- MAGIC
-- MAGIC Calculate the number of active employees for each date based on hire and termination dates.

-- COMMAND ----------

-- DBTITLE 1,Active Headcount by Date
-- Active headcount over time
WITH date_range AS (
  SELECT DISTINCT punch_apply_date AS calendar_date
  FROM timesheet
  WHERE punch_apply_date IS NOT NULL
  ORDER BY calendar_date
)
SELECT 
  calendar_date,
  COUNT(DISTINCT e.client_employee_id) AS active_headcount
FROM date_range d
INNER JOIN employee e
  ON d.calendar_date >= e.hire_date
  AND (e.term_date IS NULL OR d.calendar_date <= e.term_date)
GROUP BY calendar_date
ORDER BY calendar_date

-- COMMAND ----------

-- DBTITLE 1,Section: Turnover Trend
-- MAGIC %md
-- MAGIC ## 2. Turnover Trend
-- MAGIC
-- MAGIC Calculate monthly terminations.

-- COMMAND ----------

-- DBTITLE 1,Monthly Terminations
-- Monthly termination trend
SELECT 
  DATE_TRUNC('month', term_date) AS termination_month,
  COUNT(*) AS termination_count
FROM employee
WHERE term_date IS NOT NULL
GROUP BY termination_month
ORDER BY termination_month

-- COMMAND ----------

-- DBTITLE 1,Section: Average Tenure by Department
-- MAGIC %md
-- MAGIC ## 3. Average Tenure by Department
-- MAGIC
-- MAGIC Group employees by department and calculate average tenure.

-- COMMAND ----------

-- DBTITLE 1,Tenure by Department
-- Average tenure by department
SELECT 
  COALESCE(department_code, 'Unknown') AS department,
  COUNT(*) AS employee_count,
  ROUND(AVG(tenure_days) / 365.25, 2) AS avg_tenure_years
FROM employee
WHERE tenure_days IS NOT NULL
GROUP BY department_code
ORDER BY avg_tenure_years DESC

-- COMMAND ----------

-- DBTITLE 1,Section: Average Working Hours
-- MAGIC %md
-- MAGIC ## 4. Average Working Hours per Employee
-- MAGIC
-- MAGIC Use the existing Gold employee_working_hours table.

-- COMMAND ----------

-- DBTITLE 1,Top Employees by Working Hours
-- Employee-level average working hours
SELECT 
  client_employee_id,
  total_working_days,
  total_hours_worked,
  avg_hours_per_day
FROM employee_working_hours
WHERE total_working_days > 0
ORDER BY avg_hours_per_day DESC
LIMIT 20

-- COMMAND ----------

-- DBTITLE 1,Department Working Hours Summary
-- Department-level working hours summary
SELECT 
  e.department_code,
  COUNT(DISTINCT ewh.client_employee_id) AS employee_count,
  ROUND(AVG(ewh.avg_hours_per_day), 2) AS avg_hours_per_day,
  ROUND(SUM(ewh.total_hours_worked), 2) AS total_dept_hours
FROM employee_working_hours ewh
INNER JOIN employee e ON ewh.client_employee_id = e.client_employee_id
WHERE ewh.total_working_days > 0
GROUP BY e.department_code
ORDER BY avg_hours_per_day DESC

-- COMMAND ----------

-- DBTITLE 1,Section: Late Arrival Frequency
-- MAGIC %md
-- MAGIC ## 5. Late Arrival Frequency
-- MAGIC
-- MAGIC Use the existing is_late_arrival flag (5-minute grace period).

-- COMMAND ----------

-- DBTITLE 1,Late Arrivals by Employee
-- Late arrival frequency by employee
SELECT 
  t.client_employee_id,
  e.first_name,
  e.last_name,
  COUNT(*) AS late_arrival_count,
  ROUND(SUM(t.late_arrival_minutes) / 60.0, 2) AS total_late_hours
FROM timesheet t
INNER JOIN employee e ON t.client_employee_id = e.client_employee_id
WHERE t.is_late_arrival = TRUE
GROUP BY t.client_employee_id, e.first_name, e.last_name
ORDER BY late_arrival_count DESC
LIMIT 20

-- COMMAND ----------

-- DBTITLE 1,Late Arrivals by Department
-- Late arrival frequency by department
SELECT 
  e.department_code,
  COUNT(*) AS late_arrival_count,
  COUNT(DISTINCT t.client_employee_id) AS employees_with_late_arrivals
FROM timesheet t
INNER JOIN employee e ON t.client_employee_id = e.client_employee_id
WHERE t.is_late_arrival = TRUE
GROUP BY e.department_code
ORDER BY late_arrival_count DESC

-- COMMAND ----------

-- DBTITLE 1,Section: Early Departure Count
-- MAGIC %md
-- MAGIC ## 6. Early Departure Count
-- MAGIC
-- MAGIC Use the existing is_early_departure flag (5-minute grace period).

-- COMMAND ----------

-- DBTITLE 1,Early Departures by Employee
-- Early departure count by employee
SELECT 
  t.client_employee_id,
  e.first_name,
  e.last_name,
  COUNT(*) AS early_departure_count,
  ROUND(SUM(t.early_departure_minutes) / 60.0, 2) AS total_early_departure_hours
FROM timesheet t
INNER JOIN employee e ON t.client_employee_id = e.client_employee_id
WHERE t.is_early_departure = TRUE
GROUP BY t.client_employee_id, e.first_name, e.last_name
ORDER BY early_departure_count DESC
LIMIT 20

-- COMMAND ----------

-- DBTITLE 1,Early Departures by Department
-- Early departure count by department
SELECT 
  e.department_code,
  COUNT(*) AS early_departure_count,
  COUNT(DISTINCT t.client_employee_id) AS employees_with_early_departures
FROM timesheet t
INNER JOIN employee e ON t.client_employee_id = e.client_employee_id
WHERE t.is_early_departure = TRUE
GROUP BY e.department_code
ORDER BY early_departure_count DESC

-- COMMAND ----------

-- DBTITLE 1,Section: Total Overtime Count
-- MAGIC %md
-- MAGIC ## 7. Total Overtime Count
-- MAGIC
-- MAGIC Use the existing is_overtime flag (5-minute grace period).

-- COMMAND ----------

-- DBTITLE 1,Overtime by Employee
-- Overtime events by employee
SELECT 
  t.client_employee_id,
  e.first_name,
  e.last_name,
  COUNT(*) AS overtime_event_count,
  ROUND(SUM(t.overtime_minutes) / 60.0, 2) AS total_overtime_hours
FROM timesheet t
INNER JOIN employee e ON t.client_employee_id = e.client_employee_id
WHERE t.is_overtime = TRUE
GROUP BY t.client_employee_id, e.first_name, e.last_name
ORDER BY overtime_event_count DESC
LIMIT 20

-- COMMAND ----------

-- DBTITLE 1,Overtime by Department
-- Overtime events by department
SELECT 
  e.department_code,
  COUNT(*) AS overtime_event_count,
  ROUND(SUM(t.overtime_minutes) / 60.0, 2) AS total_overtime_hours,
  COUNT(DISTINCT t.client_employee_id) AS employees_with_overtime
FROM timesheet t
INNER JOIN employee e ON t.client_employee_id = e.client_employee_id
WHERE t.is_overtime = TRUE
GROUP BY e.department_code
ORDER BY overtime_event_count DESC

-- COMMAND ----------

-- DBTITLE 1,Section: Rolling Average Working Hours
-- MAGIC %md
-- MAGIC ## 8. Rolling Average Working Hours
-- MAGIC
-- MAGIC Use the existing Gold rolling_avg_hours table.

-- COMMAND ----------

-- DBTITLE 1,Rolling Average Trend
-- Recent rolling average working hours trend
SELECT 
  punch_apply_date,
  ROUND(actual_duration_hours, 2) AS daily_hours,
  ROUND(rolling_7day_avg_hours, 2) AS avg_7day,
  ROUND(rolling_30day_avg_hours, 2) AS avg_30day
FROM rolling_avg_hours
WHERE punch_apply_date IS NOT NULL
ORDER BY punch_apply_date DESC
LIMIT 30

-- COMMAND ----------

-- DBTITLE 1,Section: Early Attrition Rate
-- MAGIC %md
-- MAGIC ## 9. Early Attrition Rate
-- MAGIC
-- MAGIC Early attrition = termination within 90 days of hire.

-- COMMAND ----------

-- DBTITLE 1,Early Attrition Calculation
-- Early attrition rate
WITH attrition_summary AS (
  SELECT 
    COUNT(*) AS total_employees,
    SUM(CASE WHEN is_early_attrition = TRUE THEN 1 ELSE 0 END) AS early_attrition_count
  FROM employee
)
SELECT 
  total_employees,
  early_attrition_count,
  CASE 
    WHEN total_employees > 0 
    THEN ROUND(100.0 * early_attrition_count / total_employees, 2)
    ELSE 0 
  END AS early_attrition_rate_pct
FROM attrition_summary

-- COMMAND ----------

-- DBTITLE 1,Section: SQL Demonstrations
-- MAGIC %md
-- MAGIC ## 10. Additional SQL Demonstrations
-- MAGIC
-- MAGIC Demonstrate JOIN, GROUP BY, CASE, and window functions.

-- COMMAND ----------

-- DBTITLE 1,Demo: JOIN - Employee Timesheet Summary
-- JOIN demonstration: Employee with timesheet summary
SELECT 
  e.client_employee_id,
  e.first_name,
  e.last_name,
  e.department_code,
  e.employment_status,
  COUNT(t.timesheet_id) AS total_timesheet_records,
  ROUND(SUM(t.hours_worked), 2) AS total_hours_worked
FROM employee e
LEFT JOIN timesheet t ON e.client_employee_id = t.client_employee_id
GROUP BY e.client_employee_id, e.first_name, e.last_name, e.department_code, e.employment_status
ORDER BY total_hours_worked DESC NULLS LAST
LIMIT 20

-- COMMAND ----------

-- DBTITLE 1,Demo: CASE - Employee Status Classification
-- CASE demonstration: Employee status classification
SELECT 
  client_employee_id,
  first_name,
  last_name,
  hire_date,
  term_date,
  tenure_days,
  CASE 
    WHEN term_date IS NOT NULL AND tenure_days <= 90 THEN 'Early Attrition'
    WHEN term_date IS NOT NULL THEN 'Terminated'
    WHEN tenure_days > 365 THEN 'Tenured Employee'
    WHEN tenure_days BETWEEN 91 AND 365 THEN 'New Employee'
    ELSE 'Very New Employee'
  END AS employee_category
FROM employee
ORDER BY tenure_days DESC NULLS LAST
LIMIT 30

-- COMMAND ----------

-- DBTITLE 1,Demo: Window Function - Ranked Departments by Working Hours
-- Window function demonstration: Rank departments by total working hours
WITH dept_hours AS (
  SELECT 
    e.department_code,
    COUNT(DISTINCT t.client_employee_id) AS employee_count,
    ROUND(SUM(t.hours_worked), 2) AS total_hours
  FROM employee e
  INNER JOIN timesheet t ON e.client_employee_id = t.client_employee_id
  WHERE e.department_code IS NOT NULL
  GROUP BY e.department_code
)
SELECT 
  department_code,
  employee_count,
  total_hours,
  RANK() OVER (ORDER BY total_hours DESC) AS hours_rank,
  ROUND(100.0 * total_hours / SUM(total_hours) OVER (), 2) AS pct_of_total_hours
FROM dept_hours
ORDER BY hours_rank

-- COMMAND ----------

-- DBTITLE 1,Section: Final KPI Summary
-- MAGIC %md
-- MAGIC ## 11. Final KPI Summary
-- MAGIC
-- MAGIC One comprehensive view of all key performance indicators.

-- COMMAND ----------

-- DBTITLE 1,KPI Summary Dashboard
-- Comprehensive KPI summary
WITH current_headcount AS (
  SELECT COUNT(*) AS active_employees
  FROM employee
  WHERE active_status = TRUE
),
terminations AS (
  SELECT COUNT(*) AS total_terminations
  FROM employee
  WHERE term_date IS NOT NULL
),
early_attrition AS (
  SELECT 
    COUNT(*) AS early_attrition_count,
    COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM employee), 0) AS early_attrition_rate_pct
  FROM employee
  WHERE is_early_attrition = TRUE
),
avg_hours AS (
  SELECT ROUND(AVG(avg_hours_per_day), 2) AS avg_working_hours_per_day
  FROM employee_working_hours
  WHERE total_working_days > 0
),
late_arrivals AS (
  SELECT COUNT(*) AS total_late_arrivals
  FROM timesheet
  WHERE is_late_arrival = TRUE
),
early_departures AS (
  SELECT COUNT(*) AS total_early_departures
  FROM timesheet
  WHERE is_early_departure = TRUE
),
overtime_count AS (
  SELECT 
    COUNT(*) AS total_overtime_events,
    ROUND(SUM(overtime_minutes) / 60.0, 2) AS total_overtime_hours
  FROM timesheet
  WHERE is_overtime = TRUE
)
SELECT 
  ch.active_employees,
  t.total_terminations,
  ea.early_attrition_count,
  ROUND(ea.early_attrition_rate_pct, 2) AS early_attrition_rate_pct,
  ah.avg_working_hours_per_day,
  la.total_late_arrivals,
  ed.total_early_departures,
  oc.total_overtime_events,
  oc.total_overtime_hours
FROM current_headcount ch
CROSS JOIN terminations t
CROSS JOIN early_attrition ea
CROSS JOIN avg_hours ah
CROSS JOIN late_arrivals la
CROSS JOIN early_departures ed
CROSS JOIN overtime_count oc