# Employee Timesheet Analytics

An end-to-end data engineering project built using Databricks, PySpark,
Spark SQL, Delta Lake and FastAPI.

The project processes employee and timesheet data to generate workforce,
attendance and productivity analytics.

## Project Status

In development.

## Business Objective

The goal of this project is to build an end-to-end ETL pipeline that
transforms employee and timesheet data into reliable, analytics-ready
datasets and business insights.

The project covers:

- Data ingestion
- Data cleaning and transformation
- Data quality validation
- Analytics
- Data visualization
- Pipeline orchestration
- REST API
- Documentation

## Technology Stack

- Databricks
- PySpark
- Spark SQL
- Delta Lake
- Unity Catalog
- Python
- FastAPI
- GitHub

## Architecture

The project follows a Medallion-style architecture:

Source Files
    ↓
Bronze
    ↓
Silver
    ↓
Gold
    ↓
Analytics / Dashboard


## Data Sources

The project uses:

- Employee data
- Timesheet data

The original source files are stored separately from the Git repository
and loaded into a Databricks Volume.

## Databricks Structure

Current Databricks structure:

workforce
├── raw
│   └── raw_data
├── bronze
├── silver
└── gold

## Analytics

The project will produce metrics including:

- Active headcount over time
- Turnover trend
- Average tenure by department
- Average working hours
- Late arrival frequency
- Early departure count
- Overtime
- Rolling average working hours
- Early attrition rate

## Project Structure

```text
employee-timesheet-analytics/
│
├── README.md
├── .gitignore
├── notebooks/
├── sql/
└── docs/