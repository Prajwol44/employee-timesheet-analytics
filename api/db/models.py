"""SQLAlchemy models for the Employee and Timesheet tables.

Columns mirror the Silver layer output from notebooks/02_silver_employee.py
and notebooks/03_silver_timesheet.py. Pipeline lineage columns
(_source_file, _ingested_at, _batch_id) and PII fields (dob, address,
city, state, country, cell_phone, work_email) are dropped on purpose,
the API has no use for them.
"""

from sqlalchemy import Boolean, Column, Date, DateTime, Float, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Employee(Base):
    __tablename__ = "employees"

    client_employee_id = Column(String, primary_key=True)

    # identity
    first_name = Column(String)
    last_name = Column(String)
    preferred_name = Column(String)
    full_name = Column(String)

    # job info
    job_code = Column(String)
    job_title = Column(String)
    job_start_date = Column(Date)
    department_id = Column(String)
    department_code = Column(String)
    department_name = Column(String)
    organization_name = Column(String)

    # manager can point to an employee_id that doesn't exist in this
    # table (manager_exists tells us which), so no FK constraint here.
    manager_employee_id = Column(String, index=True)
    manager_exists = Column(Boolean)

    fte_status = Column(String)
    scheduled_weekly_hour = Column(Float)

    # employment dates / status
    hire_date = Column(Date)
    recent_hire_date = Column(Date)
    anniversary_date = Column(Date)
    term_date = Column(Date)
    active_status = Column(Boolean)
    employment_status = Column(String)
    termination_reason = Column(String)

    # tenure, already computed in Silver
    tenure_days = Column(Integer)
    tenure_years = Column(Float)
    tenure_end_date = Column(Date)
    days_to_termination = Column(Integer)
    is_early_attrition = Column(Boolean)


class Timesheet(Base):
    __tablename__ = "timesheets"

    timesheet_id = Column(String, primary_key=True)

    # Not a real foreign key on purpose: the Gold layer intentionally
    # keeps timesheet rows whose employee_id has no match in the
    # employee master (see employee_master_match below), so a hard FK
    # would reject valid rows. Indexed because the API filters by it.
    client_employee_id = Column(String, index=True)
    employee_master_match = Column(Boolean)

    punch_apply_date = Column(Date, index=True)
    punch_in_datetime = Column(DateTime)
    punch_out_datetime = Column(DateTime)
    scheduled_start_datetime = Column(DateTime)
    scheduled_end_datetime = Column(DateTime)

    department_id = Column(String)
    department_name = Column(String)
    home_department_id = Column(String)
    home_department_name = Column(String)

    pay_code = Column(String)
    pay_code_count = Column(Integer)
    has_multiple_pay_codes = Column(Boolean)
    has_overtime_pay_code = Column(Boolean)
    has_leave_pay_code = Column(Boolean)
    punch_in_comment = Column(String)
    punch_out_comment = Column(String)

    hours_worked = Column(Float)
    actual_duration_minutes = Column(Float)
    actual_duration_hours = Column(Float)
    scheduled_duration_minutes = Column(Float)
    scheduled_duration_hours = Column(Float)
    hours_worked_vs_punch_variance = Column(Float)
    source_duplicate_count = Column(Integer)
    is_schedulable = Column(Boolean)

    # the 5-minute-grace attendance flags from Silver
    arrival_variance_minutes = Column(Float)
    late_arrival_minutes = Column(Float)
    is_late_arrival = Column(Boolean)
    departure_variance_minutes = Column(Float)
    early_departure_minutes = Column(Float)
    is_early_departure = Column(Boolean)
    schedule_variance_minutes = Column(Float)
    overtime_minutes = Column(Float)
    overtime_hours = Column(Float)
    is_overtime = Column(Boolean)
    is_excessive_duration = Column(Boolean)
