"""One-off loader: reads the Silver CSV exports and fills the local
SQLite database. Safe to re-run, it wipes and reloads both tables
each time instead of appending duplicates.

Usage (from the api/ directory):
    python -m db.load_data
"""

from pathlib import Path

import pandas as pd
from sqlalchemy import delete, insert

from db.models import Employee, Timesheet
from db.session import SessionLocal, init_db

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "silver"

EMPLOYEE_DATE_COLUMNS = [
    "job_start_date",
    "hire_date",
    "recent_hire_date",
    "anniversary_date",
    "term_date",
    "tenure_end_date",
]

TIMESHEET_DATE_COLUMNS = ["punch_apply_date"]
TIMESHEET_DATETIME_COLUMNS = [
    "punch_in_datetime",
    "punch_out_datetime",
    "scheduled_start_datetime",
    "scheduled_end_datetime",
]

# ID/code columns must stay text. Left to type inference, pandas reads
# a column like "00037" as an integer and drops the leading zeros,
# which would break every employee <-> timesheet match.
EMPLOYEE_ID_COLUMNS = {
    "client_employee_id": str,
    "manager_employee_id": str,
    "department_id": str,
    "department_code": str,
    "job_code": str,
}
TIMESHEET_ID_COLUMNS = {
    "client_employee_id": str,
    "timesheet_id": str,
    "department_id": str,
    "home_department_id": str,
}


def _prepare(df, date_columns):
    """Keep only the columns our table actually has, parse dates, and
    turn pandas' NaN/NaT into plain None so SQLAlchemy can bind them.
    """
    for column in date_columns:
        df[column] = pd.to_datetime(df[column], errors="coerce")
    return df.astype(object).where(pd.notnull(df), None)


def load_employees(session):
    df = pd.read_csv(DATA_DIR / "employee.csv", dtype=EMPLOYEE_ID_COLUMNS)
    columns = [c.name for c in Employee.__table__.columns]
    df = _prepare(df[columns], EMPLOYEE_DATE_COLUMNS)

    session.execute(delete(Employee))
    session.execute(insert(Employee), df.to_dict(orient="records"))
    print(f"Loaded {len(df)} employees")


def load_timesheets(session):
    df = pd.read_csv(DATA_DIR / "timesheet.csv", dtype=TIMESHEET_ID_COLUMNS)
    columns = [c.name for c in Timesheet.__table__.columns]
    df = _prepare(df[columns], TIMESHEET_DATE_COLUMNS + TIMESHEET_DATETIME_COLUMNS)

    session.execute(delete(Timesheet))
    session.execute(insert(Timesheet), df.to_dict(orient="records"))
    print(f"Loaded {len(df)} timesheet rows")


def main():
    init_db()
    with SessionLocal() as session:
        load_employees(session)
        load_timesheets(session)
        session.commit()
    print("Done.")


if __name__ == "__main__":
    main()
