"""One-off loader: reads the Silver CSV exports and fills the local
SQLite database. Safe to re-run, it wipes and reloads both tables
each time instead of appending duplicates.

Reads from local disk by default. Set DATA_SOURCE=s3 to read the same
files from a MinIO/S3 bucket instead (see the S3_* env vars below).

Usage (from the api/ directory):
    python -m db.load_data
"""

import os
from pathlib import Path

import pandas as pd
from sqlalchemy import delete, insert

from db.models import Employee, Timesheet
from db.session import SessionLocal, init_db

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "silver"

DATA_SOURCE = os.environ.get("DATA_SOURCE", "local")  # "local" or "s3"
S3_BUCKET = os.environ.get("S3_BUCKET", "")
S3_PREFIX = os.environ.get("S3_PREFIX", "silver")
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "")

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


def _read_csv(filename, **kwargs):
    """Reads a Silver CSV from local disk, or from a MinIO/S3 bucket
    when DATA_SOURCE=s3. Same file layout either way, just a different
    source root.
    """
    if DATA_SOURCE == "s3":
        path = f"s3://{S3_BUCKET}/{S3_PREFIX}/{filename}"
        storage_options = {
            "key": S3_ACCESS_KEY,
            "secret": S3_SECRET_KEY,
            "client_kwargs": {"endpoint_url": S3_ENDPOINT_URL},
        }
        return pd.read_csv(path, storage_options=storage_options, **kwargs)
    return pd.read_csv(DATA_DIR / filename, **kwargs)


def _prepare(df, date_columns):
    """Keep only the columns our table actually has, parse dates, and
    turn pandas' NaN/NaT into plain None so SQLAlchemy can bind them.
    """
    for column in date_columns:
        df[column] = pd.to_datetime(df[column], errors="coerce")
    return df.astype(object).where(pd.notnull(df), None)


def load_employees(session):
    df = _read_csv("employee.csv", dtype=EMPLOYEE_ID_COLUMNS)
    columns = [c.name for c in Employee.__table__.columns]
    df = _prepare(df[columns], EMPLOYEE_DATE_COLUMNS)

    session.execute(delete(Employee))
    session.execute(insert(Employee), df.to_dict(orient="records"))
    print(f"Loaded {len(df)} employees")


def load_timesheets(session):
    # .gz because the raw CSV is ~166MB, over GitHub's 100MB file limit.
    # pandas decompresses on the fly based on the extension.
    df = _read_csv("timesheet.csv.gz", dtype=TIMESHEET_ID_COLUMNS)
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
