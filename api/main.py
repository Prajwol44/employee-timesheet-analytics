from datetime import date

from fastapi import Depends, FastAPI, HTTPException, Query
from sqlalchemy.orm import Session

from auth import authenticate, create_access_token, get_current_user, require_admin
from db.models import Employee, Timesheet
from db.session import get_db
from schemas import (
    EmployeeCreate,
    EmployeeOut,
    EmployeeUpdate,
    LoginRequest,
    TimesheetOut,
    TokenResponse,
)

app = FastAPI(
    title="Employee Timesheet Analytics API",
    version="0.1.0",
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/login", response_model=TokenResponse)
def login(credentials: LoginRequest):
    role = authenticate(credentials.username, credentials.password)
    if not role:
        raise HTTPException(status_code=401, detail="Invalid username or password")
    return TokenResponse(access_token=create_access_token(credentials.username, role))


@app.post(
    "/employees",
    response_model=EmployeeOut,
    status_code=201,
    dependencies=[Depends(require_admin)],
)
def create_employee(employee: EmployeeCreate, db: Session = Depends(get_db)):
    if db.get(Employee, employee.client_employee_id):
        raise HTTPException(status_code=409, detail="Employee already exists")
    db_employee = Employee(**employee.model_dump())
    db.add(db_employee)
    db.commit()
    db.refresh(db_employee)
    return db_employee


@app.get(
    "/employees",
    response_model=list[EmployeeOut],
    dependencies=[Depends(get_current_user)],
)
def list_employees(db: Session = Depends(get_db)):
    return db.query(Employee).all()


@app.get(
    "/employees/{employee_id}",
    response_model=EmployeeOut,
    dependencies=[Depends(get_current_user)],
)
def get_employee(employee_id: str, db: Session = Depends(get_db)):
    employee = db.get(Employee, employee_id)
    if not employee:
        raise HTTPException(status_code=404, detail="Employee not found")
    return employee


@app.put(
    "/employees/{employee_id}",
    response_model=EmployeeOut,
    dependencies=[Depends(require_admin)],
)
def update_employee(
    employee_id: str, update: EmployeeUpdate, db: Session = Depends(get_db)
):
    employee = db.get(Employee, employee_id)
    if not employee:
        raise HTTPException(status_code=404, detail="Employee not found")
    for field, value in update.model_dump(exclude_unset=True).items():
        setattr(employee, field, value)
    db.commit()
    db.refresh(employee)
    return employee


@app.delete(
    "/employees/{employee_id}",
    status_code=204,
    dependencies=[Depends(require_admin)],
)
def delete_employee(employee_id: str, db: Session = Depends(get_db)):
    employee = db.get(Employee, employee_id)
    if not employee:
        raise HTTPException(status_code=404, detail="Employee not found")
    db.delete(employee)
    db.commit()


def _filter_by_date(query, start_date: date | None, end_date: date | None):
    if start_date:
        query = query.filter(Timesheet.punch_apply_date >= start_date)
    if end_date:
        query = query.filter(Timesheet.punch_apply_date <= end_date)
    return query


@app.get(
    "/timesheets",
    response_model=list[TimesheetOut],
    dependencies=[Depends(get_current_user)],
)
def list_timesheets(
    employee_id: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int = Query(default=100, le=1000),
    offset: int = 0,
    db: Session = Depends(get_db),
):
    query = db.query(Timesheet)
    if employee_id:
        query = query.filter(Timesheet.client_employee_id == employee_id)
    query = _filter_by_date(query, start_date, end_date)
    return (
        query.order_by(Timesheet.punch_apply_date)
        .offset(offset)
        .limit(limit)
        .all()
    )


@app.get(
    "/timesheets/{employee_id}",
    response_model=list[TimesheetOut],
    dependencies=[Depends(get_current_user)],
)
def get_employee_timesheets(
    employee_id: str,
    start_date: date | None = None,
    end_date: date | None = None,
    db: Session = Depends(get_db),
):
    query = db.query(Timesheet).filter(Timesheet.client_employee_id == employee_id)
    query = _filter_by_date(query, start_date, end_date)
    return query.order_by(Timesheet.punch_apply_date).all()