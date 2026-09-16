from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy.orm import Session

from db.models import Employee
from db.session import get_db
from schemas import EmployeeCreate, EmployeeOut, EmployeeUpdate

app = FastAPI(
    title="Employee Timesheet Analytics API",
    version="0.1.0",
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/employees", response_model=EmployeeOut, status_code=201)
def create_employee(employee: EmployeeCreate, db: Session = Depends(get_db)):
    if db.get(Employee, employee.client_employee_id):
        raise HTTPException(status_code=409, detail="Employee already exists")
    db_employee = Employee(**employee.model_dump())
    db.add(db_employee)
    db.commit()
    db.refresh(db_employee)
    return db_employee


@app.get("/employees", response_model=list[EmployeeOut])
def list_employees(db: Session = Depends(get_db)):
    return db.query(Employee).all()


@app.get("/employees/{employee_id}", response_model=EmployeeOut)
def get_employee(employee_id: str, db: Session = Depends(get_db)):
    employee = db.get(Employee, employee_id)
    if not employee:
        raise HTTPException(status_code=404, detail="Employee not found")
    return employee


@app.put("/employees/{employee_id}", response_model=EmployeeOut)
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


@app.delete("/employees/{employee_id}", status_code=204)
def delete_employee(employee_id: str, db: Session = Depends(get_db)):
    employee = db.get(Employee, employee_id)
    if not employee:
        raise HTTPException(status_code=404, detail="Employee not found")
    db.delete(employee)
    db.commit()