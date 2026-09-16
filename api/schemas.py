"""Pydantic request/response shapes for the Employee API."""

from datetime import date

from pydantic import BaseModel, ConfigDict


class EmployeeBase(BaseModel):
    first_name: str | None = None
    last_name: str | None = None
    preferred_name: str | None = None
    full_name: str | None = None
    job_code: str | None = None
    job_title: str | None = None
    job_start_date: date | None = None
    department_id: str | None = None
    department_code: str | None = None
    department_name: str | None = None
    organization_name: str | None = None
    manager_employee_id: str | None = None
    manager_exists: bool | None = None
    fte_status: str | None = None
    scheduled_weekly_hour: float | None = None
    hire_date: date | None = None
    recent_hire_date: date | None = None
    anniversary_date: date | None = None
    term_date: date | None = None
    active_status: bool | None = None
    employment_status: str | None = None
    termination_reason: str | None = None
    tenure_days: int | None = None
    tenure_years: float | None = None
    tenure_end_date: date | None = None
    days_to_termination: int | None = None
    is_early_attrition: bool | None = None


class EmployeeCreate(EmployeeBase):
    client_employee_id: str


class EmployeeUpdate(EmployeeBase):
    pass


class EmployeeOut(EmployeeBase):
    model_config = ConfigDict(from_attributes=True)

    client_employee_id: str
