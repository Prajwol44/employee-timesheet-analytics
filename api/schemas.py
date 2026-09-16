"""Pydantic request/response shapes for the Employee and Timesheet APIs."""

from datetime import date, datetime

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


class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class EmployeeOut(EmployeeBase):
    model_config = ConfigDict(from_attributes=True)

    client_employee_id: str


class TimesheetOut(BaseModel):
    """Read-only, so there's no Create/Update counterpart."""

    model_config = ConfigDict(from_attributes=True)

    timesheet_id: str
    client_employee_id: str
    employee_master_match: bool

    punch_apply_date: date
    punch_in_datetime: datetime | None = None
    punch_out_datetime: datetime | None = None
    scheduled_start_datetime: datetime | None = None
    scheduled_end_datetime: datetime | None = None

    department_id: str | None = None
    department_name: str | None = None
    home_department_id: str | None = None
    home_department_name: str | None = None

    pay_code: str | None = None
    pay_code_count: int | None = None
    has_multiple_pay_codes: bool | None = None
    has_overtime_pay_code: bool | None = None
    has_leave_pay_code: bool | None = None
    punch_in_comment: str | None = None
    punch_out_comment: str | None = None

    hours_worked: float | None = None
    actual_duration_minutes: float | None = None
    actual_duration_hours: float | None = None
    scheduled_duration_minutes: float | None = None
    scheduled_duration_hours: float | None = None
    hours_worked_vs_punch_variance: float | None = None
    source_duplicate_count: int | None = None
    is_schedulable: bool | None = None

    arrival_variance_minutes: float | None = None
    late_arrival_minutes: float | None = None
    is_late_arrival: bool | None = None
    departure_variance_minutes: float | None = None
    early_departure_minutes: float | None = None
    is_early_departure: bool | None = None
    schedule_variance_minutes: float | None = None
    overtime_minutes: float | None = None
    overtime_hours: float | None = None
    is_overtime: bool | None = None
    is_excessive_duration: bool | None = None
