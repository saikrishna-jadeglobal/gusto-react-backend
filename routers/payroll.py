# routers/payroll_router.py

from fastapi import APIRouter
from services.payroll_service import run_payroll

router = APIRouter(prefix="/api/payroll", tags=["Payroll"])

@router.post("/run")
def run_payroll_api():
    """
    Trigger Payroll reconciliation.
    """
    return run_payroll()
