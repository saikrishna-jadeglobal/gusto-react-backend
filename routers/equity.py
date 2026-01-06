from fastapi import APIRouter
from schemas.equity import EquityRunRequest, EquityRunResponse
from services.equity_service import run_equity
from services.approval_service import create_approval

router = APIRouter(prefix="/api/equity", tags=["Equity"])

@router.post("/run", response_model=EquityRunResponse)
def run(payload: EquityRunRequest):
    result = run_equity(payload)
    approval = create_approval("equity", result)
    return approval
