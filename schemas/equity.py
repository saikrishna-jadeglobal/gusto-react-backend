from pydantic import BaseModel
from typing import List, Optional

class EquityTask(BaseModel):
    id: str
    label: str
    hasCheckbox: Optional[bool] = False
    completed: Optional[bool] = False


class EquityRunRequest(BaseModel):
    period: str
    business_day: Optional[str] = None
    tasks: List[EquityTask]


class EquityRunResponse(BaseModel):
    approvalId: str
    workflowName: str
    agentType: str
    status: str
    results: dict
