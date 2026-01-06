from pydantic import BaseModel

class ApprovalActionRequest(BaseModel):
    approver_email: str
