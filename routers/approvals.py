from fastapi import APIRouter
from schemas.approvals import ApprovalActionRequest
from services.approval_service import list_approvals, approve

router = APIRouter(prefix="/api/approvals", tags=["Approvals"])


@router.get("")
def get_all():
    return list_approvals()


@router.post("/{approval_id}/approve")
def approve_item(approval_id: str, payload: ApprovalActionRequest):
    return approve(approval_id, payload.approver_email)
