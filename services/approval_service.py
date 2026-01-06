import json
from pathlib import Path
from services.netsuite_service import push_equity_to_netsuite

STATE_FILE = Path("state/approvals.json")
STATE_FILE.parent.mkdir(exist_ok=True)

def _load():
    if not STATE_FILE.exists():
        return {}
    return json.loads(STATE_FILE.read_text())

def _save(data):
    STATE_FILE.write_text(json.dumps(data, indent=2))

def create_approval(agent_type, result):
    data = _load()
    approval_id = f"{agent_type}-{len(data)+1}"

    data[approval_id] = {
    "approvalId": approval_id,
    "workflowName": "Equity Month-End Close",
    "agentType": agent_type,
    "status": "pending",
    "results": result
}


    _save(data)
    return data[approval_id]

def list_approvals():
    return list(_load().values())

def approve(approval_id):
    data = _load()
    approval = data[approval_id]

    if approval["agentType"] == "equity":
        push_equity_to_netsuite()

    approval["status"] = "approved"
    _save(data)
    return approval
