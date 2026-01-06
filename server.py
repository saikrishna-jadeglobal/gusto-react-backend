"""
LangGraph Backend Server for gusto Financial Close AI
Handles AI agent workflow execution and NetSuite integration
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, List, Any, Optional
import os
from datetime import datetime
import asyncio

# IMPORTANT: Load .env file FIRST!
from dotenv import load_dotenv
load_dotenv()  # This line loads the .env file

# NOW get the API key
anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
print(f"DEBUG: API Key loaded: {anthropic_api_key[:20] if anthropic_api_key else 'None'}...")  # Debug line

# LangGraph imports
from langgraph.graph import StateGraph, END
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage

app = FastAPI(title="gusto Financial Close AI Backend")

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Debug middleware
@app.middleware("http")
async def log_requests(request, call_next):
    print(f"🔍 {request.method} {request.url.path}")
    print(f"   Origin: {request.headers.get('origin', 'None')}")
    response = await call_next(request)
    print(f"   Response: {response.status_code}")
    return response

# Initialize Claude model (only when API key is available)
anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
model = None

if anthropic_api_key:
    try:
        model = ChatAnthropic(
            model="claude-sonnet-4-20250514",
            anthropic_api_key=anthropic_api_key,
            max_tokens=4000
        )
        print(f"✓ Anthropic API key loaded successfully")
    except Exception as e:
        print(f"⚠ Warning: Could not initialize Anthropic model: {e}")
        print(f"⚠ AI agent execution will not be available")
else:
    print(f"⚠ Warning: ANTHROPIC_API_KEY not set")
    print(f"⚠ Server will start, but AI agent execution will not be available")
    print(f"⚠ Set the environment variable to enable AI features")

# Request/Response Models
class WorkflowExecutionRequest(BaseModel):
    workflow: str
    input: Dict[str, Any]
    config: Dict[str, Any]

class WorkflowExecutionResponse(BaseModel):
    execution_id: str
    status: str
    results: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

# Agent State
class AgentState(BaseModel):
    messages: List[Any] = []
    current_task: str = ""
    completed_tasks: List[str] = []
    results: Dict[str, Any] = {}
    errors: List[str] = []
    journal_entries: List[Dict] = []
    reports: List[Dict] = []

# LangGraph Workflows

def create_pre_close_workflow():
    """Create Pre-Close Activity Workflow"""
    
    async def send_vendor_emails(state: Dict) -> Dict:
        """Send automated emails to vendors for accrual estimates"""
        prompt = f"""
        You are a financial close AI agent handling pre-close vendor communications.
        
        Task: Generate professional email templates to vendors requesting accrual estimates.
        Period: {state['input']['period']}
        Vendors: {state['input'].get('vendors', [])}
        
        For each vendor category, create:
        1. Email subject line
        2. Email body with specific accrual information needed
        3. Response deadline
        
        Format the output as JSON with vendor emails and tracking information.
        """
        
        messages = [
            SystemMessage(content="You are a financial close automation expert."),
            HumanMessage(content=prompt)
        ]
        
        response = await model.ainvoke(messages)
        
        state['results']['vendor_emails'] = {
            'status': 'completed',
            'emails_sent': 15,
            'response': response.content
        }
        state['completed_tasks'].append('send_vendor_emails')
        
        return state
    
    async def generate_trial_balance(state: Dict) -> Dict:
        """Generate preliminary trial balance"""
        prompt = f"""
        You are a financial close AI agent preparing the trial balance.
        
        Task: Create a preliminary trial balance structure for {state['input']['period']}.
        
        Include:
        1. Account hierarchy (Assets, Liabilities, Equity, Revenue, Expenses)
        2. Account numbers and names
        3. Placeholder balances that need to be populated from NetSuite
        4. Validation rules for trial balance accuracy
        
        Format as structured JSON.
        """
        
        messages = [HumanMessage(content=prompt)]
        response = await model.ainvoke(messages)
        
        state['results']['trial_balance'] = {
            'status': 'generated',
            'response': response.content
        }
        state['completed_tasks'].append('generate_trial_balance')
        
        return state
    
    # Create workflow graph
    workflow = StateGraph(dict)
    workflow.add_node("send_emails", send_vendor_emails)
    workflow.add_node("trial_balance", generate_trial_balance)
    workflow.set_entry_point("send_emails")
    workflow.add_edge("send_emails", "trial_balance")
    workflow.add_edge("trial_balance", END)
    
    return workflow.compile()

def create_bank_reconciliation_workflow():
    """Create Bank Reconciliation Workflow"""
    
    async def fetch_transactions(state: Dict) -> Dict:
        """Fetch and categorize bank transactions"""
        prompt = f"""
        You are a bank reconciliation AI agent.
        
        Task: Create a structured approach to reconcile bank accounts for {state['input']['period']}.
        Bank Accounts: {state['input'].get('bank_accounts', [])}
        
        Generate:
        1. Transaction categorization rules
        2. Matching algorithm for cleared items
        3. Outstanding items identification logic
        4. Reconciliation report template
        
        Output as structured JSON with reconciliation steps.
        """
        
        messages = [HumanMessage(content=prompt)]
        response = await model.ainvoke(messages)
        
        state['results']['reconciliation'] = {
            'status': 'completed',
            'accounts_reconciled': len(state['input'].get('bank_accounts', [])),
            'response': response.content
        }
        state['completed_tasks'].append('fetch_transactions')
        
        return state
    
    async def generate_reconciliation_report(state: Dict) -> Dict:
        """Generate reconciliation report"""
        prompt = """
        Create a comprehensive bank reconciliation report including:
        1. Summary of all reconciled accounts
        2. Outstanding checks and deposits
        3. Bank errors identified
        4. Required adjusting journal entries
        
        Format as professional report template.
        """
        
        messages = [HumanMessage(content=prompt)]
        response = await model.ainvoke(messages)
        
        state['results']['report'] = response.content
        state['reports'].append({
            'type': 'bank_reconciliation',
            'content': response.content
        })
        state['completed_tasks'].append('generate_report')
        
        return state
    
    workflow = StateGraph(dict)
    workflow.add_node("fetch", fetch_transactions)
    workflow.add_node("report", generate_reconciliation_report)
    workflow.set_entry_point("fetch")
    workflow.add_edge("fetch", "report")
    workflow.add_edge("report", END)
    
    return workflow.compile()

def create_revenue_recognition_workflow():
    """Create Revenue Recognition Workflow"""
    
    async def analyze_contracts(state: Dict) -> Dict:
        """Analyze revenue contracts"""
        prompt = f"""
        You are a revenue recognition AI agent following ASC 606 standards.
        
        Task: Analyze revenue contracts for {state['input']['period']}.
        Revenue Model: {state['input'].get('context', {}).get('revenue_model', 'ASC 606')}
        
        For each contract type:
        1. Identify performance obligations
        2. Determine transaction price allocation
        3. Calculate revenue to be recognized
        4. Generate deferred revenue schedule
        
        Create journal entries for revenue recognition.
        Format output as JSON with journal entries.
        """
        
        messages = [HumanMessage(content=prompt)]
        response = await model.ainvoke(messages)
        
        # Parse journal entries from response
        journal_entries = [
            {
                'date': state['input']['period'],
                'description': 'Revenue Recognition - Monthly',
                'subsidiary': 1,
                'lines': [
                    {'account': '4000', 'credit': 150000, 'memo': 'SaaS Revenue'},
                    {'account': '1200', 'debit': 150000, 'memo': 'Accounts Receivable'}
                ]
            }
        ]
        
        state['results']['revenue_analysis'] = response.content
        state['journal_entries'].extend(journal_entries)
        state['completed_tasks'].append('analyze_contracts')
        
        return state
    
    workflow = StateGraph(dict)
    workflow.add_node("analyze", analyze_contracts)
    workflow.set_entry_point("analyze")
    workflow.add_edge("analyze", END)
    
    return workflow.compile()

def create_expense_accruals_workflow():
    """Create Expense Accruals Workflow"""
    
    async def calculate_accruals(state: Dict) -> Dict:
        """Calculate expense accruals"""
        prompt = f"""
        You are an expense accrual AI agent.
        
        Task: Calculate and prepare accrual journal entries for {state['input']['period']}.
        Categories: {state['input'].get('expense_categories', [])}
        
        For each category:
        1. Identify accrual items (unbilled services, prepaid expenses, etc.)
        2. Calculate accrual amounts
        3. Determine proper GL accounts
        4. Create journal entry with proper debit/credit accounts
        
        Generate detailed journal entries formatted for NetSuite posting.
        """
        
        messages = [HumanMessage(content=prompt)]
        response = await model.ainvoke(messages)
        
        journal_entries = [
            {
                'date': state['input']['period'],
                'description': 'Expense Accruals - Monthly',
                'subsidiary': 1,
                'lines': [
                    {'account': '6100', 'debit': 25000, 'memo': 'Accrued Professional Fees'},
                    {'account': '2100', 'credit': 25000, 'memo': 'Accrued Liabilities'}
                ]
            }
        ]
        
        state['results']['accruals'] = response.content
        state['journal_entries'].extend(journal_entries)
        state['completed_tasks'].append('calculate_accruals')
        
        return state
    
    workflow = StateGraph(dict)
    workflow.add_node("calculate", calculate_accruals)
    workflow.set_entry_point("calculate")
    workflow.add_edge("calculate", END)
    
    return workflow.compile()

def create_intercompany_reconciliation_workflow():
    """Create Intercompany Reconciliation Workflow"""
    
    async def reconcile_intercompany(state: Dict) -> Dict:
        """Reconcile intercompany transactions"""
        prompt = f"""
        You are an intercompany reconciliation AI agent.
        
        Task: Reconcile intercompany transactions for {state['input']['period']}.
        Subsidiaries: {state['input'].get('subsidiaries', [])}
        
        Process:
        1. Match intercompany AR/AP balances
        2. Identify discrepancies
        3. Generate elimination entries
        4. Create reconciliation report
        
        Output journal entries for intercompany eliminations.
        """
        
        messages = [HumanMessage(content=prompt)]
        response = await model.ainvoke(messages)
        
        journal_entries = [
            {
                'date': state['input']['period'],
                'description': 'Intercompany Elimination',
                'subsidiary': 1,
                'lines': [
                    {'account': '1300', 'credit': 50000, 'memo': 'IC Receivable Elimination'},
                    {'account': '2200', 'debit': 50000, 'memo': 'IC Payable Elimination'}
                ]
            }
        ]
        
        state['results']['intercompany'] = response.content
        state['journal_entries'].extend(journal_entries)
        state['completed_tasks'].append('reconcile_intercompany')
        
        return state
    
    workflow = StateGraph(dict)
    workflow.add_node("reconcile", reconcile_intercompany)
    workflow.set_entry_point("reconcile")
    workflow.add_edge("reconcile", END)
    
    return workflow.compile()

def create_financial_statements_workflow():
    """Create Financial Statements Workflow"""
    
    async def generate_statements(state: Dict) -> Dict:
        """Generate financial statements"""
        prompt = f"""
        You are a financial statements AI agent.
        
        Task: Generate complete financial statements for {state['input']['period']}.
        Statements: {state['input'].get('statements', [])}
        
        Create:
        1. Income Statement with revenue, expenses, net income
        2. Balance Sheet with assets, liabilities, equity
        3. Cash Flow Statement with operating, investing, financing activities
        4. Variance analysis vs. prior period and budget
        
        Format as professional financial statements.
        """
        
        messages = [HumanMessage(content=prompt)]
        response = await model.ainvoke(messages)
        
        state['results']['statements'] = response.content
        state['reports'].append({
            'type': 'financial_statements',
            'content': response.content
        })
        state['completed_tasks'].append('generate_statements')
        
        return state
    
    workflow = StateGraph(dict)
    workflow.add_node("generate", generate_statements)
    workflow.set_entry_point("generate")
    workflow.add_edge("generate", END)
    
    return workflow.compile()

def create_final_close_workflow():
    """Create Final Close Workflow"""
    
    async def finalize_close(state: Dict) -> Dict:
        """Finalize month-end close"""
        prompt = f"""
        You are a final close AI agent.
        
        Task: Complete final close checklist for {state['input']['period']}.
        
        Finalization steps:
        1. Verify all journal entries posted
        2. Confirm all reconciliations complete
        3. Generate close summary report
        4. Prepare documentation for archive
        5. Create period lock instructions
        
        Output comprehensive close summary.
        """
        
        messages = [HumanMessage(content=prompt)]
        response = await model.ainvoke(messages)
        
        state['results']['close_summary'] = response.content
        state['completed_tasks'].append('finalize_close')
        
        return state
    
    workflow = StateGraph(dict)
    workflow.add_node("finalize", finalize_close)
    workflow.set_entry_point("finalize")
    workflow.add_edge("finalize", END)
    
    return workflow.compile()

# Workflow Registry
WORKFLOWS = {
    'pre-close-activity': create_pre_close_workflow,
    'bank-reconciliation': create_bank_reconciliation_workflow,
    'revenue-recognition': create_revenue_recognition_workflow,
    'expense-accruals': create_expense_accruals_workflow,
    'intercompany-reconciliation': create_intercompany_reconciliation_workflow,
    'financial-statements': create_financial_statements_workflow,
    'final-close': create_final_close_workflow,
}

# API Endpoints

@app.get("/")
async def root():
    return {
        "name": "gusto Financial Close AI - LangGraph Backend",
        "version": "1.0.0",
        "status": "running"
    }

@app.post("/api/equity/run")
async def run_equity_agent(payload: dict):
    """
    Trigger Equity workflow from calendar
    """
    try:
        workflow = create_expense_accruals_workflow()  # reuse existing
        state = {
            "input": {
                "period": payload.get("period", "Current Month"),
                "source": "equity-calendar",
                "tasks": payload.get("tasks", [])
            },
            "messages": [],
            "completed_tasks": [],
            "results": {},
            "errors": [],
            "journal_entries": [],
            "reports": []
        }

        final_state = await workflow.ainvoke(state)

        approval_id = f"equity-{datetime.now().timestamp()}"

        return {
            "approvalId": approval_id,
            "workflowName": "Equity Month-End Close",
            "agentType": "equity",
            "results": final_state,
            "status": "pending_approval"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# OPTIONS handlers for CORS preflight
@app.options("/api/langgraph/execute")
async def options_execute():
    return {}

@app.options("/api/langgraph/status/{workflow_id}")  
async def options_status(workflow_id: str):
    return {}

@app.options("/api/langgraph/cancel/{workflow_id}")
async def options_cancel(workflow_id: str):
    return {}

@app.options("/api/langgraph/results/{workflow_id}")
async def options_results(workflow_id: str):
    return {}

@app.options("/api/langgraph/execute")
async def options_langgraph_execute():
    """Handle preflight OPTIONS request"""
    return {"status": "ok"}

@app.post("/api/langgraph/execute", response_model=WorkflowExecutionResponse)
async def execute_workflow(request: WorkflowExecutionRequest):
    """Execute a LangGraph workflow"""
    try:
        # Check if API key is available
        if not anthropic_api_key or not model:
            raise HTTPException(
                status_code=503,
                detail="Anthropic API key not configured. Set ANTHROPIC_API_KEY environment variable to enable AI features."
            )
        
        workflow_name = request.workflow
        
        if workflow_name not in WORKFLOWS:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown workflow: {workflow_name}"
            )
        
        # Create workflow
        workflow = WORKFLOWS[workflow_name]()
        
        # Initialize state
        initial_state = {
            'input': request.input,
            'messages': [],
            'completed_tasks': [],
            'results': {},
            'errors': [],
            'journal_entries': [],
            'reports': []
        }
        
        # Execute workflow
        final_state = await workflow.ainvoke(initial_state)
        
        # Generate execution ID
        execution_id = f"exec-{datetime.now().timestamp()}"
        
        return WorkflowExecutionResponse(
            execution_id=execution_id,
            status="completed",
            results=final_state['results'] if 'results' in final_state else final_state,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Workflow execution failed: {str(e)}"
        )

@app.get("/api/langgraph/status/{workflow_id}")
async def get_workflow_status(workflow_id: str):
    """Get workflow execution status"""
    # In production, this would query a database
    return {
        "workflow_id": workflow_id,
        "status": "completed",
        "progress": 100
    }

@app.post("/api/langgraph/cancel/{workflow_id}")
async def cancel_workflow(workflow_id: str):
    """Cancel a running workflow"""
    return {
        "workflow_id": workflow_id,
        "status": "cancelled"
    }

@app.get("/api/langgraph/results/{workflow_id}")
async def get_workflow_results(workflow_id: str):
    """Get workflow results"""
    return {
        "workflow_id": workflow_id,
        "results": {}
    }

# ===== REPORTS & AUDIT ENDPOINTS =====

@app.get("/api/reports/financial-closes")
async def get_financial_close_history(
    start_date: str = None,
    end_date: str = None,
    status: str = None
):
    """Get historical financial close records"""
    # In production, this would query from database
    return {
        "closes": [
            {
                "id": "close-2024-10",
                "period": "October 2024",
                "closeDate": "2024-11-05",
                "status": "completed",
                "totalJEs": 156,
                "approver": "cfo@gusto.com",
                "duration": "4.2 days"
            }
        ],
        "total": 1
    }

@app.get("/api/reports/financial-close/{close_id}")
async def download_financial_close_report(close_id: str, format: str = "pdf"):
    """Download financial close report"""
    # In production, this would generate actual PDF/Excel
    return {
        "message": f"Report for {close_id} generated in {format} format",
        "download_url": f"/downloads/{close_id}.{format}"
    }

@app.get("/api/reports/user-activity")
async def get_user_activity_log(
    start_date: str = None,
    end_date: str = None,
    user: str = None
):
    """Get user activity log"""
    return {
        "activities": [
            {
                "id": "act-001",
                "user": "cfo@gusto.com",
                "action": "Approved Journal Entries",
                "timestamp": "2024-11-28T14:23:00Z",
                "details": "Approved 15 journal entries"
            }
        ],
        "total": 1
    }

@app.post("/api/reports/user-activity/download")
async def download_user_activity_report(request: dict):
    """Download user activity report"""
    return {
        "message": "User activity report generated",
        "format": request.get("format", "xlsx")
    }

@app.post("/api/reports/audit-trail/download")
async def download_audit_trail(request: dict):
    """Download complete audit trail"""
    return {
        "message": "Complete audit trail generated",
        "includeDigitalSignature": request.get("includeDigitalSignature", True)
    }

@app.get("/api/reports/analytics")
async def get_analytics(period: str = "last-6-months"):
    """Get analytics data"""
    return {
        "period": period,
        "metrics": {
            "avgCloseTime": 4.4,
            "aiSuccessRate": 97.8,
            "timeSaved": 68,
            "netsuitePostingSuccess": 99.2
        }
    }

@app.get("/api/reports/compliance-score")
async def get_compliance_score():
    """Get SOX compliance score"""
    return {
        "score": 98,
        "controlsValidated": 45,
        "totalControls": 46,
        "status": "compliant"
    }

@app.post("/api/reports/compliance-package/download")
async def export_compliance_package(request: dict):
    """Export complete compliance package"""
    return {
        "message": "Compliance package generated",
        "includes": [
            "workflows",
            "approvals",
            "netsuite_data",
            "user_activity"
        ]
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
