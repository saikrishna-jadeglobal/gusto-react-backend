# workflows/payroll_workflow.py
from agents.payroll_recon import run_payroll_agent

def run_payroll_workflow():
    return run_payroll_agent(period="Current Month")
