from agents.equity_agent import run_equity_agent

def run_equity(payload):
    return run_equity_agent(
        period=payload.period,
        business_day=payload.business_day,
        tasks=[t.dict() for t in payload.tasks]
    )
