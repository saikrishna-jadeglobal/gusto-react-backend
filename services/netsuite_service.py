import asyncio
from agents.netsuite_push_agent import process_equity_agent_sheet

def push_equity_to_netsuite():
    asyncio.run(process_equity_agent_sheet("data/config_mapping.xlsx"))
    return {"netsuite": "success"}
