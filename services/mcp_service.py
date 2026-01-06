import asyncio
from agents.netsuite_push_agent import process_equity_agent_sheet

EXCEL_PATH = "data/config_mapping.xlsx"

def push_to_netsuite():
    """
    Push Equity JEs via MCP
    """
    asyncio.run(process_equity_agent_sheet(EXCEL_PATH))

    return {
        "success": True,
        "message": "Journal Entries posted to NetSuite"
    }
