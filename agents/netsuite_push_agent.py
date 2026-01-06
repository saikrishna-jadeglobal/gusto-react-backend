"""
NetSuite Journal Entry Push Agent - Stdio Transport (WORKING VERSION)

This version uses stdio (process communication) transport which is the 
standard way to connect to FastMCP servers.

Requirements:
    pip install mcp pandas openpyxl --break-system-packages
"""

import os
import json
import asyncio
import sys
import pandas as pd  # type: ignore
from openpyxl import load_workbook  # type: ignore
from typing import Dict, Any, Tuple

# MCP imports for stdio transport
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

# =======================
# CONFIGURATION
# =======================

# Paths
EXCEL_FILE_PATH = "config_mapping.xlsx"
ACCRUALS_OUTPUT_FOLDER = "Accruals Output"

# Equity Sheet Config
SHEET_NAME = "Equity_Agent_Database"
COL_APPROVAL = "Approval status"
COL_PAYLOAD_US = "Payload_US"
COL_PAYLOAD_CAD = "Payload_CAD"
COL_PUSH_STATUS = "Netsuit Pushed status"
COL_OUTPUT = "Output response"

# MCP Server Configuration
# Use absolute path to the mcp-server directory
import os
# Get the absolute path to this file's directory (Agent/)
AGENT_DIR = os.path.dirname(os.path.abspath(__file__))
# Go up one level to project root, then into mcp-server/
PROJECT_ROOT = os.path.dirname(AGENT_DIR)
MCP_SERVER_DIR = os.path.join(PROJECT_ROOT, "mcp-server")
MCP_SERVER_SCRIPT = os.path.join(MCP_SERVER_DIR, "mcp_server_stdio_wrapper.py")

# Debug: Print paths (will go to stderr)
import sys
print(f"[DEBUG] Agent Dir: {AGENT_DIR}", file=sys.stderr)
print(f"[DEBUG] Project Root: {PROJECT_ROOT}", file=sys.stderr)
print(f"[DEBUG] MCP Server Script: {MCP_SERVER_SCRIPT}", file=sys.stderr)
print(f"[DEBUG] MCP Server Script Exists: {os.path.exists(MCP_SERVER_SCRIPT)}", file=sys.stderr)


# =======================
# MCP STDIO CLIENT
# =======================

class NetSuiteMCPClient:
    """Stdio-based MCP Client for NetSuite MCP Server"""
    
    def __init__(self, server_script: str):
        self.server_script = server_script
        self.python_exe = sys.executable
        self.session: ClientSession = None
        self.stdio_context = None
    
    async def connect(self):
        """Connect to the MCP server via stdio"""
        if self.session:
            return  # Already connected
        
        # Check if server script exists
        if not os.path.exists(self.server_script):
            raise FileNotFoundError(
                f"MCP server script not found: {self.server_script}\n"
                f"Current directory: {os.getcwd()}\n"
                f"Full path checked: {os.path.abspath(self.server_script)}\n"
                f"Please ensure the script is in the correct location."
            )
        
        # Server parameters for stdio transport
        server_params = StdioServerParameters(
            command=self.python_exe,
            args=[self.server_script],
            env=None
        )
        
        print("🔌 Connecting to MCP server...")
        print(f"   Python: {self.python_exe}", file=sys.stderr)
        print(f"   Script: {os.path.abspath(self.server_script)}", file=sys.stderr)
        
        try:
            # Create stdio client with timeout
            self.stdio_context = stdio_client(server_params)
            read, write = await asyncio.wait_for(
                self.stdio_context.__aenter__(), 
                timeout=10.0
            )
            
            # Create session
            self.session = ClientSession(read, write)
            await asyncio.wait_for(
                self.session.__aenter__(),
                timeout=10.0
            )
            
            # Initialize
            await asyncio.wait_for(
                self.session.initialize(),
                timeout=10.0
            )
            print("✅ Connected to NetSuite MCP Server")
            
        except asyncio.TimeoutError:
            print("❌ Timeout connecting to MCP server", file=sys.stderr)
            print("   The server may have failed to start. Check:", file=sys.stderr)
            print(f"   1. Python path is correct: {self.python_exe}", file=sys.stderr)
            print(f"   2. Server script exists: {self.server_script}", file=sys.stderr)
            print("   3. All dependencies installed: pip install mcp", file=sys.stderr)
            raise
        except Exception as e:
            print(f"❌ Failed to connect to MCP server: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            raise
    
    async def disconnect(self):
        """Disconnect from the MCP server"""
        if self.session:
            await self.session.__aexit__(None, None, None)
        if self.stdio_context:
            await self.stdio_context.__aexit__(None, None, None)
        self.session = None
    
    async def create_journal_entry(self, payload: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Create a journal entry via MCP server
        
        Args:
            payload: Dictionary containing JE data with keys:
                - subsidiary: int
                - tran_date: str
                - memo: str
                - lines: list of line items
                - approval_status: str (optional)
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            # Ensure we're connected
            if not self.session:
                await self.connect()
            
            # Call the MCP tool
            result = await self.session.call_tool(
                "create_journal_entry",
                arguments={
                    "subsidiary": payload.get("subsidiary"),
                    "tran_date": payload.get("tran_date"),
                    "memo": payload.get("memo"),
                    "lines": payload.get("lines", []),
                    "approval_status": payload.get("approval_status", "Pending Approval")
                }
            )
            
            # Check if error
            if result.isError:
                error_msg = "Unknown error"
                if result.content:
                    error_msg = result.content[0].text
                return False, f"Error: {error_msg}"
            
            # Extract content from result
            if result.content:
                response_text = result.content[0].text
                try:
                    response_data = json.loads(response_text)
                    if response_data.get("success"):
                        je_id = response_data.get("journal_entry_id") or response_data.get("id")
                        return True, f"SUCCESS | Journal Entry ID: {je_id if je_id else 'N/A'}"
                    else:
                        error_msg = response_data.get("error", "Unknown error")
                        return False, f"FAILED | {error_msg}"
                except json.JSONDecodeError:
                    # Response is plain text
                    return True, f"SUCCESS | Response: {response_text}"
            
            return False, "No response from server"
            
        except Exception as e:
            return False, f"Exception: {str(e)}"


# =======================
# HELPER FUNCTIONS
# =======================

def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Make sure all required columns exist in the dataframe."""
    required_cols = [
        COL_APPROVAL,
        COL_PAYLOAD_US,
        COL_PAYLOAD_CAD,
        COL_PUSH_STATUS,
        COL_OUTPUT,
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = ""
    return df


async def post_payload_to_mcp(
    mcp_client: NetSuiteMCPClient,
    payload: dict,
    label: str
) -> tuple[bool, str]:
    """Send a single JE payload to the MCP server."""
    print(f"   🚀 [{label}] Sending to NetSuite (via MCP)...")
    
    ok, msg = await mcp_client.create_journal_entry(payload)
    return ok, f"[{label}] {msg}"


# =======================
# MODE 1: EQUITY AGENT (NO USER INPUT)
# =======================

async def process_equity_agent_sheet(path: str):
    """
    Reads agent DB, loads JSON payloads from file paths, posts them to MCP, and updates sheet.
    This runs AUTOMATICALLY based on the Excel status (Approved/Fail).
    """
    
    print("\n=== 🦋 MODE 1: EQUITY AGENT PROCESSING ===")
    
    if not os.path.exists(path):
        print(f"❌ Excel file not found: {path}")
        return

    print(f"📂 Loading workbook: {path}")
    df = pd.read_excel(path, sheet_name=SHEET_NAME)
    df = ensure_columns(df)

    processed_count = 0
    
    # Create MCP client
    mcp_client = NetSuiteMCPClient(MCP_SERVER_SCRIPT)
    
    try:
        # Connect to MCP server
        await mcp_client.connect()
        
        for idx, row in df.iterrows():
            approval_raw = str(row[COL_APPROVAL]).strip().lower()
            push_status_raw = str(row[COL_PUSH_STATUS]).strip().lower()

            # Skip if not approved or already processed
            if approval_raw != "approved" or push_status_raw == "pass":
                continue

            processed_count += 1
            print(f"\n🧾 Row {idx + 2}: Approved → Processing...")
            logs = []
            all_ok = True

            for col_name, label in [(COL_PAYLOAD_US, "US"), (COL_PAYLOAD_CAD, "CAD")]:
                cell_value = row.get(col_name, "")

                if pd.isna(cell_value) or str(cell_value).strip() == "":
                    continue

                payload_path = str(cell_value).strip()

                if not os.path.exists(payload_path):
                    err = f"[{label}] ❌ Payload file not found: {payload_path}"
                    print(f"   {err}")
                    logs.append(err)
                    all_ok = False
                    continue

                try:
                    with open(payload_path, "r") as f:
                        payload = json.load(f)
                except Exception as e:
                    err = f"[{label}] ❌ Could not load JSON file: {e}"
                    print(f"   {err}")
                    logs.append(err)
                    all_ok = False
                    continue

                # Send payload
                ok, msg = await post_payload_to_mcp(mcp_client, payload, label)
                print(f"   ↪ {msg}")
                logs.append(msg)
                if not ok:
                    all_ok = False

            # Update row output
            if logs:
                df.at[idx, COL_OUTPUT] = "\n".join(logs)
                df.at[idx, COL_PUSH_STATUS] = "Pass" if all_ok else "Fail"

        if processed_count == 0:
            print("\n✅ No new approved rows to process.")
            return

        # ---- Save results back to Excel ----
        print("\n💾 Updating Excel sheet...")
        wb = load_workbook(path)
        wb.close()
        with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            df.to_excel(writer, sheet_name=SHEET_NAME, index=False)
        print("✅ Done — Status & Responses Updated.")
    
    finally:
        # Disconnect from MCP server
        await mcp_client.disconnect()


# =======================
# MODE 2: ZIP ACCRUAL AGENT (INTERACTIVE)
# =======================

async def process_zip_accrual_push(bd_code: str = "BD1"):
    """
    Automated push for ZIP Accrual payloads.
    Finds and pushes the {BD_CODE}_Payload.json file from Accruals Output folder.
    
    Args:
        bd_code: Business day code (BD1, BD3, BD5, BD7)
    
    This is called from the UI after the admin approves the accrual files.
    """
    
    print(f"\n=== 🚀 ZIP ACCRUAL PUSH AGENT ({bd_code}) ===")
    
    # Construct payload path
    json_filename = f"{bd_code}_Payload.json"
    json_path = os.path.join(ACCRUALS_OUTPUT_FOLDER, json_filename)
    
    print(f"🔎 Looking for payload: {json_path}")

    if not os.path.exists(json_path):
        print(f"❌ Error: Payload file not found for {bd_code}.")
        print(f"   Expected location: {os.path.abspath(json_path)}")
        return

    # Load JSON
    try:
        with open(json_path, "r") as f:
            payload = json.load(f)
        print(f"✅ Loaded {bd_code} Payload ({len(payload.get('lines', []))} lines)")
    except Exception as e:
        print(f"❌ Error reading JSON: {e}")
        return

    # Push to NetSuite via MCP
    mcp_client = NetSuiteMCPClient(MCP_SERVER_SCRIPT)
    
    try:
        await mcp_client.connect()
        ok, msg = await post_payload_to_mcp(mcp_client, payload, f"ZIP-{bd_code}")
        
        print("\n" + "="*50)
        print(msg)
        print("="*50 + "\n")
    
    finally:
        await mcp_client.disconnect()


# =======================
# MODE 2: ZIP ACCRUAL AGENT (INTERACTIVE) - For CLI use
# =======================

async def process_zip_accrual_push_interactive():
    """
    Interactive Mode:
    1. Asks user for BD Code.
    2. Finds the payload file dynamically.
    3. Pushes to NetSuite via MCP.
    """
    
    print("\n=== 🤝 MODE 2: ZIP ACCRUAL PUSH AGENT ===")
    
    # 1. Get Input
    valid_codes = ["BD1", "BD3", "BD5", "BD7"]
    bd_input = input(f"Enter BD Code to Push ({'/'.join(valid_codes)}): ").strip().upper()
    
    if bd_input not in valid_codes:
        print(f"❌ Invalid Code. Please enter one of: {', '.join(valid_codes)}")
        return

    # 2. Construct Dynamic Path
    json_filename = f"{bd_input}_Payload.json"
    json_path = os.path.join(ACCRUALS_OUTPUT_FOLDER, json_filename)
    
    print(f"🔎 Looking for payload: {json_path}")

    if not os.path.exists(json_path):
        print(f"❌ Error: Payload file not found for {bd_input}.")
        print(f"   Expected location: {os.path.abspath(json_path)}")
        return

    # 3. Load JSON
    try:
        with open(json_path, "r") as f:
            payload = json.load(f)
        print(f"✅ Loaded {bd_input} Payload ({len(payload.get('lines', []))} lines)")
    except Exception as e:
        print(f"❌ Error reading JSON: {e}")
        return

    # 4. Confirm Push
    confirm = input(f"⚠️  Ready to push {bd_input} to NetSuite? (yes/no): ").strip().lower()
    if confirm != "yes":
        print("🚫 Operation cancelled.")
        return

    # 5. Push to NetSuite via MCP
    mcp_client = NetSuiteMCPClient(MCP_SERVER_SCRIPT)
    
    try:
        await mcp_client.connect()
        ok, msg = await post_payload_to_mcp(mcp_client, payload, f"ZIP-{bd_input}")
        
        print("\n" + "="*50)
        print(msg)
        print("="*50 + "\n")
    
    finally:
        await mcp_client.disconnect()


# =======================
# MAIN MENU
# =======================

async def push_equity_to_netsuite(excel_path: str):
    print("\nGUSTO POC: JOURNAL ENTRY PUSH AGENT (MCP STDIO VERSION)")
    print("--------------------------------------------------------")
    print("1. Process Equity Agent (Excel Database)")
    print("2. Process Zip Accrual Agent (Manual Push)")
    
    choice = input("\nSelect Mode (1 or 2): ").strip()

    if choice == "1":
        await process_equity_agent_sheet(excel_path)
    elif choice == "2":
        await process_zip_accrual_push_interactive()
    else:
        print("❌ Invalid selection.")


if __name__ == "__main__":
    asyncio.run(push_equity_to_netsuite(EXCEL_FILE_PATH))
