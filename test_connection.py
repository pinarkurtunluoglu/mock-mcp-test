"""Quick test script to verify Dataverse connection directly."""

import msal
import httpx
from dotenv import load_dotenv
import os

load_dotenv()

DATAVERSE_URL = os.getenv("DATAVERSE_URL", "").rstrip("/")
CLIENT_ID = os.getenv("CLIENT_ID", "")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "")
TENANT_ID = os.getenv("TENANT_ID", "")
ENTITY_SET = os.getenv("ENTITY_SET_NAME", "mserp_tryaiinventoryagingreportentities")

print("=" * 50)
print("DATAVERSE CONNECTION TEST")
print("=" * 50)
print(f"URL:       {DATAVERSE_URL}")
print(f"Client ID: {CLIENT_ID[:8]}..." if CLIENT_ID else "Client ID: NOT SET!")
print(f"Tenant ID: {TENANT_ID[:8]}..." if TENANT_ID else "Tenant ID: NOT SET!")
print(f"Secret:    {'***set***' if CLIENT_SECRET else 'NOT SET!'}")
print(f"Entity:    {ENTITY_SET}")
print()

# Step 1: Get token
print("[1] Acquiring token from Azure AD...")
authority = f"https://login.microsoftonline.com/{TENANT_ID}"
scope = [f"{DATAVERSE_URL}/.default"]
print(f"    Authority: {authority}")
print(f"    Scope:     {scope}")

app = msal.ConfidentialClientApplication(
    CLIENT_ID, authority=authority, client_credential=CLIENT_SECRET,
)
result = app.acquire_token_for_client(scopes=scope)

if "access_token" in result:
    token = result["access_token"]
    print(f"    [SUCCESS] Token acquired! (expires in {result.get('expires_in')}s)")
    print(f"    Token preview: {token[:30]}...")
else:
    print(f"    [FAILED] TOKEN FAILED!")
    print(f"    Error: {result.get('error')}")
    print(f"    Description: {result.get('error_description')}")
    exit(1)

# Step 2: Query entity with pagination
print()
print(f"[2] Querying {ENTITY_SET} with PAGINATION (top 1000)...")
# We'll fetch 1000 records to test the pagination logic
url = f"{ENTITY_SET}?$top=1000"
print(f"    URL: {url}")

async def test_pagination():
    from dataverse_mcp.client import DataverseClient
    from dataverse_mcp.config import get_settings
    s = get_settings()
    c = DataverseClient(s.dataverse_url, s.client_id, s.client_secret, s.tenant_id)
    
    print("    Fetching records...")
    records = await c.query_table(ENTITY_SET, fetch_all=True, max_records=1000)
    print(f"    [SUCCESS] Fetched {len(records)} records using pagination.")
    await c.close()
    return records

import asyncio
records = asyncio.run(test_pagination())

if response.status_code == 200:
    data = response.json()
    records = data.get("value", [])
    print(f"    [SUCCESS] SUCCESS! Got {len(records)} records sorted by Header Report Date (DESC)")
    
    print("\n    TOP 10 RECORDS SUMMARY:")
    print("    " + "-"*110)
    print(f"    {'#':<3} | {'Report Date':<20} | {'Item Name':<40} | {'Qty':<10} | {'City/State':<15}")
    print("    " + "-"*110)
    
    for idx, rec in enumerate(records, 1):
        date = rec.get("mserp_headerreportdate", "N/A")
        name = rec.get("mserp_itemname", "N/A")
        qty = rec.get("mserp_qty", "N/A")
        city = rec.get("mserp_deliverycitysate", "N/A")
        
        # Safe printing for Windows console
        line = f"    {idx:<3} | {str(date):<20} | {str(name):<40} | {str(qty):<10} | {str(city):<15}"
        print(line.encode("ascii", errors="replace").decode("ascii"))
    
    print("    " + "-"*110)
else:
    print(f"    [FAILED] REQUEST FAILED!")
    print(f"    Response: {response.text[:500]}")
