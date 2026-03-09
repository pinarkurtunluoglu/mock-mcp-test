"""Verification script for Dataverse Server-Side Aggregation ($apply)."""

import asyncio
from dotenv import load_dotenv
from dataverse_mcp.client import DataverseClient
from dataverse_mcp.config import get_settings
import time

async def verify_server_side_aggregation():
    load_dotenv()
    settings = get_settings()
    
    print("=" * 60)
    print("VERIFYING SERVER-SIDE AGGREGATION")
    print("=" * 60)
    
    client = DataverseClient(
        settings.dataverse_url, 
        settings.client_id, 
        settings.client_secret, 
        settings.tenant_id
    )
    
    entity_set = settings.entity_set_name
    test_field = "mserp_qty"
    
    try:
        print(f"\nTesting Server-Side Aggregation for '{test_field}' on '{entity_set}'...")
        
        # Test SUM
        print("\n[1] Testing sum...")
        start_time = time.time()
        sum_result = await client.aggregate_table(entity_set, test_field, "sum")
        duration = time.time() - start_time
        print(f"    Raw Result: {sum_result}")
        print(f"    Duration: {duration:.2f} seconds")
        
        # Test AVERAGE
        print("\n[2] Testing average...")
        start_time = time.time()
        avg_result = await client.aggregate_table(entity_set, test_field, "avg")
        duration = time.time() - start_time
        print(f"    Raw Result: {avg_result}")
        print(f"    Duration: {duration:.2f} seconds")

    except Exception as e:
        print(f"\n[ERROR] Verification failed. Note that some Virtual Entities in Dynamics 365 do not support $apply.")
        print(f"    Details: {e}")
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(verify_server_side_aggregation())
