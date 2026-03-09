"""Script to reproduce 400 Bad Request error."""

import asyncio
from dotenv import load_dotenv
from dataverse_mcp.client import DataverseClient
from dataverse_mcp.config import get_settings
import urllib.parse

async def reproduce_error():
    load_dotenv()
    settings = get_settings()
    
    print("=" * 60)
    print("REPRODUCING 400 BAD REQUEST ERROR")
    print("=" * 60)
    
    client = DataverseClient(
        settings.dataverse_url, 
        settings.client_id, 
        settings.client_secret, 
        settings.tenant_id
    )
    
    entity_set = settings.entity_set_name
    
    try:
        # Test 1: Direct filter via aggregate_table
        filter_query = "mserp_itemname eq 'BUĞDAY TOHUMU - EKMEKLİK KRASUNIA ODESKA'"
        print(f"\n[Test 1] Aggregating with filter_query: {filter_query}")
        result = await client.aggregate_table(entity_set, "mserp_qty", "sum", filter_query=filter_query)
        print(f"Result: {result}")
        
    except Exception as e:
        print(f"ERROR: {e}")
        
    try:
        # Test 2: Standard query_table with filter
        filter_query = "mserp_itemname eq 'BUĞDAY TOHUMU - EKMEKLİK KRASUNIA ODESKA'"
        print(f"\n[Test 2] Querying directly with filter_query: {filter_query}")
        result = await client.query_table(entity_set, filter_query=filter_query, select="mserp_itemname,mserp_qty", top=5)
        print(f"Result length: {len(result)}")
        
    except Exception as e:
        print(f"ERROR: {e}")
        
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(reproduce_error())
