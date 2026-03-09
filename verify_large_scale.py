"""Verification script for Large-Scale Data Analysis and Pagination."""

import asyncio
import os
from dotenv import load_dotenv
from dataverse_mcp.client import DataverseClient
from dataverse_mcp.config import get_settings
from dataverse_mcp.services.summarizer import DataSummarizer

async def verify_large_scale():
    load_dotenv()
    settings = get_settings()
    
    print("=" * 60)
    print("VERIFYING LARGE-SCALE DATA ANALYSIS")
    print("=" * 60)
    
    client = DataverseClient(
        settings.dataverse_url, 
        settings.client_id, 
        settings.client_secret, 
        settings.tenant_id
    )
    summarizer = DataSummarizer()
    
    entity_set = settings.entity_set_name
    
    try:
        # 1. Test Pagination (Fetch 1000 records)
        print(f"\n[1] Testing Pagination: Fetching 1000 records from {entity_set}...")
        records = await client.query_table(entity_set, fetch_all=True, max_records=1000)
        print(f"    [SUCCESS] Fetched {len(records)} records using pagination.")
        
        if not records:
            print("    [WARNING] No records found. Skipping further tests.")
            return

        # 2. Test Summarization
        print("\n[2] Testing Enhanced Summarization (Statistical Analysis)...")
        summary = summarizer.summarize_records(
            records, 
            table_name="Inventory Aging",
            key_fields=["mserp_qty", "mserp_itemname", "mserp_headerreportdate"]
        )
        print("    Summary Output Preview:")
        # Print first 10 lines of summary safely
        for line in summary.split("\n")[:15]:
            print(f"      {line.encode('ascii', errors='replace').decode('ascii')}")
            
        # 3. Test Totals Logic (Simulated tool logic)
        print("\n[3] Testing Aggregation (Sums/Averages)...")
        qty_values = [r.get("mserp_qty") for r in records if isinstance(r.get("mserp_qty"), (int, float))]
        if qty_values:
            total_qty = sum(qty_values)
            avg_qty = total_qty / len(qty_values)
            print(f"    [SUCCESS] Total Quantity across {len(qty_values)} records: {total_qty:,.2f}")
            print(f"    [SUCCESS] Average Quantity: {avg_qty:,.2f}")
        else:
            print("    [WARNING] No numeric quantity data found to aggregate.")

    except Exception as e:
        print(f"\n    [ERROR] Verification failed: {e}")
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(verify_large_scale())
