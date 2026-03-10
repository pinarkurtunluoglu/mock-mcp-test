"""FastMCP Server — Dataverse Inventory Aging Report.

Provides specialized MCP tools for the mserp_tryaiinventoryagingreportentities entity
from the Tiryaki Operations Dataverse environment.
"""

from __future__ import annotations

import os
import structlog
from fastmcp import FastMCP

# ── Auth Disable ───────────────────────────────────────
os.environ.pop("FASTMCP_AUTH_TOKEN", None)

from dataverse_mcp.config import get_settings
from dataverse_mcp.services.formatter import DataFormatter
from dataverse_mcp.services.summarizer import DataSummarizer
from dataverse_mcp.client import DataverseClient

logger = structlog.get_logger(__name__)

# ── Settings & Dependencies ─────────────────────────────
settings = get_settings()

ENTITY_SET = settings.entity_set_name
ENTITY_LOGICAL = settings.entity_logical_name

mcp = FastMCP(
    name=settings.mcp_server_name,
    instructions=(
        "This is an MCP server for querying Inventory Aging Report data from Microsoft Dataverse. "
        "It provides tools to list, query, filter, search, and summarize records from the "
        f"'{ENTITY_SET}' entity. Use these tools to analyze inventory aging data.\n\n"
        "IMPORTANT FIELD NOTES:\n"
        "- DATE FIELD: Always use 'mserp_headerreportdate' for date filtering. "
        "'mserp_reportdate' is mostly NULL and should be ignored.\n"
        "- PRODUCT NAME: 'mserp_itemname' (full name), 'mserp_itemid' (code like 10IQ4112)\n"
        "- PRODUCT CATEGORY: 'mserp_etgproductlevel03' (e.g. 'WHEAT', 'CORN')\n"
        "- QUANTITY: 'mserp_qty' (inventory quantity)\n"
        "- AGING: 'mserp_purchfifo' (purchase FIFO days), 'mserp_purchlifo' (LIFO days)\n"
        "- SITE/WAREHOUSE: 'mserp_inventsiteid' (site), 'mserp_inventlocationid' (warehouse)\n"
        "- COMPANY: 'mserp_companyid' (code), 'mserp_companyname' (full name)\n"
        "- ORIGIN: 'mserp_inventcolorid' (country of origin like RUS, TR)\n\n"
        "ANALYSIS RULES:\n"
        "- For TOTALS/SUMS/COUNTS/INSIGHTS, ALWAYS use 'calculate_inventory_totals', NEVER 'query_inventory_aging' or 'summarize_inventory_aging'.\n"
        "- For INSIGHTS and TRENDS, call 'calculate_inventory_totals' MULTIPLE TIMES with different group_by values "
        "to analyze the FULL dataset from multiple dimensions. Example sequence:\n"
        "  1. group_by='mserp_companyid' → company breakdown\n"
        "  2. group_by='mserp_inventsiteid' → site breakdown\n"
        "  3. group_by='mserp_etgproductlevel03' → product category breakdown\n"
        "  4. group_by='mserp_headerreportdate' → time-based trend\n"
        "- For cross-dimensional analysis, use filter_query to fix one dimension then group_by the other. "
        "Example: filter_query=\"mserp_companyid eq 'mesq'\" + group_by='mserp_etgproductlevel03'.\n"
        "- IMPORTANT: group_by only supports ONE column at a time. NEVER pass multiple columns.\n"
        "- Combine results from multiple calls to produce comprehensive insights.\n"
        "- 'summarize_inventory_aging' only analyzes a SAMPLE (max 5000 rows), NOT the full dataset. "
        "Use it ONLY for showing example records, NEVER for calculating totals or insights."
    ),
)

# Initialize components
client = DataverseClient(
    dataverse_url=settings.dataverse_url,
    client_id=settings.client_id,
    client_secret=settings.client_secret,
    tenant_id=settings.tenant_id,
)
summarizer = DataSummarizer(max_tokens=settings.summary_max_tokens)
formatter = DataFormatter()


# ═══════════════════════════════════════════════════════════
# MCP TOOLS — Inventory Aging Report
# ═══════════════════════════════════════════════════════════


@mcp.tool()
async def get_inventory_aging_schema() -> str:
    """Returns the schema (columns and data types) of the Inventory Aging Report table.
    Use this first to understand what fields are available before querying data."""
    try:
        schema = await client.get_table_schema(ENTITY_LOGICAL)
        return formatter.format_schema(schema)
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def get_inventory_aging_count() -> str:
    """Returns the total number of records in the Inventory Aging Report table."""
    try:
        count = await client.get_record_count(ENTITY_SET)
        return f"Total records in Inventory Aging Report: **{count:,}**"
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def query_inventory_aging(
    select: str = "",
    filter_query: str = "",
    orderby: str = "",
    top: int = 50,
) -> str:
    """Queries the Inventory Aging Report table and returns raw records.
    
    WARNING: DO NOT use this tool to calculate totals or averages. It only returns a maximum of 500 records. 
    If you need to calculate a total, sum, or average across many records, you MUST use the `calculate_inventory_totals` tool instead.

    Args:
        select: Comma-separated column names to return (e.g. 'mserp_itemid,mserp_qty,mserp_amountmst').
                Leave empty for all columns.
        filter_query: OData $filter expression (e.g. "mserp_itemname eq 'BUĞDAY TOHUMU - EKMEKLİK KRASUNIA ODESKA'").
        orderby: OData $orderby expression (e.g. "mserp_amountmst desc").
        top: Maximum number of raw records to return (default: 50, max: 500).
    """
    try:
        if top > 500:
            top = 500
        
        # Get total count to inform AI about data completeness
        try:
            total_count = await client.get_record_count(ENTITY_SET)
        except Exception:
            total_count = None
        
        records = await client.query_table(
            ENTITY_SET,
            select=select or None,
            filter_query=filter_query or None,
            orderby=orderby or None,
            top=top,
        )
        columns = [c.strip() for c in select.split(",")] if select else None
        result = formatter.format_records_table(records, columns=columns)
        
        # Build response with completeness warning
        header = f"**Inventory Aging Report** — Showing {len(records)} records"
        if total_count and total_count > len(records):
            header += f" out of {total_count:,} total"
            header += f"\n\n> **Bu tabloda {total_count:,} kayıt var. Tamamını gösteremiyorum.** "
            header += "Toplam, ortalama gibi hesaplamalar için `calculate_inventory_totals` tool'unu kullanın."
        
        return f"{header}\n\n{result}"
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def search_inventory_aging(
    search_field: str,
    search_term: str,
    select: str = "",
    top: int = 20,
) -> str:
    """Searches the Inventory Aging Report for records matching a specific value in a field.

    Args:
        search_field: The column name to search in (e.g. 'mserp_itemid', 'mserp_name').
        search_term: The value to search for (case-insensitive contains search).
        select: Comma-separated column names to return. Leave empty for all columns.
        top: Maximum number of results (default: 20).
    """
    try:
        records = await client.search_records(
            ENTITY_SET, search_field=search_field, search_term=search_term,
            select=select or None, top=top,
        )
        result = formatter.format_records_table(records)
        return f"**Search Results** — Found {len(records)} records where '{search_field}' contains '{search_term}'\n\n{result}"
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def get_inventory_aging_record(record_id: str, select: str = "") -> str:
    """Retrieves a single Inventory Aging Report record by its unique ID (GUID).

    Args:
        record_id: The GUID of the record to retrieve.
        select: Comma-separated column names to return. Leave empty for all columns.
    """
    try:
        record = await client.get_record(
            ENTITY_SET, record_id, select=select or None,
        )
        return formatter.format_record(record, table_name="Inventory Aging Report")
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def summarize_inventory_aging(
    select: str = "mserp_qty,mserp_itemname,mserp_headerreportdate",
    filter_query: str = "",
    top: int = 2000,
    sample_size: int = 5,
) -> str:
    """Generates a deep statistical summary of the Inventory Aging Report data.
    Fetch up to 2000 records (default) to provide a representative view of the dataset.

    Args:
        select: Comma-separated key fields to compute statistics on (e.g. 'mserp_qty,mserp_itemname').
        filter_query: OData $filter to narrow the data before summarizing.
        top: Number of records to include in the analysis (default: 2000, max: 5000).
        sample_size: Number of sample records to show (default: 5).
    """
    try:
        # Cap at 5000 for safety but allow deep analysis
        actual_top = min(top, 5000)
        
        records = await client.query_table(
            ENTITY_SET, 
            select=select or None, 
            filter_query=filter_query or None, 
            fetch_all=True,
            max_records=actual_top
        )
        
        key_fields = [c.strip() for c in select.split(",")] if select else None
        return summarizer.summarize_records(
            records, table_name="Inventory Aging Report",
            sample_size=sample_size, key_fields=key_fields,
        )
    except Exception as e:
        return f"Error during large-scale summary: {e}"


@mcp.tool()
async def calculate_inventory_totals(
    numeric_field: str = "",
    agg_type: str = "sum",
    group_by: str = "",
    filter_query: str = "",
) -> str:
    """Calculates totals, averages, min, max, or COUNT across the ENTIRE dataset (500k+ records).
    Uses Dataverse server-side aggregation (Query Pushdown) — no rows are downloaded.

    CRITICAL: ALWAYS use this tool for insights, trends, and analysis. Call it MULTIPLE TIMES
    with different group_by values to analyze different dimensions.
    NEVER use 'summarize_inventory_aging' for insights — it only sees a tiny sample.

    Args:
        numeric_field: The numeric column to aggregate (e.g. 'mserp_qty', 'mserp_purchfifo'). Leave empty for 'count'.
        agg_type: Aggregation type: 'sum', 'average', 'min', 'max', or 'count'.
        group_by: ONE column to group results by (only single column supported).
                  Common values: 'mserp_inventsiteid', 'mserp_companyid', 'mserp_etgproductlevel03',
                  'mserp_headerreportdate', 'mserp_inventcolorid', 'mserp_itemname'.
                  For cross-dimensional analysis, use filter_query to fix one dimension and group_by the other.
        filter_query: (Optional) OData $filter to narrow data before aggregating
                      (e.g. "mserp_etgproductlevel03 eq 'WHEAT'" or "contains(mserp_itemname,'BUGDAY')").
    """
    try:
        result = await client.aggregate_table(
            ENTITY_SET, numeric_field, agg_type,
            filter_query=filter_query, group_by=group_by,
        )
        
        # Determine alias based on agg type
        if agg_type.lower() == "count":
            alias = "record_count"
            label = "Record Count"
        else:
            alias = f"{numeric_field}_{agg_type}"
            label = f"{numeric_field} ({agg_type})"
        
        header = f"### Server-Side Aggregation (Query Pushdown — no rows downloaded)"
        
        if group_by and isinstance(result, list):
            # Grouped results → format as table (supports multi-column group_by)
            group_cols = [c.strip() for c in group_by.split(",")]
            col_headers = " | ".join(group_cols) + f" | {label}"
            col_seps = " | ".join(["---"] * len(group_cols)) + " | ---"
            lines = [header, f"\n| {col_headers} |", f"| {col_seps} |"]
            for row in result:
                group_vals = " | ".join(str(row.get(c, "N/A")) for c in group_cols)
                agg_val = row.get(alias, 0)
                lines.append(f"| {group_vals} | {agg_val:,.2f} |")
            return "\n".join(lines)
        else:
            # Single result
            value = result.get(alias, 0) if isinstance(result, dict) else 0
            detail = f"- **{label}**: `{value:,.2f}`"
            if filter_query:
                detail += f"\n- Filter: `{filter_query}`"
            return f"{header}\n{detail}"
    except Exception as e:
        return f"Error: Server-side aggregation failed. {e}"


# ═══════════════════════════════════════════════════════════
# MCP RESOURCES
# ═══════════════════════════════════════════════════════════


@mcp.resource("dataverse://inventory-aging/schema")
async def resource_inventory_aging_schema() -> str:
    """Schema of the Inventory Aging Report entity."""
    schema = await client.get_table_schema(ENTITY_LOGICAL)
    return formatter.format_schema(schema)


# ═══════════════════════════════════════════════════════════
# MCP PROMPTS
# ═══════════════════════════════════════════════════════════


@mcp.prompt()
def analyze_inventory_aging(analysis_goal: str = "genel analiz") -> str:
    """Prompt template for analyzing inventory aging data."""
    return f"""Lütfen Envanter Yaşlandırma Raporu (Inventory Aging Report) verilerini analiz edin.

Analiz Hedefi: {analysis_goal}

Adımlar:
1. Önce `get_inventory_aging_schema` ile tablo yapısını inceleyin.
2. `get_inventory_aging_count` ile toplam kayıt sayısını öğrenin.
3. `summarize_inventory_aging` ile istatistiksel özet alın.
4. Gerekirse `query_inventory_aging` ile detaylı veri çekin.
5. Bulgularınızı ve önerilerinizi özetleyin.

Yanıtlarınızı Türkçe verin."""


@mcp.prompt()
def filter_aging_items(min_days: str = "90", field_name: str = "mserp_qty") -> str:
    """Prompt template for filtering aged inventory items."""
    return f"""Envanter Yaşlandırma Raporundan {min_days} günden eski kalemleri analiz edin.

Adımlar:
1. `get_inventory_aging_schema` ile hangi alanların mevcut olduğunu kontrol edin.
2. `query_inventory_aging` ile uygun filtreler kullanarak verileri çekin.
3. `{field_name}` alanına göre sıralayarak en kritik kalemleri belirleyin.
4. Sonuçları ve tavsiyeleri özetleyin.

Yanıtlarınızı Türkçe verin."""
