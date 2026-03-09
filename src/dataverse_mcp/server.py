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
        f"'{ENTITY_SET}' entity. Use these tools to analyze inventory aging data."
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
    """Queries the Inventory Aging Report table with optional OData filters.

    Args:
        select: Comma-separated column names to return (e.g. 'mserp_itemid,mserp_quantity,mserp_amount').
                Leave empty for all columns.
        filter_query: OData $filter expression (e.g. "mserp_quantity gt 100").
        orderby: OData $orderby expression (e.g. "mserp_amount desc").
        top: Maximum number of records to return (default: 50, max: 500).
    """
    try:
        if top > 500:
            top = 500
        records = await client.query_table(
            ENTITY_SET,
            select=select or None,
            filter_query=filter_query or None,
            orderby=orderby or None,
            top=top,
        )
        columns = [c.strip() for c in select.split(",")] if select else None
        result = formatter.format_records_table(records, columns=columns)
        return f"**Inventory Aging Report** — {len(records)} records returned\n\n{result}"
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
    select: str = "",
    filter_query: str = "",
    top: int = 200,
    sample_size: int = 5,
) -> str:
    """Generates a statistical summary of the Inventory Aging Report data.
    Includes field distributions, numeric stats (min/max/avg), and sample records.

    Args:
        select: Comma-separated key fields to compute statistics on.
        filter_query: OData $filter to narrow the data before summarizing.
        top: Number of records to include in the analysis (default: 200).
        sample_size: Number of sample records to show (default: 5).
    """
    try:
        records = await client.query_table(
            ENTITY_SET, select=select or None, filter_query=filter_query or None, top=top,
        )
        key_fields = [c.strip() for c in select.split(",")] if select else None
        return summarizer.summarize_records(
            records, table_name="Inventory Aging Report",
            sample_size=sample_size, key_fields=key_fields,
        )
    except Exception as e:
        return f"Error: {e}"


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
def filter_aging_items(min_days: str = "90", field_name: str = "mserp_quantity") -> str:
    """Prompt template for filtering aged inventory items."""
    return f"""Envanter Yaşlandırma Raporundan {min_days} günden eski kalemleri analiz edin.

Adımlar:
1. `get_inventory_aging_schema` ile hangi alanların mevcut olduğunu kontrol edin.
2. `query_inventory_aging` ile uygun filtreler kullanarak verileri çekin.
3. `{field_name}` alanına göre sıralayarak en kritik kalemleri belirleyin.
4. Sonuçları ve tavsiyeleri özetleyin.

Yanıtlarınızı Türkçe verin."""
