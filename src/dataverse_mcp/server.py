"""FastMCP Server — Exposes Microsoft Dataverse data to AI Agents.

Defines 6 Tools, 2 Resources, and 2 Prompts.
All data is processed and summarized on the Python side before delivery.
"""

from __future__ import annotations

import os
import structlog
from fastmcp import FastMCP

from dataverse_mcp.config import get_settings
from dataverse_mcp.dataverse_client import DataverseError
from dataverse_mcp.services.formatter import DataFormatter
from dataverse_mcp.services.summarizer import DataSummarizer

logger = structlog.get_logger(__name__)

# ── Settings & Dependencies ─────────────────────────────
settings = get_settings()

mcp = FastMCP(
    name=settings.mcp_server_name,
    instructions=(
        "This MCP server provides access to Microsoft Dataverse data. "
        "You can perform querying, searching, statistics, and summarization "
        "operations on tables. Large datasets are automatically "
        "summarized to ensure efficient context window usage."
    ),
)

# Mock mode: Forced to True for this dedicated mock-test version
_use_mock = True

if _use_mock:
    from dataverse_mcp.mock_client import MockDataverseClient
    client = MockDataverseClient()
else:
    from dataverse_mcp.auth import DataverseAuth
    from dataverse_mcp.dataverse_client import DataverseClient
    auth = DataverseAuth(settings)
    client = DataverseClient(settings, auth)  # type: ignore[assignment]

summarizer = DataSummarizer(max_tokens=settings.summary_max_tokens)
formatter = DataFormatter()


def _get_allowed_list() -> list[str]:
    """Returns the list of allowed tables (whitelist)."""
    if not settings.allowed_tables:
        return []
    return [t.strip() for t in settings.allowed_tables.split(",")]


def _is_allowed(table_name_or_set: str) -> bool:
    """Checks if the table is accessible based on the whitelist."""
    allowed = _get_allowed_list()
    if not allowed:
        return True
    return table_name_or_set in allowed


# ═══════════════════════════════════════════════════════════
# MCP TOOLS
# ═══════════════════════════════════════════════════════════


@mcp.tool()
async def list_tables() -> str:
    """Lists all available tables in Dataverse.

    Returns logical names, display names, and entity set names for all entities.
    Use this to discover which tables are available for querying.
    """
    try:
        entities = await client.list_tables()
        allowed = _get_allowed_list()
        if allowed:
            # Check against both logical name and entity set name
            entities = [
                e for e in entities
                if e.get("LogicalName") in allowed or e.get("EntitySetName") in allowed
            ]
        return formatter.format_table_list(entities)
    except DataverseError as e:
        return f" Error: {e}"


@mcp.tool()
async def query_table(
    entity_set: str,
    select: str = "",
    filter_query: str = "",
    orderby: str = "",
    top: int = 20,
) -> str:
    """Fetches and formats data from a specific table using OData queries.

    Args:
        entity_set: Table entity set name (e.g., 'accounts', 'contacts')
        select: Columns to retrieve, comma-separated (e.g., 'name,revenue')
        filter_query: OData filter expression (e.g., "revenue gt 1000000")
        orderby: Sort order (e.g., 'createdon desc')
        top: Maximum records to fetch (default: 20)
    """
    if not _is_allowed(entity_set):
        return f" Error: You do not have permission to access table '{entity_set}'."
    try:
        records = await client.query_table(
            entity_set,
            select=select or None,
            filter_query=filter_query or None,
            orderby=orderby or None,
            top=top,
        )
        columns = select.split(",") if select else None
        return formatter.format_records_table(records, columns=columns)
    except DataverseError as e:
        return f" Error: {e}"


@mcp.tool()
async def search_records(
    entity_set: str,
    search_field: str,
    search_term: str,
    select: str = "",
    top: int = 20,
) -> str:
    """Searches for records using a keyword in a specified field.

    Args:
        entity_set: Table entity set name (e.g., 'accounts')
        search_field: Column name to search in (e.g., 'name')
        search_term: Search keyword
        select: Columns to retrieve, comma-separated
        top: Maximum results to return (default: 20)
    """
    if not _is_allowed(entity_set):
        return f" Error: You do not have permission to access table '{entity_set}'."
    try:
        records = await client.search_records(
            entity_set, search_field=search_field, search_term=search_term,
            select=select or None, top=top,
        )
        return formatter.format_records_table(records)
    except DataverseError as e:
        return f" Error: {e}"


@mcp.tool()
async def get_record(
    entity_set: str,
    record_id: str,
    select: str = "",
    expand: str = "",
) -> str:
    """Retrieves a single record by its GUID.

    Args:
        entity_set: Table entity set name (e.g., 'accounts')
        record_id: Record GUID (e.g., '00000000-0000-0000-0000-000000000000')
        select: Columns to retrieve, comma-separated
        expand: Related tables to expand ($expand)
    """
    if not _is_allowed(entity_set):
        return f" Error: You do not have permission to access table '{entity_set}'."
    try:
        record = await client.get_record(
            entity_set, record_id, select=select or None, expand=expand or None,
        )
        return formatter.format_record(record, table_name=entity_set)
    except DataverseError as e:
        return f" Error: {e}"


@mcp.tool()
async def get_table_stats(entity_set: str, table_logical_name: str) -> str:
    """Returns table statistics: record count and schema information.

    Args:
        entity_set: Table entity set name (e.g., 'accounts')
        table_logical_name: Table logical name (e.g., 'account')
    """
    if not _is_allowed(entity_set) and not _is_allowed(table_logical_name):
        return f" Error: You do not have permission to access table '{entity_set}'."
    try:
        count = await client.get_record_count(entity_set)
        schema = await client.get_table_schema(table_logical_name)
        return summarizer.summarize_table_stats(count, table_logical_name, schema=schema)
    except DataverseError as e:
        return f" Error: {e}"


@mcp.tool()
async def summarize_table(
    entity_set: str,
    select: str = "",
    filter_query: str = "",
    top: int = 100,
    sample_size: int = 5,
) -> str:
    """Generates an AI-friendly summary of a table by processing records.

    Summarizes large datasets into statistics, field distributions, and
    sample records. Optimizes context window usage.

    Args:
        entity_set: Table entity set name (e.g., 'accounts')
        select: Columns to summarize, comma-separated
        filter_query: OData filter expression
        top: Maximum records to process (default: 100)
        sample_size: Number of sample records to show (default: 5)
    """
    if not _is_allowed(entity_set):
        return f" Error: You do not have permission to access table '{entity_set}'."
    try:
        records = await client.query_table(
            entity_set, select=select or None, filter_query=filter_query or None, top=top,
        )
        key_fields = select.split(",") if select else None
        return summarizer.summarize_records(
            records, table_name=entity_set, sample_size=sample_size, key_fields=key_fields,
        )
    except DataverseError as e:
        return f" Error: {e}"


# ═══════════════════════════════════════════════════════════
# MCP RESOURCES
# ═══════════════════════════════════════════════════════════


@mcp.resource("dataverse://tables")
async def resource_tables() -> str:
    """List of all available tables in Dataverse."""
    entities = await client.list_tables()
    allowed = _get_allowed_list()
    if allowed:
        entities = [
            e for e in entities
            if e.get("LogicalName") in allowed or e.get("EntitySetName") in allowed
        ]
    return formatter.format_table_list(entities)


@mcp.resource("dataverse://schema/{table_name}")
async def resource_schema(table_name: str) -> str:
    """Schema information for a specific table."""
    if not _is_allowed(table_name):
        return f" Error: You do not have permission to access schema for '{table_name}'."
    schema = await client.get_table_schema(table_name)
    return formatter.format_schema(schema)


# ═══════════════════════════════════════════════════════════
# MCP PROMPTS
# ═══════════════════════════════════════════════════════════


@mcp.prompt()
def analyze_data(table_name: str, analysis_goal: str = "general analysis") -> str:
    """Structured data analysis on a Dataverse table.

    Args:
        table_name: Table name to analyze
        analysis_goal: Goal of the analysis (e.g., 'sales trend', 'customer segmentation')
    """
    return f"""Please analyze the '{table_name}' table.

Analysis Goal: {analysis_goal}

Steps:
1. First, check available tables using `list_tables`.
2. Use `get_table_stats` to understand the general state of the table.
3. Use `summarize_table` to get a data summary.
4. If necessary, fetch detailed data with `query_table`.
5. Summarize your findings and recommendations.

Provide your response in English and offer insights relevant to business decisions."""


@mcp.prompt()
def compare_records(table_name: str, record_id_1: str, record_id_2: str) -> str:
    """Compares two records to analyze differences and similarities.

    Args:
        table_name: Table entity set name
        record_id_1: First record GUID
        record_id_2: Second record GUID
    """
    return f"""Please compare these two records in the '{table_name}' table:

Record 1: {record_id_1}
Record 2: {record_id_2}

Steps:
1. Fetch both records using `get_record`.
2. Compare fields: which are the same, which are different?
3. Present differences in a table.
4. Comment on key differences.

Provide your response in English."""
