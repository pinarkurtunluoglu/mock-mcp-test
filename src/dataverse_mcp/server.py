"""FastMCP Server — Hyperion Mining Mock Version.

Defines 6 Tools, 2 Resources, and 2 Prompts.
This version IS HARDCODED to use mock data for verification.
"""

from __future__ import annotations

import structlog
from fastmcp import FastMCP

from dataverse_mcp.config import get_settings
from dataverse_mcp.services.formatter import DataFormatter
from dataverse_mcp.services.summarizer import DataSummarizer
from dataverse_mcp.mock_client import MockDataverseClient

logger = structlog.get_logger(__name__)

# ── Settings & Dependencies ─────────────────────────────
settings = get_settings()

mcp = FastMCP(
    name=settings.mcp_server_name,
    instructions=(
        "This is a specialized MOCK version of the Dataverse MCP server. "
        "It uses the fictional 'Hyperion Mining' dataset for verification. "
        "No real Dataverse connection is used."
    ),
)

# Initialize components
client = MockDataverseClient()
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
    """Lists fictional Hyperion Mining tables."""
    try:
        entities = await client.list_tables()
        allowed = _get_allowed_list()
        if allowed:
            entities = [
                e for e in entities
                if e.get("LogicalName") in allowed or e.get("EntitySetName") in allowed
            ]
        return formatter.format_table_list(entities)
    except Exception as e:
        return f" Error: {e}"


@mcp.tool()
async def query_table(
    entity_set: str,
    select: str = "",
    filter_query: str = "",
    orderby: str = "",
    top: int = 20,
) -> str:
    """Fetches data from the fictional Hyperion tables."""
    if not _is_allowed(entity_set):
        return f" Error: Permission denied for table '{entity_set}'."
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
    except Exception as e:
        return f" Error: {e}"


@mcp.tool()
async def search_records(
    entity_set: str,
    search_field: str,
    search_term: str,
    select: str = "",
    top: int = 20,
) -> str:
    """Searches for records in the fictional Hyperion database."""
    if not _is_allowed(entity_set):
        return f" Error: Permission denied for table '{entity_set}'."
    try:
        records = await client.search_records(
            entity_set, search_field=search_field, search_term=search_term,
            select=select or None, top=top,
        )
        return formatter.format_records_table(records)
    except Exception as e:
        return f" Error: {e}"


@mcp.tool()
async def get_record(
    entity_set: str,
    record_id: str,
    select: str = "",
    expand: str = "",
) -> str:
    """Retrieves a single record GUID from Hyperion database."""
    if not _is_allowed(entity_set):
        return f" Error: Permission denied for table '{entity_set}'."
    try:
        record = await client.get_record(
            entity_set, record_id, select=select or None, expand=expand or None,
        )
        return formatter.format_record(record, table_name=entity_set)
    except Exception as e:
        return f" Error: {e}"


@mcp.tool()
async def get_table_stats(entity_set: str, table_logical_name: str) -> str:
    """Returns statistics for the fictional Hyperion tables."""
    if not _is_allowed(entity_set) and not _is_allowed(table_logical_name):
        return f" Error: Permission denied for table '{entity_set}'."
    try:
        count = await client.get_record_count(entity_set)
        schema = await client.get_table_schema(table_logical_name)
        return summarizer.summarize_table_stats(count, table_logical_name, schema=schema)
    except Exception as e:
        return f" Error: {e}"


@mcp.tool()
async def summarize_table(
    entity_set: str,
    select: str = "",
    filter_query: str = "",
    top: int = 100,
    sample_size: int = 5,
) -> str:
    """Summarizes fictional Hyperion yields and staff data."""
    if not _is_allowed(entity_set):
        return f" Error: Permission denied for table '{entity_set}'."
    try:
        records = await client.query_table(
            entity_set, select=select or None, filter_query=filter_query or None, top=top,
        )
        key_fields = select.split(",") if select else None
        return summarizer.summarize_records(
            records, table_name=entity_set, sample_size=sample_size, key_fields=key_fields,
        )
    except Exception as e:
        return f" Error: {e}"


# ═══════════════════════════════════════════════════════════
# MCP RESOURCES
# ═══════════════════════════════════════════════════════════


@mcp.resource("dataverse://tables")
async def resource_tables() -> str:
    """List of fictional Hyperion Mining tables."""
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
    """Schema for a fictional Hyperion table."""
    if not _is_allowed(table_name):
        return f" Error: Permission denied for schema '{table_name}'."
    schema = await client.get_table_schema(table_name)
    return formatter.format_schema(schema)


# ═══════════════════════════════════════════════════════════
# MCP PROMPTS
# ═══════════════════════════════════════════════════════════


@mcp.prompt()
def analyze_data(table_name: str, analysis_goal: str = "general analysis") -> str:
    """Prompt template for analyzing Hyperion Mining data."""
    return f"""Please analyze the fictional '{table_name}' table from Hyperion Mining.

Analysis Goal: {analysis_goal}

Steps:
1. First, check available tables using `list_tables`.
2. Use `get_table_stats` to understand the scale of the site.
3. Use `summarize_table` to get yield summaries.
4. If necessary, fetch detailed data with `query_table`.
5. Summarize your findings and recommendations for the CEO.

All answers should be in English."""


@mcp.prompt()
def compare_records(table_name: str, record_id_1: str, record_id_2: str) -> str:
    """Prompt template for comparing Hyperion yield logs or staff."""
    return f"""Please compare these two fictional records in the '{table_name}' table:

Record 1: {record_id_1}
Record 2: {record_id_2}

Provide your response in English."""
