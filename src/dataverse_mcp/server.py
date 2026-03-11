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
from dataverse_mcp.services.response_guard import guard
from dataverse_mcp.services.column_guard import fix_select, fix_filter, fix_group_by, fix_column, ALLOWED_COLUMNS
from dataverse_mcp.client import DataverseClient

logger = structlog.get_logger(__name__)

# ── Latest Date Helper ────────────────────────────────────
async def _ensure_latest_date_filter(filter_query: str = "") -> str:
    """If mserp_headerreportdate is not in the filter, find latest date and add it.
    This ensures we only look at the most recent snapshot as requested.
    """
    if "mserp_headerreportdate" in (filter_query or ""):
        return filter_query or ""
    
    try:
        # Find the latest date
        result = await client.aggregate_table(
            ENTITY_SET, numeric_field="mserp_headerreportdate", agg_type="max"
        )
        max_date = result.get("mserp_headerreportdate_max")
        if not max_date:
            return filter_query or ""
            
        # Format as YYYY-MM-DD
        if isinstance(max_date, str) and "T" in max_date:
            max_date = max_date.split("T")[0]
            
        date_filter = f"mserp_headerreportdate eq {max_date}"
        if filter_query:
            return f"{date_filter} and ({filter_query})"
        return date_filter
    except Exception:
        return filter_query or ""

# ── Settings & Dependencies ─────────────────────────────
settings = get_settings()

ENTITY_SET = settings.entity_set_name
ENTITY_LOGICAL = settings.entity_logical_name

mcp = FastMCP(
    name=settings.mcp_server_name,
    instructions=(
        # ── ROLE ──────────────────────────────────────────────
        "You are a Senior Supply-Chain Data Analyst with deep expertise in inventory management. "
        "You have access to the Tiryaki Group's Inventory Aging Report stored in Microsoft Dataverse "
        f"(entity: '{ENTITY_SET}', ~500 000 records). "
        "Always respond in the same language as the user's message (usually Turkish).\n\n"

        # ── FIELD CATALOG ─────────────────────────────────────
        "## Field Catalog (ALWAYS use these exact column names)\n"
        "| Concept | Column Name | Notes |\n"
        "|---|---|---|\n"
        "| Product Name | mserp_itemname | Full product name (Turkish text) |\n"
        "| Product Code | mserp_itemid | Short code like 10IQ4112 |\n"
        "| Product Category | mserp_etgproductlevel03name | e.g. Wheat, Corn (mixed-case English) |\n"
        "| Quantity | mserp_qty | Inventory quantity (numeric) |\n"
        "| FIFO Age (days) | mserp_purchfifo | Days since purchase (FIFO) |\n"
        "| Report Date | mserp_headerreportdate | Use for ALL date filtering |\n"
        "| Site / Facility | mserp_inventsitename | e.g. Gaziantep Tesisi, Vessel |\n"
        "| Warehouse | mserp_inventlocationname | Sub-location within a site |\n"
        "| Company | mserp_companyname | e.g. MESOPOTAMIA FZE IRAQ BRANCH |\n"

        # ── TOOL SELECTION (Decision Matrix) ──────────────────
        "## Tool Selection — ALWAYS pick the right tool\n"
        "**CRITICAL RULE: When the user asks for 'toplam', 'ortalama', 'minimum', 'maximum', 'kaç kayıt', "
        "'kaç gün', 'kaç ton' → you MUST use `calculate_inventory_totals` or `calculate_multi_metrics`. "
        "NEVER use `summarize_inventory_aging` or `query_inventory_aging` for any calculation.**\n\n"
        "| User Intent | Correct Tool | Why |\n"
        "|---|---|---|\n"
        "| Totals, sums, averages, counts, insights, trends | calculate_inventory_totals | Server-side aggregation on FULL dataset |\n"
        "| Multiple metrics at once (sum+avg+min+max+count) | calculate_multi_metrics | FASTEST - runs all 5 in PARALLEL, one call |\n"
        "| Compare groups (company vs company, site vs site) | calculate_inventory_totals (multiple calls) | Use different group_by per call |\n"
        "| Cross-dimensional (e.g. category breakdown within one company) | calculate_inventory_totals with filter_query + group_by | filter fixes one dimension, group_by splits the other |\n"
        "| View specific raw records, examples, samples | query_inventory_aging | Returns max 500 rows |\n"
        "| Find a record by name/keyword | search_inventory_aging | Case-insensitive contains search |\n"
        "| Understand table structure | get_inventory_aging_schema | Returns columns and types |\n"
        "| Get total record count | get_inventory_aging_count | Single number for latest date |\n\n"

        # ── ODATA QUERY RULES ─────────────────────────────────
        "## OData Query Rules\n"
        "1. **Text search**: ALWAYS use `contains(column, 'value')`. NEVER use `eq` for text/name columns.\n"
        "2. **Case sensitivity**: `contains()` is case-insensitive, so 'wheat', 'WHEAT', 'Wheat' all work.\n"
        "3. **group_by limit**: Only ONE column per call. For multi-dimension analysis, make multiple calls.\n"
        "4. **Numeric filters**: Use standard OData operators: `mserp_purchfifo gt 100`, `mserp_qty lt 500`.\n"
        "5. **Date filters**: `mserp_headerreportdate ge 2024-01-01`.\n"
        "6. **Datetime groupby**: NEVER use group_by on `mserp_headerreportdate` — Dataverse rejects groupby on datetime fields.\n\n"

        # ── DATE AWARENESS ────────────────────────────────────
        "## Date Awareness — AUTOMATIC\n"
        "The server AUTOMATICALLY filters all queries to the LATEST report date.\n"
        "1. You do NOT need to call `get_latest_report_date` anymore.\n"
        "2. You do NOT need to add `mserp_headerreportdate` to your filters unless the user asks for a specific date.\n"
        "3. Focus ONLY on filtering by company, site, category, or product.\n\n"

        # ── FORBIDDEN PATTERNS ────────────────────────────────
        "## Forbidden Patterns — NEVER do these\n"
        "- NEVER use any column name NOT listed in the Field Catalog above.\n"
        "- NEVER invent or shorten column names. Use ONLY exact names from the catalog.\n"
        "- NEVER use `tolower()` or `toupper()` in OData — Virtual Entities reject these.\n"
        "- NEVER use group_by on `mserp_headerreportdate` — Dataverse rejects groupby on datetime fields.\n"
        "- NEVER calculate totals from `query_inventory_aging` or `summarize_inventory_aging` results.\n"
        "- NEVER pass multiple columns to group_by.\n\n"

        # ── TURKISH LANGUAGE MAPPING ──────────────────────────
        "## Turkish → Column Mapping\n"
        "When the user says:\n"
        "- 'ürün adı' / 'ürün ismi' / 'malzeme' → search in `mserp_itemname`\n"
        "- 'kategori' / 'ürün grubu' / 'ürün kategorisi' → search in `mserp_etgproductlevel03name`\n"
        "- 'tesis' / 'depo' / 'site' → search in `mserp_inventsitename`\n"
        "- 'şirket' / 'firma' → search in `mserp_companyname`\n"
        "- 'yaş' / 'bekleme süresi' / 'stok yaşı' → use `mserp_purchfifo`\n"
        "- 'miktar' / 'adet' / 'ton' → use `mserp_qty`\n\n"

        # ── MULTI-STEP ANALYSIS WORKFLOW ──────────────────────
        "## Multi-Step Analysis Workflow\n"
        "For comprehensive insights on the LATEST data, follow this pattern:\n"
        "1. Call `calculate_inventory_totals` with group_by='mserp_companyname'\n"
        "2. Call `calculate_inventory_totals` with group_by='mserp_inventsitename'\n"
        "3. Call `calculate_inventory_totals` with group_by='mserp_etgproductlevel03name'\n"
        "4. Combine and cross-reference all results to produce actionable insights.\n"
        "5. If deeper drill-down is needed, use filter_query to fix one dimension, then group_by another.\n"
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
        return guard(formatter.format_schema(schema))
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def get_inventory_aging_count(filter_query: str = "") -> str:
    """Returns the total number of records in the Inventory Aging Report.
    If no date is specified, it returns the count for the LATEST report date.
    """
    try:
        filter_query = fix_filter(filter_query)
        filter_query = await _ensure_latest_date_filter(filter_query)
        count = await client.get_record_count(ENTITY_SET, filter_query=filter_query)
        msg = f"Inventory Aging Report record count: **{count:,}**"
        if filter_query:
            msg += f"\n- Filter: `{filter_query}`"
        return msg
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def get_latest_report_date() -> str:
    """Returns the most recent report date (mserp_headerreportdate) available in the dataset.
    ALWAYS call this FIRST when the user does not specify a date.
    Use the returned date in filter_query like: mserp_headerreportdate eq <date>
    """
    try:
        result = await client.aggregate_table(
            ENTITY_SET, numeric_field="mserp_headerreportdate", agg_type="max"
        )
        max_date = result.get("mserp_headerreportdate_max", "Unknown")
        # Extract just the date portion (remove time)
        if isinstance(max_date, str) and "T" in max_date:
            max_date = max_date.split("T")[0]
        return f"Latest report date: **{max_date}**\nUse this in your filters: `mserp_headerreportdate eq {max_date}`"
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def query_inventory_aging(
    select: str = "",
    filter_query: str = "",
    orderby: str = "",
    top: int = 50,
) -> str:
    """Returns raw records from the Inventory Aging Report (max 500 rows).
    Use ONLY for viewing specific records or examples — NEVER for calculating totals.

    Args:
        select: Comma-separated columns to return (e.g. 'mserp_itemname,mserp_qty'). Leave empty for all.
        filter_query: OData $filter (e.g. "contains(mserp_itemname, 'BUĞDAY')").
        orderby: OData $orderby (e.g. "mserp_qty desc").
        top: Max records to return (default: 50, max: 500).
    """
    try:
        if top > 500:
            top = 500
        
        # Get total count to inform AI about data completeness
        try:
            total_count = await client.get_record_count(ENTITY_SET)
        except Exception:
            total_count = None
        
        # Auto-correct hallucinated column names
        select = fix_select(select)
        filter_query = fix_filter(filter_query)
        filter_query = await _ensure_latest_date_filter(filter_query)
        
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
        
        return guard(f"{header}\n\n{result}")
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def search_inventory_aging(
    search_field: str,
    search_term: str,
    select: str = "",
    top: int = 20,
) -> str:
    """Searches for records where a field contains the given term (case-insensitive).
    Always use Name columns (e.g. mserp_companyname, NOT mserp_companyid).

    Args:
        search_field: Column to search in (e.g. 'mserp_itemname', 'mserp_companyname').
        search_term: Value to search for (case-insensitive).
        select: Comma-separated columns to return. Leave empty for all.
        top: Max results (default: 20).
    """
    try:
        # Auto-correct and enforce date
        search_field = fix_column(search_field)
        select = fix_select(select)
        filter_query = await _ensure_latest_date_filter("")
        
        records = await client.search_records(
            ENTITY_SET, search_field=search_field, search_term=search_term,
            select=select or None, top=top, filter_query=filter_query
        )
        result = formatter.format_records_table(records)
        return guard(f"**Search Results** — Found {len(records)} records where '{search_field}' contains '{search_term}'\n\n{result}")
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
    """Generates a statistical summary from a SAMPLE of records (max 5000).
    Use ONLY for showing patterns and examples — NEVER for calculating totals or insights.
    For accurate totals, use calculate_inventory_totals instead.

    Args:
        select: Comma-separated fields to analyze (e.g. 'mserp_qty,mserp_itemname').
        filter_query: OData $filter to narrow data before sampling.
        top: Sample size (default: 2000, max: 5000).
        sample_size: Number of example records to display (default: 5).
    """
    try:
        # Cap at 5000 for safety but allow deep analysis
        actual_top = min(top, 5000)
        
        # Auto-correct and enforce date
        select = fix_select(select)
        filter_query = fix_filter(filter_query)
        filter_query = await _ensure_latest_date_filter(filter_query)
        
        records = await client.query_table(
            ENTITY_SET, 
            select=select or None, 
            filter_query=filter_query or None, 
            fetch_all=True,
            max_records=actual_top
        )
        
        key_fields = [c.strip() for c in select.split(",")] if select else None
        result = summarizer.summarize_records(
            records, table_name="Inventory Aging Report",
            sample_size=sample_size, key_fields=key_fields,
        )
        return guard(result)
    except Exception as e:
        return f"Error during large-scale summary: {e}"


@mcp.tool()
async def calculate_inventory_totals(
    numeric_field: str = "",
    agg_type: str = "sum",
    group_by: str = "",
    filter_query: str = "",
    top_n: int = 50,
) -> str:
    """Server-side aggregation across the ENTIRE dataset (~500k records). No rows downloaded.
    THIS IS THE PRIMARY TOOL for all totals, averages, counts, trends, and insights.
    Call MULTIPLE TIMES with different group_by values for multi-dimensional analysis.

    Args:
        numeric_field: Column to aggregate (e.g. 'mserp_qty', 'mserp_purchfifo'). Leave empty for count.
        agg_type: One of: 'sum', 'average', 'min', 'max', 'count'.
        group_by: ONE column to group by. See Field Catalog in instructions for valid names.
        filter_query: OData $filter to narrow scope (e.g. "contains(mserp_companyname, 'MESQ')").
        top_n: Max number of grouped rows to return, sorted by aggregate value (default: 50).
    """
    try:
        # Auto-correct hallucinated column names
        numeric_field = fix_column(numeric_field) if numeric_field else numeric_field
        group_by = fix_group_by(group_by)
        filter_query = fix_filter(filter_query)
        filter_query = await _ensure_latest_date_filter(filter_query)
        
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
            # Sort by aggregate value descending and cap at top_n
            total_groups = len(result)
            result.sort(key=lambda r: r.get(alias, 0), reverse=True)
            display_result = result[:top_n]
            
            group_cols = [c.strip() for c in group_by.split(",")]
            col_headers = " | ".join(group_cols) + f" | {label}"
            col_seps = " | ".join(["---"] * len(group_cols)) + " | ---"
            lines = [header, f"\n| {col_headers} |", f"| {col_seps} |"]
            for row in display_result:
                group_vals = " | ".join(str(row.get(c, "N/A")) for c in group_cols)
                agg_val = row.get(alias, 0)
                lines.append(f"| {group_vals} | {agg_val:,.2f} |")
            
            if total_groups > top_n:
                lines.append(f"\n> *...ve {total_groups - top_n} grup daha (toplam {total_groups} grup). Daha dar bir filtre kullanın.*")
            
            return guard("\n".join(lines))
        else:
            # Single result
            value = result.get(alias, 0) if isinstance(result, dict) else 0
            detail = f"- **{label}**: `{value:,.2f}`"
            if filter_query:
                detail += f"\n- Filter: `{filter_query}`"
            return f"{header}\n{detail}"
    except Exception as e:
        return f"Error: Server-side aggregation failed. {e}"


@mcp.tool()
async def calculate_multi_metrics(
    numeric_field: str,
    filter_query: str = "",
) -> str:
    """Calculates SUM, AVERAGE, MIN, MAX, and COUNT for a field in ONE call (runs in parallel).
    This is the FASTEST way to get multiple statistics for the LATEST report date.

    Args:
        numeric_field: Column to analyze (e.g. 'mserp_qty', 'mserp_purchfifo').
        filter_query: OData $filter to narrow scope.
    """
    try:
        numeric_field = fix_column(numeric_field)
        filter_query = fix_filter(filter_query)
        filter_query = await _ensure_latest_date_filter(filter_query)

        agg_types = ["sum", "average", "min", "max", "count"]
        tasks = [
            client.aggregate_table(
                ENTITY_SET,
                numeric_field=numeric_field if agg != "count" else "",
                agg_type=agg,
                filter_query=filter_query,
            )
            for agg in agg_types
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        lines = [
            f"### Multi-Metric Analysis: `{numeric_field}`",
            "*All metrics calculated in a SINGLE parallel call for the latest date*\n",
        ]
        if filter_query:
            lines.append(f"- **Filter:** `{filter_query}`\n")

        labels = {
            "sum": "Toplam (Sum)",
            "average": "Ortalama (Average)",
            "min": "Minimum",
            "max": "Maximum",
            "count": "Kayit Sayisi (Count)",
        }

        for agg, result in zip(agg_types, results):
            if isinstance(result, Exception):
                lines.append(f"- **{labels[agg]}**: Error")
                continue
            alias = "record_count" if agg == "count" else f"{numeric_field}_{agg}"
            value = result.get(alias, 0) if isinstance(result, dict) else 0
            lines.append(f"- **{labels[agg]}**: `{value:,.2f}`")

        return "\n".join(lines)
    except Exception as e:
        return f"Error: Multi-metric calculation failed. {e}"


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
1. `calculate_inventory_totals` ile şirket bazlı toplam miktarları hesaplayın (group_by='mserp_companyname').
2. `calculate_inventory_totals` ile tesis bazlı dağılımı çıkarın (group_by='mserp_inventsitename').
3. `calculate_inventory_totals` ile ürün kategorisi kırılımını alın (group_by='mserp_etgproductlevel03name').
4. Gerekirse `query_inventory_aging` ile ham kayıt örnekleri getirin.
5. Tüm sonuçları birleştirerek kapsamlı içgörüler ve öneriler sunun.

Yanıtlarınızı Türkçe verin."""


@mcp.prompt()
def filter_aging_items(min_days: str = "90", field_name: str = "mserp_purchfifo") -> str:
    """Prompt template for filtering aged inventory items."""
    return f"""Envanter Yaşlandırma Raporundan {min_days} günden eski kalemleri analiz edin.

Adımlar:
1. `calculate_inventory_totals` ile `{field_name} gt {min_days}` filtresi uygulayarak toplam kayıt sayısını bulun.
2. `calculate_inventory_totals` ile aynı filtreyi uygulayıp şirket bazında kırılım çıkarın (group_by='mserp_companyname').
3. `query_inventory_aging` ile en kritik 10 kalemi getirin (orderby='{field_name} desc', top=10).
4. Sonuçları ve tavsiyeleri özetleyin.

Yanıtlarınızı Türkçe verin."""

