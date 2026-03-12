"""FastMCP Server — Dataverse Inventory Aging Report.

Provides specialized MCP tools for the mserp_tryaiinventoryagingreportentities entity
from the Tiryaki Operations Dataverse environment.
"""

from __future__ import annotations

import asyncio
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

        # ── FIELD CATALOG (THE ONLY TRUTH) ────────────────────
        "## Field Catalog — THE ONLY COLUMNS THAT EXIST\n"
        "| Concept | Column Name | Notes |\n"
        "|---|---|---|\n"
        "| Product Name | mserp_itemname | Full product name (Turkish text) |\n"
        "| Product Code | mserp_itemid | Short code like 10IQ4112 |\n"
        "| Product Category | mserp_etgproductlevel03name | e.g. Wheat, Corn |\n"
        "| Quantity | mserp_qty | Inventory quantity (numeric) |\n"
        "| FIFO Age (days) | mserp_purchfifo | Days since purchase (FIFO) |\n"
        "| Report Date | mserp_headerreportdate | Use for ALL date filtering |\n"
        "| Site / Facility | mserp_inventsitename | e.g. Gaziantep Tesisi, Muş |\n"
        "| Warehouse | mserp_inventlocationname | Sub-location within a site |\n"
        "| Company | mserp_companyname | e.g. MESOPOTAMIA FZE |\n\n"

        # ── TURKISH → COLUMN MAPPING ──────────────────────────
        "## Turkish → Column Mapping\n"
        "When the user says:\n"
        "- 'ürün' / 'malzeme' / 'isim' → use `mserp_itemname`\n"
        "- 'kategori' / 'grup' → use `mserp_etgproductlevel03name`\n"
        "- 'tesis' / 'depo' / 'site' → use `mserp_inventsitename`\n"
        "- 'şirket' / 'firma' → use `mserp_companyname`\n"
        "- **'ortalama yaş' (average age)** → ALWAYS use `calculate_weighted_average` with `mserp_purchfifo` weighted by `mserp_qty`.\n\n"

        # ── FILTERING RULES — NO EXCEPTIONS ──────────────────
        "## Filtering Rules — NO EXCEPTIONS\n"
        "1. **SEARCH BY NAME**: ALWAYS use `contains(column, 'value')` for text fields (`mserp_itemname`, `mserp_inventsitename`, etc.).\n"
        "2. **NO TECHNICAL IDs**: NEVER use fields ending in 'id' (e.g., `mserp_siteid`) for filtering by text. They do not exist for you.\n"
        "3. **DATE AUTOMATION**: The server automatically filters to the LATEST date. Do NOT add `mserp_headerreportdate` to filters unless a specific past date is requested.\n"
        "4. **TURKISH CHARS**: Dataverse search is SENSITIVE to Turkish characters. If searching for Muş, use exact 'Muş' or 'MUŞ' in `contains()`. NEVER swap 'Ş' for 'S'.\n\n"

        # ── UNIVERSAL DATA AWARENESS ──────────────────────────
        "## Universal Data Awareness — How you 'see' everything\n"
        "You have access to the ENTIRE latest report (~500k rows) through three lenses:\n"
        "1. **Eagle Eye (Aggregation)**: Use `calculate_inventory_totals` to see the WHOLE report's sums/averages instantly. You are OMNISCIENT here.\n"
        "2. **Searchlight (Filtering)**: Use `search_inventory_aging` to find ANY specific needle in the 500k haystack.\n"
        "3. **Paging (Scrolling)**: Use `query_inventory_aging` with `next_token` to scroll through the report page-by-page. "
        "F&O Virtual Entities do not support `skip`. They ONLY support `next_token`. To see the next page, "
        "pass the token returned in the previous response's footer.\n\n"

        # ── MULTI-STEP ANALYSIS WORKFLOW ──────────────────────
        "## Multi-Step Analysis Workflow\n"
        "For comprehensive insights on the LATEST data, follow this pattern:\n"
        "1. Start with `calculate_inventory_totals` (Eagle Eye) to see the big picture (totals by company/site).\n"
        "2. Identify anomalies or interests, then `query_inventory_aging` (Paging) to see specific examples.\n"
        "3. If a specific entity is mentioned, use `search_inventory_aging` (Searchlight).\n"
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
    Only essential business columns are returned to ensure focus."""
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
    next_token: str = "",
) -> str:
    """Returns raw records from the Inventory Aging Report. Use `next_token` for "scrolling".
    F&O Virtual Entities do NOT support `skip`. They only support `next_token` (skiptoken).

    Args:
        select: Comma-separated columns (e.g. 'mserp_itemname,mserp_qty').
        filter_query: OData $filter (e.g. "contains(mserp_itemname, 'BUĞDAY')").
        orderby: OData $orderby (e.g. "mserp_qty desc").
        top: Max records per page (default: 50, max: 500).
        next_token: The pagination token from the previous response to see the next page.
    """
    try:
        if top > 500:
            top = 500
        
        # Auto-correct hallucinated column names
        select = fix_select(select)
        filter_query = fix_filter(filter_query)
        filter_query = await _ensure_latest_date_filter(filter_query)
        
        # Call client with skiptoken support
        result_dict = await client.query_table(
            ENTITY_SET,
            select=select or None,
            filter_query=filter_query or None,
            orderby=orderby or None,
            top=top,
            next_link=next_token or None,
        )
        
        records = result_dict.get("value", [])
        next_link = result_dict.get("@odata.nextLink")
        
        columns = [c.strip() for c in select.split(",")] if select else None
        result_table = formatter.format_records_table(records, columns=columns)
        
        # Build response with completeness & pagination info
        header = f"**Inventory Aging Report** — Showing {len(records)} records."
        
        if next_link:
            header += f"\n\n> **Daha fazla kayıt var.** Bir sonraki sayfayı görmek için `next_token` parametresine şu değeri yapıştırın:\n> `{next_link}`"
        
        if not next_token:
             header += "\n> Toplam/ortalama hesaplamaları için `calculate_inventory_totals` kullanmanızı öneririm."

        return guard(f"{header}\n\n{result_table}")
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def search_inventory_aging(
    search_field: str,
    search_term: str,
    select: str = "",
    top: int = 20,
    next_token: str = "",
) -> str:
    """Searches for records using a keyword. Use `next_token` for more results.
    Always use Name columns (e.g. mserp_companyname, NOT mserp_companyid).

    Args:
        search_field: Column to search in (e.g. 'mserp_itemname', 'mserp_companyname').
        search_term: Value to search for (case-insensitive).
        select: Comma-separated columns to return.
        top: Max results per page (default: 20).
        next_token: Pagination token for the next page of results.
    """
    try:
        # Auto-correct and enforce date
        search_field = fix_column(search_field)
        select = fix_select(select)
        filter_query = await _ensure_latest_date_filter("")
        
        result_dict = await client.search_records(
            ENTITY_SET, search_field=search_field, search_term=search_term,
            select=select or None, top=top, filter_query=filter_query,
            next_link=next_token or None
        )
        records = result_dict.get("value", [])
        next_link = result_dict.get("@odata.nextLink")
        
        result_table = formatter.format_records_table(records)
        
        header = f"**Search Results** — Found {len(records)} records where '{search_field}' contains '{search_term}'"
        if next_link:
            header += f"\n\n> **Daha fazla sonuç var.** Devamı için `next_token` parametresini kullanın:\n> `{next_link}`"
            
        return guard(f"{header}\n\n{result_table}")
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
        
        result_dict = await client.query_table(
            ENTITY_SET, 
            select=select or None, 
            filter_query=filter_query or None, 
            fetch_all=True,
            max_records=actual_top
        )
        records = result_dict.get("value", [])
        
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


@mcp.tool()
async def calculate_weighted_average(
    value_field: str = "mserp_purchfifo",
    weight_field: str = "mserp_qty",
    group_by: str = "",
    filter_query: str = "",
) -> str:
    """Calculates WEIGHTED AVERAGE (e.g., true inventory age) across the ENTIRE dataset.
    This is much more accurate than arithmetic average for inventory age.
    
    Args:
        value_field: The column to average (default: 'mserp_purchfifo' - inventory age).
        weight_field: The column to use as weight (default: 'mserp_qty' - quantity).
        group_by: Optional column to group results by (e.g. 'mserp_companyname', 'mserp_inventsitename').
        filter_query: OData $filter to narrow scope.
    """
    try:
        value_field = fix_column(value_field)
        weight_field = fix_column(weight_field)
        group_by = fix_group_by(group_by)
        filter_query = fix_filter(filter_query)
        filter_query = await _ensure_latest_date_filter(filter_query)
        
        result = await client.calculate_weighted_average(
            ENTITY_SET,
            value_field=value_field,
            weight_field=weight_field,
            filter_query=filter_query,
            group_by=group_by
        )
        
        header = f"### Weighted Average Analysis"
        sub_header = f"- **Target Field:** `{value_field}`\n- **Weight Field:** `{weight_field}`"
        
        if group_by and isinstance(result, list):
            # Sort by weighted average descending
            result.sort(key=lambda r: r.get(f"{value_field}_weighted_avg", 0), reverse=True)
            
            lines = [header, sub_header, f"\n| {group_by} | Weighted Average | Total Weight |", "|---|---|---|"]
            for row in result:
                g_val = row.get(group_by, "N/A")
                avg = row.get(f"{value_field}_weighted_avg", 0)
                weight = row.get("total_weight", 0)
                lines.append(f"| {g_val} | **{avg:.2f}** | {weight:,.2f} |")
            return "\n".join(lines)
        else:
            # Single value
            avg_val = result if isinstance(result, (int, float)) else 0
            return f"{header}\n{sub_header}\n\n**Calculated Weighted Average: {avg_val:.2f}**"

    except Exception as e:
        return f"Error calculating weighted average: {e}"


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

