"""Column Auto-Correction — Fixes AI-hallucinated column names before they reach Dataverse.

Best practice: Never trust the AI to use exact column names. Intercept and fix at the server level.
"""

from __future__ import annotations

import re

# ── Alias Map ─────────────────────────────────────────────
# Maps commonly hallucinated/shortened names → correct Dataverse column names.
COLUMN_ALIASES: dict[str, str] = {
    # Site / Facility
    "mserp_site": "mserp_inventsitename",
    "mserp_sitename": "mserp_inventsitename",
    "mserp_siteid": "mserp_inventsitename",
    "mserp_inventsiteid": "mserp_inventsitename",
    "mserp_facility": "mserp_inventsitename",
    # Company
    "mserp_company": "mserp_companyname",
    "mserp_companyid": "mserp_companyname",
    "mserp_firma": "mserp_companyname",
    # Warehouse
    "mserp_warehouse": "mserp_inventlocationname",
    "mserp_warehousename": "mserp_inventlocationname",
    "mserp_inventlocationid": "mserp_inventlocationname",
    "mserp_location": "mserp_inventlocationname",
    # Product
    "mserp_product": "mserp_itemname",
    "mserp_productname": "mserp_itemname",
    "mserp_item": "mserp_itemname",
    # Product Category
    "mserp_category": "mserp_etgproductlevel03name",
    "mserp_productcategory": "mserp_etgproductlevel03name",
    "mserp_etgproductlevel03": "mserp_etgproductlevel03name",
    "mserp_productlevel": "mserp_etgproductlevel03name",
    # Country of Origin
    "mserp_countryoforigin": "mserp_inventcolorid",
    "mserp_countryoforiginname": "mserp_inventcolorid",
    "mserp_origin": "mserp_inventcolorid",
    "mserp_country": "mserp_inventcolorid",
    # Date
    "mserp_reportdate": "mserp_headerreportdate",
    "mserp_date": "mserp_headerreportdate",
}


def fix_column(name: str) -> str:
    """Fixes a single column name if it's a known alias."""
    return COLUMN_ALIASES.get(name.strip().lower(), name.strip())


def fix_select(select: str) -> str:
    """Fixes all column names in a $select string (comma-separated)."""
    if not select:
        return select
    columns = [fix_column(c) for c in select.split(",")]
    return ",".join(columns)


def fix_filter(filter_query: str) -> str:
    """Fixes column names inside an OData $filter expression."""
    if not filter_query:
        return filter_query
    
    result = filter_query
    for wrong, correct in COLUMN_ALIASES.items():
        # Replace whole-word matches only (case-insensitive)
        pattern = re.compile(re.escape(wrong), re.IGNORECASE)
        result = pattern.sub(correct, result)
    return result


def fix_group_by(group_by: str) -> str:
    """Fixes column names in a group_by parameter."""
    if not group_by:
        return group_by
    columns = [fix_column(c) for c in group_by.split(",")]
    return ",".join(columns)
