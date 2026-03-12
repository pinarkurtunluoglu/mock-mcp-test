"""Column Auto-Correction — Fixes AI-hallucinated column names before they reach Dataverse.

Best practice: Never trust the AI to use exact column names. Intercept and fix at the server level.
"""

from __future__ import annotations

import re

# ── Allowed Columns ───────────────────────────────────────
# ONLY these columns from the Field Catalog are allowed.
ALLOWED_COLUMNS: set[str] = {
    "mserp_itemname",
    "mserp_itemid",
    "mserp_etgproductlevel03name",
    "mserp_qty",
    "mserp_purchfifo",
    "mserp_headerreportdate",
    "mserp_inventsitename",
    "mserp_inventlocationname",
    "mserp_companyname",
}

# ── Alias Map ─────────────────────────────────────────────
# Maps commonly hallucinated/shortened names → correct Dataverse column names.
COLUMN_ALIASES: dict[str, str] = {
    # Site / Facility
    "mserp_site": "mserp_inventsitename",
    "mserp_sitename": "mserp_inventsitename",
    "mserp_siteid": "mserp_inventsitename",
    "mserp_site_id": "mserp_inventsitename",
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
    "mserp_itemnamename": "mserp_itemname",
    # Product Category
    "mserp_category": "mserp_etgproductlevel03name",
    "mserp_productcategory": "mserp_etgproductlevel03name",
    "mserp_etgproductlevel03": "mserp_etgproductlevel03name",
    "mserp_productlevel": "mserp_etgproductlevel03name",
    # Date
    "mserp_reportdate": "mserp_headerreportdate",
    "mserp_date": "mserp_headerreportdate",
}


def fix_column(name: str) -> str | None:
    """Fixes a single column name if it's a known alias. Returns None if not in whitelist."""
    name = name.strip().lower()
    fixed = COLUMN_ALIASES.get(name, name)
    return fixed if fixed in ALLOWED_COLUMNS else None


def fix_select(select: str) -> str:
    """Fixes all column names in a $select string. Discards non-whitelisted columns."""
    if not select:
        return select
    fixed_cols = []
    for c in select.split(","):
        fixed = fix_column(c)
        if fixed:
            fixed_cols.append(fixed)
    return ",".join(fixed_cols)


def fix_filter(filter_query: str) -> str:
    """Fixes column names inside an OData $filter expression. Discards alias if target not in whitelist."""
    if not filter_query:
        return filter_query
    
    result = filter_query
    for wrong, correct in COLUMN_ALIASES.items():
        if correct in ALLOWED_COLUMNS:
            # Replace whole-word matches only (case-insensitive) using word boundaries
            pattern = re.compile(rf"\b{re.escape(wrong)}\b", re.IGNORECASE)
            result = pattern.sub(correct, result)
    return result


def fix_group_by(group_by: str) -> str:
    """Fixes column names in a group_by parameter. Discards non-whitelisted columns."""
    if not group_by:
        return group_by
    fixed_cols = []
    for c in group_by.split(","):
        fixed = fix_column(c)
        if fixed:
            fixed_cols.append(fixed)
    return ",".join(fixed_cols)
