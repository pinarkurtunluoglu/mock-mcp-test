"""Mock Dataverse client — Hyperion Mining Operations (Fictional Verification Dataset).

This dataset is designed for verification. All values are unique and non-existent
in the real world to ensure the AI Agent is reading from the MCP server.
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta
from typing import Any

import structlog

logger = structlog.get_logger(__name__)

# ═══════════════════════════════════════════════════════════
# UNIQUE VERIFICATION DATASET — Hyperion Mining Corp
# ═══════════════════════════════════════════════════════════

_MINING_SITES = [
    ("Hyperion-Alpha-9", "Sector 7G", "Xenon-Crystals", 842.55),
    ("Hyperion-Beta-2", "Void-Delta", "Void-Matter", 12.004),
    ("Hyperion-Gamma-5", "Nebula-X", "Aether-Dust", 5542.11),
    ("Hyperion-Zeta-1", "Outer-Rim", "Neutron-Strands", 0.0827),
    ("Deep-Core-Zero", "Unknown-Region", "Stable-Singularity", 1.0001),
    ("Abandoned-Outpost-X", None, "Unknown", None),
]

_ENGINEERS = [
    ("Dr. Alistair Thorne", "athorne@hyperion.void", "Lead Quantum Geologist", "Level 5"),
    ("Commander Sarah Vance", "svance@hyperion.void", "Extraction Specialist", "Level 4"),
    ("Unit-X42", "x42@hyperion.void", "Automation Overseer", "Level 9"),
    ("Engineer Kaelen", "kaelen@hyperion.void", "Stability Technician", "Level 3"),
]

_YIELD_LOGS = [
    ("LOG-9912X", "Alpha-9 Regular Harvest", 42.783, "Optimal"),
    ("LOG-9913X", "Gamma-5 Storm Extraction", 128.552, "Unstable"),
    ("LOG-9914X", "Beta-2 Void Compression", 0.0042, "Critical-Success"),
    ("LOG-9915X", "Zeta-1 Drift Collection", 12.991, "Degraded"),
    ("LOG-9916X", "Core-Zero Singularity Pulse", 0.777, "Anomalous"),
]


def _make_guid() -> str:
    return str(uuid.uuid4())


def _random_date(days_back: int = 365) -> str:
    d = datetime.now() - timedelta(days=random.randint(0, days_back))
    return d.strftime("%Y-%m-%dT%H:%M:%SZ")


def _build_mining_sites() -> list[dict[str, Any]]:
    records = []
    for name, region, mineral, reserves in _MINING_SITES:
        records.append({
            "ms_siteid": _make_guid(),
            "ms_name": name,
            "ms_region": region,
            "ms_mineral_type": mineral,
            "ms_current_reserves": reserves,
            "ms_integrity_index": random.uniform(0.1, 0.99),
            "createdon": _random_date(),
        })
    return records


def _build_staff() -> list[dict[str, Any]]:
    records = []
    for name, email, role, clearance in _ENGINEERS:
        records.append({
            "ms_staffid": _make_guid(),
            "ms_fullname": name,
            "ms_email": email,
            "ms_role": role,
            "ms_clearance_level": clearance,
            "ms_years_active": random.randint(1, 150),
            "createdon": _random_date(),
        })
    return records


def _build_yield_logs() -> list[dict[str, Any]]:
    records = []
    for log_id, desc, yield_val, status in _YIELD_LOGS:
        records.append({
            "ms_logid": _make_guid(),
            "ms_code": log_id,
            "ms_description": desc,
            "ms_net_yield_kg": yield_val,
            "ms_status": status,
            "ms_purity_percent": random.uniform(99.0, 99.999),
            "createdon": _random_date(30),
        })
    return records


MOCK_DATA: dict[str, list[dict[str, Any]]] = {
    "ms_mining_sites": _build_mining_sites(),
    "ms_staff": _build_staff(),
    "ms_extraction_logs": _build_yield_logs(),
}

MOCK_TABLES = [
    {
        "LogicalName": "ms_mining_site",
        "DisplayName": {"UserLocalizedLabel": {"Label": "Mining Site"}},
        "EntitySetName": "ms_mining_sites",
        "PrimaryIdAttribute": "ms_siteid",
        "PrimaryNameAttribute": "ms_name",
    },
    {
        "LogicalName": "ms_staff",
        "DisplayName": {"UserLocalizedLabel": {"Label": "Hyperion Staff"}},
        "EntitySetName": "ms_staff",
        "PrimaryIdAttribute": "ms_staffid",
        "PrimaryNameAttribute": "ms_fullname",
    },
    {
        "LogicalName": "ms_extraction_log",
        "DisplayName": {"UserLocalizedLabel": {"Label": "Extraction Log"}},
        "EntitySetName": "ms_extraction_logs",
        "PrimaryIdAttribute": "ms_logid",
        "PrimaryNameAttribute": "ms_code",
    },
]

MOCK_SCHEMAS: dict[str, dict[str, Any]] = {
    "ms_mining_site": {
        "LogicalName": "ms_mining_site",
        "EntitySetName": "ms_mining_sites",
        "Attributes": [
            {"LogicalName": "ms_siteid", "AttributeType": "Uniqueidentifier"},
            {"LogicalName": "ms_name", "AttributeType": "String"},
            {"LogicalName": "ms_region", "AttributeType": "String"},
            {"LogicalName": "ms_mineral_type", "AttributeType": "String"},
            {"LogicalName": "ms_current_reserves", "AttributeType": "Double"},
            {"LogicalName": "ms_integrity_index", "AttributeType": "Double"},
        ],
    },
    "ms_staff": {
        "LogicalName": "ms_staff",
        "EntitySetName": "ms_staff",
        "Attributes": [
            {"LogicalName": "ms_staffid", "AttributeType": "Uniqueidentifier"},
            {"LogicalName": "ms_fullname", "AttributeType": "String"},
            {"LogicalName": "ms_email", "AttributeType": "String"},
            {"LogicalName": "ms_role", "AttributeType": "String"},
            {"LogicalName": "ms_clearance_level", "AttributeType": "String"},
        ],
    },
}


class MockDataverseClient:
    """Mock client for Hyperion Mining Operations dataset."""

    def __init__(self) -> None:
        self._logger = logger.bind(component="mock_client")
        self._logger.info("hyperion_dataset_active", message="💎 HYPERION MODE — Specialized verification data loaded")

    async def list_tables(self) -> list[dict[str, Any]]:
        return MOCK_TABLES

    async def get_table_schema(self, table_name: str) -> dict[str, Any]:
        return MOCK_SCHEMAS.get(table_name, {"Attributes": []})

    async def query_table(self, entity_set: str, **kwargs) -> list[dict[str, Any]]:
        top = kwargs.get("top", 20)
        return MOCK_DATA.get(entity_set, [])[:top]

    async def get_record(self, entity_set: str, record_id: str, **kwargs) -> dict[str, Any]:
        records = MOCK_DATA.get(entity_set, [])
        return records[0] if records else {"error": "Record not found"}

    async def get_record_count(self, entity_set: str, **kwargs) -> int:
        return len(MOCK_DATA.get(entity_set, []))

    async def search_records(self, entity_set: str, search_field: str, search_term: str, **kwargs) -> list[dict[str, Any]]:
        records = MOCK_DATA.get(entity_set, [])
        return [r for r in records if search_term.lower() in str(r.get(search_field, "")).lower()]

    async def close(self) -> None:
        pass
