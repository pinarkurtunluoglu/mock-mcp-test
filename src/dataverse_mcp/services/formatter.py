"""AI-friendly output formatting service."""

from __future__ import annotations

from typing import Any


class DataFormatter:
    """Converts Dataverse records into AI-friendly Markdown formats."""

    def format_record(self, record: dict[str, Any], table_name: str | None = None) -> str:
        """Formats a single record as a Markdown list."""
        title = table_name.capitalize() if table_name else "Record Detail"
        lines = [f"### {title}\n"]
        lines.append("| Field | Value |")
        lines.append("| --- | --- |")

        # Sort fields, keeping metadata at the bottom
        meta_fields = ["id", "createdon", "modifiedon", "versionnumber"]
        keys = sorted(record.keys(), key=lambda k: (k in meta_fields, k))

        for key in keys:
            if key.startswith("@"): continue  # Skip OData metadata
            value = self._format_value(record[key])
            lines.append(f"| **{key}** | {value} |")

        return "\n".join(lines)

    def format_records_table(
        self, records: list[dict[str, Any]], columns: list[str] | None = None, max_rows: int = 50
    ) -> str:
        """Formats multiple records as a Markdown table.
        
        Args:
            records: List of record dicts.
            columns: Specific columns to show. Auto-detected if None.
            max_rows: Maximum rows to include in the table (default: 50).
        """
        if not records:
            return "No records found."

        if not columns:
            # Use keys from the first record, skipping metadata
            meta_fields = ["id", "createdon", "modifiedon", "versionnumber", "ownerid"]
            columns = [k for k in records[0].keys() if k not in meta_fields and not k.startswith("@")]
            columns = columns[:8]  # Limit width for context efficiency

        # Cap the number of rows
        total = len(records)
        display_records = records[:max_rows]

        header = "| " + " | ".join(columns) + " |"
        separator = "| " + " | ".join(["---"] * len(columns)) + " |"
        lines = [header, separator]

        for record in display_records:
            row = []
            for col in columns:
                val = self._format_value(record.get(col, ""))
                row.append(self._truncate_cell(val))
            lines.append("| " + " | ".join(row) + " |")

        if total > max_rows:
            lines.append(f"\n> *...ve {total - max_rows} kayıt daha (toplam {total}). Daha az kayıt için filtre kullanın.*")

        return "\n".join(lines)

    def format_table_list(self, entities: list[dict[str, Any]]) -> str:
        """Formats the list of Dataverse tables."""
        lines = ["### Available Tables\n", "| Logical Name | Display Name | Entity Set |", "| --- | --- | --- |"]
        for entity in entities:
            logical = entity.get("LogicalName", "N/A")
            display = entity.get("DisplayName", {}).get("UserLocalizedLabel", {}).get("Label", "N/A")
            entity_set = entity.get("EntitySetName", "N/A")
            lines.append(f"| {logical} | {display} | {entity_set} |")
        return "\n".join(lines)

    # Key fields that are most relevant for analysis — shown first in schema
    KEY_COLUMNS = {
        "mserp_itemname", "mserp_itemid", "mserp_etgproductlevel03name",
        "mserp_qty", "mserp_purchfifo", "mserp_purchlifo",
        "mserp_headerreportdate", "mserp_inventsitename",
        "mserp_inventlocationname", "mserp_companyname",
        "mserp_inventcolorid", "mserp_amountmst",
    }

    def format_schema(self, schema: dict[str, Any], key_only: bool = True) -> str:
        """Formats the table schema information.
        
        Args:
            schema: Dataverse schema dict.
            key_only: If True, only show the key analysis columns (default: True).
        """
        logical = schema.get("LogicalName", "N/A")
        display = schema.get("DisplayName", {}).get("UserLocalizedLabel", {}).get("Label", logical)
        attrs = schema.get("Attributes", [])

        if key_only:
            key_attrs = [a for a in attrs if a.get("LogicalName") in self.KEY_COLUMNS]
            other_count = len(attrs) - len(key_attrs)
            title_note = f" (showing {len(key_attrs)} key columns, {other_count} others hidden)"
        else:
            key_attrs = attrs
            title_note = ""

        lines = [
            f"### Table Schema: `{logical}`{title_note}\n",
            f"- **Display Name:** {display}",
            f"- **Logical Name:** {logical}",
            f"- **Entity Set:** {schema.get('EntitySetName', 'N/A')}",
            f"\n#### Columns ({len(key_attrs)} items)\n",
            "| Logical Name | Display Name | Type | Required |",
            "| --- | --- | --- | --- |",
        ]

        for attr in sorted(key_attrs, key=lambda a: a.get("LogicalName", "")):
            attr_logical = attr.get("LogicalName", "N/A")
            attr_display = attr.get("DisplayName", {}).get("UserLocalizedLabel", {}).get("Label", attr_logical)
            attr_type = attr.get("AttributeType", "N/A")
            required = attr.get("RequiredLevel", {}).get("Value", "None")

            lines.append(f"| {attr_logical} | {attr_display} | {attr_type} | {required} |")

        return "\n".join(lines)

    def _format_value(self, value: Any) -> str:
        """Helper to format various data types into strings."""
        if value is None: return ""
        if isinstance(value, bool): return "✅" if value else "❌"
        if isinstance(value, (int, float)): return f"{value:,}"
        return str(value)

    def _truncate_cell(self, text: str, length: int = 50) -> str:
        """Truncates cell value if it's too long to save space."""
        if len(text) <= length: return text
        return f"{text[:length-3]}..."
