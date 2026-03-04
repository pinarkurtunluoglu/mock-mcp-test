"""Data summarization service — Intelligent processing of large datasets."""

from __future__ import annotations

from typing import Any


class DataSummarizer:
    """Intelligently summarizes Dataverse data for AI context efficiency."""

    def __init__(self, max_tokens: int = 2000) -> None:
        self._max_tokens = max_tokens

    def summarize_records(
        self,
        records: list[dict[str, Any]],
        table_name: str,
        sample_size: int = 5,
        key_fields: list[str] | None = None,
    ) -> str:
        """Summarizes a list of Dataverse records."""
        if not records:
            return f"No matching records found in table **{table_name}**."

        parts = []
        parts.append(f"**{table_name.capitalize()}** Table Summary")
        parts.append(f"Total entries: **{len(records)}**\n")

        # Field statistics and distributions
        if key_fields:
            parts.append("### Key Field Statistics\n")
            stats = self._compute_field_stats(records, key_fields)
            parts.append(stats)

        # Sample records
        parts.append(f"### Sample Records ({min(sample_size, len(records))})\n")
        sample = self._get_sample_records(records, sample_size)
        parts.append(sample)

        result = "\n".join(parts)
        if len(result) > self._max_tokens:
            result = result[:self._max_tokens] + "\n\n...(summary truncated due to size)"
        
        return result

    def summarize_table_stats(self, count: int, table_name: str, schema: dict[str, Any] | None = None) -> str:
        """Returns a concise summary of table metadata."""
        lines = [f"### Table Summary: {table_name}"]
        lines.append(f"- **Total Records:** {count:,}")
        
        if schema:
            attrs = schema.get("Attributes", [])
            lines.append(f"- **Columns:** {len(attrs)}")
            readable_attrs = [a.get("LogicalName") for a in attrs if not a.get("LogicalName", "").startswith("address")]
            lines.append(f"- **Key Attributes:** {', '.join(readable_attrs[:15])}...")
            
        return "\n".join(lines)

    def _compute_field_stats(self, records: list[dict[str, Any]], fields: list[str]) -> str:
        """Internal helper to compute field-level statistics."""
        stats = []
        for field in fields:
            values = [r.get(field) for r in records if r.get(field) is not None]
            if not values: continue

            # Numeric fields: Min, Max, Avg
            if all(isinstance(v, (int, float)) for v in values):
                v_min, v_max, v_avg = min(values), max(values), sum(values) / len(values)
                stats.append(f"- **{field}**: Min={v_min:,}, Max={v_max:,}, Avg={v_avg:,.2f}")
            # Text/Category fields: Distribution
            else:
                dist = {}
                for v in values: dist[v] = dist.get(v, 0) + 1
                top_3 = sorted(dist.items(), key=lambda x: x[1], reverse=True)[:3]
                dist_str = ", ".join([f"{k} ({v})" for k, v in top_3])
                stats.append(f"- **{field}**: Top Categories: {dist_str}")

        return "\n".join(stats) if stats else "No quantitative stats available."

    def _get_sample_records(self, records: list[dict[str, Any]], n: int) -> str:
        """Helper to format a few sample records."""
        from dataverse_mcp.services.formatter import DataFormatter
        formatter = DataFormatter()
        return formatter.format_records_table(records[:n])
