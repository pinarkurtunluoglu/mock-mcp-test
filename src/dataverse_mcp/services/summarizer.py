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
        """Summarizes a list of Dataverse records with statistical depth."""
        if not records:
            return f"No matching records found in table **{table_name}**."

        parts = []
        parts.append(f"## {table_name.capitalize()} Analysis Summary")
        parts.append(f"- **Total Records Analyzed:** {len(records):,}")
        
        # Identify numeric vs categorical fields if key_fields not provided
        if not key_fields and records:
            # Auto-detect some interesting fields (numeric or short strings)
            key_fields = [k for k, v in records[0].items() 
                         if not k.startswith("@") and isinstance(v, (int, float, str))][:10]

        # Field statistics and distributions
        if key_fields:
            parts.append("\n### Statistical Overview\n")
            stats = self._compute_field_stats(records, key_fields)
            parts.append(stats)

        # Sample records
        parts.append(f"\n### Data Samples ({min(sample_size, len(records))})\n")
        sample = self._get_sample_records(records, sample_size)
        parts.append(sample)

        result = "\n".join(parts)
        if len(result) > 12000:  # Tuned for GPT-5.2 context window
            result = result[:12000] + "\n\n...(summary truncated due to size)"
        
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
        """Internal helper to compute deep field-level statistics."""
        stats = []
        for field in fields:
            try:
                values = [r.get(field) for r in records if r.get(field) is not None]
                if not values: continue

                # Numeric fields: Min, Max, Avg, Sum
                if all(isinstance(v, (int, float)) for v in values):
                    v_min, v_max = min(values), max(values)
                    v_sum = sum(values)
                    v_avg = v_sum / len(values)
                    stats.append(f"- **{field}** (Numeric):")
                    stats.append(f"  - Sum: `{v_sum:,.2f}` | Avg: `{v_avg:,.2f}`")
                    stats.append(f"  - Range: `{v_min:,.2f}` to `{v_max:,.2f}`")
                
                # Text/Category fields: Distribution
                else:
                    dist = {}
                    for v in values: 
                        v_str = str(v)[:50] # Truncate long strings
                        dist[v_str] = dist.get(v_str, 0) + 1
                    
                    top_n = sorted(dist.items(), key=lambda x: x[1], reverse=True)[:5]
                    total_non_null = len(values)
                    
                    stats.append(f"- **{field}** (Categorical):")
                    dist_lines = []
                    for k, v in top_n:
                        pct = (v / total_non_null) * 100
                        dist_lines.append(f"    - {k}: {v} ({pct:.1f}%)")
                    stats.append("\n".join(dist_lines))
            except Exception as e:
                stats.append(f"- **{field}**: Error computing stats ({e})")

        return "\n".join(stats) if stats else "No quantitative stats available."

    def _get_sample_records(self, records: list[dict[str, Any]], n: int) -> str:
        """Helper to format a few sample records."""
        from dataverse_mcp.services.formatter import DataFormatter
        formatter = DataFormatter()
        return formatter.format_records_table(records[:n])
