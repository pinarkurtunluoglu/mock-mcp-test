"""Response Guard — Ensures MCP tool outputs stay within AI context budget.

Best practice for MCP servers: every tool response should be compact enough
for the AI model to process alongside its conversation history and instructions.
"""

from __future__ import annotations

# ── Configuration ─────────────────────────────────────────
# Tuned for GPT-5.2 / ChatGPT Reasoning (~128K context window)
# 12000 chars ≈ 3000 tokens — generous but cost-efficient
MAX_RESPONSE_CHARS = 12000
TRUNCATION_FOOTER = "\n\n> ⚠️ **Yanıt boyut limiti aşıldığı için kısaltıldı.** Daha dar bir filtre veya daha küçük `top` değeri kullanarak tekrar deneyin."


def guard(response: str, max_chars: int = MAX_RESPONSE_CHARS) -> str:
    """Truncates a tool response if it exceeds the character budget.
    
    Preserves the first part of the response (which usually contains
    the header and most important rows) and appends a truncation warning.
    """
    if len(response) <= max_chars:
        return response
    
    # Find a clean break point (end of a line) near the budget
    cut_point = response.rfind("\n", 0, max_chars - len(TRUNCATION_FOOTER))
    if cut_point < max_chars // 2:
        # If we can't find a good line break, just cut at the budget
        cut_point = max_chars - len(TRUNCATION_FOOTER)
    
    return response[:cut_point] + TRUNCATION_FOOTER
