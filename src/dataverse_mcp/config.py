"""Configuration module — Type-safe environment variable management for the Mock MCP version."""

from __future__ import annotations

from enum import Enum

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class TransportType(str, Enum):
    """MCP Server transport types."""

    STDIO = "stdio"
    SSE = "sse"
    STREAMABLE_HTTP = "streamable-http"


class Settings(BaseSettings):
    """Application configuration for Mock-Only mode."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── MCP Server ───────────────────────────────────────
    mcp_server_name: str = Field(default="mock-mcp-test", description="MCP server name")
    mcp_transport: TransportType = Field(default=TransportType.SSE, description="MCP transport type")
    mcp_host: str = Field(default="0.0.0.0", description="HTTP server host")
    mcp_port: int = Field(default=8080, ge=1, le=65535, description="HTTP server port")

    # ── Data Processing ──────────────────────────────────
    max_page_size: int = Field(default=500, ge=1, le=5000)
    max_total_records: int = Field(default=5000, ge=1, le=50000)
    summary_max_tokens: int = Field(default=2000, ge=100, le=10000)

    # ── Logging ──────────────────────────────────────────
    log_level: str = Field(default="INFO")

    # ── Whitelist ────────────────────────────────────────
    allowed_tables: str = Field(
        default="",
        description="Comma-separated list of allowed tables (empty for all)"
    )


def get_settings() -> Settings:
    """Returns a singleton Settings instance."""
    return Settings()
