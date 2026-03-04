"""Configuration module — Type-safe environment variable management with Pydantic Settings."""

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
    """Application configuration.
    
    All values are read from environment variables or a `.env` file.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Azure AD / Entra ID ──────────────────────────────
    azure_tenant_id: str = Field(default="", description="Azure AD Tenant ID")
    azure_client_id: str = Field(default="", description="Azure AD Application (Client) ID")
    azure_client_secret: str = Field(default="", description="Azure AD Client Secret")

    # ── Dataverse ────────────────────────────────────────
    dataverse_url: str = Field(default="", description="Dataverse ortam URL'i")

    # ── MCP Server ───────────────────────────────────────
    mcp_server_name: str = Field(default="dataverse-mcp", description="MCP sunucu adı")
    mcp_transport: TransportType = Field(default=TransportType.SSE, description="MCP transport tipi")
    mcp_host: str = Field(default="0.0.0.0", description="HTTP sunucu host")
    mcp_port: int = Field(default=8080, ge=1, le=65535, description="HTTP sunucu port")

    # ── Data Processing ──────────────────────────────────
    max_page_size: int = Field(default=500, ge=1, le=5000)
    max_total_records: int = Field(default=5000, ge=1, le=50000)
    summary_max_tokens: int = Field(default=2000, ge=100, le=10000)

    # ── Logging ──────────────────────────────────────────
    log_level: str = Field(default="INFO")

    # ── Whitelist ────────────────────────────────────────
    allowed_tables: str = Field(
        default="",
        description="Virgülle ayrılmış izin verilen tablo listesi (boşsa hepsi)"
    )

    @property
    def dataverse_api_url(self) -> str:
        """Dataverse Web API base URL."""
        return f"{self.dataverse_url.rstrip('/')}/api/data/v9.2"

    @property
    def authority(self) -> str:
        """Azure AD authority URL."""
        return f"https://login.microsoftonline.com/{self.azure_tenant_id}"

    @property
    def scopes(self) -> list[str]:
        """OAuth2 scope listesi."""
        return [f"{self.dataverse_url.rstrip('/')}/.default"]

    def validate_azure_config(self) -> bool:
        """Checks if the required Azure configuration is present."""
        return all([
            self.azure_tenant_id,
            self.azure_client_id,
            self.azure_client_secret,
            self.dataverse_url,
        ])


def get_settings() -> Settings:
    """Returns a singleton Settings instance."""
    return Settings()
