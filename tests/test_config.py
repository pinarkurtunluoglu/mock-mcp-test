"""Config modülü testleri."""

import pytest


class TestSettings:
    """Settings sınıfı testleri."""

    def test_default_values(self) -> None:
        from dataverse_mcp.config import Settings
        s = Settings(
            _env_file=None,
            azure_tenant_id="", azure_client_id="", azure_client_secret="", dataverse_url="",
        )
        assert s.mcp_server_name == "dataverse-mcp"
        assert s.max_page_size == 500
        assert s.max_total_records == 5000
        assert s.summary_max_tokens == 2000
        assert s.log_level == "INFO"

    def test_dataverse_api_url(self) -> None:
        from dataverse_mcp.config import Settings
        s = Settings(
            _env_file=None,
            dataverse_url="https://myorg.crm.dynamics.com",
            azure_tenant_id="t", azure_client_id="c", azure_client_secret="s",
        )
        assert s.dataverse_api_url == "https://myorg.crm.dynamics.com/api/data/v9.2"

    def test_dataverse_api_url_trailing_slash(self) -> None:
        from dataverse_mcp.config import Settings
        s = Settings(
            _env_file=None,
            dataverse_url="https://myorg.crm.dynamics.com/",
            azure_tenant_id="t", azure_client_id="c", azure_client_secret="s",
        )
        assert s.dataverse_api_url == "https://myorg.crm.dynamics.com/api/data/v9.2"

    def test_authority_url(self) -> None:
        from dataverse_mcp.config import Settings
        s = Settings(
            _env_file=None,
            azure_tenant_id="my-tenant-123", azure_client_id="c", azure_client_secret="s",
            dataverse_url="https://x.crm.dynamics.com",
        )
        assert s.authority == "https://login.microsoftonline.com/my-tenant-123"

    def test_scopes(self) -> None:
        from dataverse_mcp.config import Settings
        s = Settings(
            _env_file=None,
            dataverse_url="https://myorg.crm.dynamics.com",
            azure_tenant_id="t", azure_client_id="c", azure_client_secret="s",
        )
        assert s.scopes == ["https://myorg.crm.dynamics.com/.default"]

    def test_validate_azure_config_complete(self) -> None:
        from dataverse_mcp.config import Settings
        s = Settings(
            _env_file=None,
            azure_tenant_id="tenant", azure_client_id="client",
            azure_client_secret="secret", dataverse_url="https://org.crm.dynamics.com",
        )
        assert s.validate_azure_config() is True

    def test_validate_azure_config_incomplete(self) -> None:
        from dataverse_mcp.config import Settings
        s = Settings(
            _env_file=None,
            azure_tenant_id="tenant", azure_client_id="",
            azure_client_secret="secret", dataverse_url="https://org.crm.dynamics.com",
        )
        assert s.validate_azure_config() is False

    def test_transport_type(self) -> None:
        from dataverse_mcp.config import Settings, TransportType
        s = Settings(
            _env_file=None,
            mcp_transport="sse",
            azure_tenant_id="t", azure_client_id="c", azure_client_secret="s",
            dataverse_url="https://x.crm.dynamics.com",
        )
        assert s.mcp_transport == TransportType.SSE
