"""Authentication module — MSAL-based token management for Azure AD / Entra ID.

Acquires an access token for Dataverse Web API using the Client-Credentials flow.
"""

from __future__ import annotations

import msal
import structlog

from dataverse_mcp.config import Settings

logger = structlog.get_logger(__name__)


class AuthenticationError(Exception):
    """Exception raised when an authentication token cannot be acquired."""


class DataverseAuth:
    """MSAL-based token management for Dataverse.

    Uses the Client-Credentials flow — no user interaction required.
    Token caching is handled automatically by MSAL.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._app: msal.ConfidentialClientApplication | None = None
        self._logger = logger.bind(component="auth")

    def _get_app(self) -> msal.ConfidentialClientApplication:
        """Returns the MSAL ConfidentialClientApplication instance (lazy initialization)."""
        if self._app is None:
            self._logger.info(
                "msal_app_init",
                tenant_id=self._settings.azure_tenant_id[:8] + "...",
                client_id=self._settings.azure_client_id[:8] + "...",
            )
            self._app = msal.ConfidentialClientApplication(
                client_id=self._settings.azure_client_id,
                client_credential=self._settings.azure_client_secret,
                authority=self._settings.authority,
            )
        return self._app

    async def get_access_token(self) -> str:
        """Returns a valid access token.

        Checks the cache first, then acquires a new token if needed.

        Raises:
            AuthenticationError: If the token cannot be acquired.
        """
        app = self._get_app()
        scopes = self._settings.scopes

        # Try to get token from cache first
        result = app.acquire_token_silent(scopes=scopes, account=None)

        if not result:
            self._logger.info("token_acquire", method="client_credentials")
            result = app.acquire_token_for_client(scopes=scopes)

        if "access_token" in result:
            self._logger.debug("token_acquired", expires_in=result.get("expires_in"))
            return result["access_token"]

        error = result.get("error", "unknown")
        error_description = result.get("error_description", "No description")
        self._logger.error("token_error", error=error, description=error_description)
        raise AuthenticationError(f"Could not acquire token: {error} — {error_description}")

    async def validate_connection(self) -> bool:
        """Validates the Dataverse connection by attempting to acquire a token."""
        try:
            token = await self.get_access_token()
            return bool(token)
        except AuthenticationError:
            return False
