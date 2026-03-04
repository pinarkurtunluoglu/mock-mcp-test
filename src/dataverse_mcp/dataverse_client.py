"""Dataverse Web API async client implementation.

Makes HTTP requests to Dataverse via the OData protocol.
Includes automatic pagination, retry, and error handling.
"""

from __future__ import annotations

from typing import Any

import httpx
import structlog

from dataverse_mcp.auth import DataverseAuth
from dataverse_mcp.config import Settings

logger = structlog.get_logger(__name__)


class DataverseError(Exception):
    """Exception raised for Dataverse API errors."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class DataverseClient:
    """Async client for interacting with the Microsoft Dataverse Web API."""

    def __init__(self, settings: Settings, auth: DataverseAuth) -> None:
        self._settings = settings
        self._auth = auth
        self._client: httpx.AsyncClient | None = None
        self._logger = logger.bind(component="dataverse_client")

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            token = await self._auth.get_access_token()
            self._client = httpx.AsyncClient(
                base_url=self._settings.dataverse_api_url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                    "OData-MaxVersion": "4.0",
                    "OData-Version": "4.0",
                    "Prefer": f"odata.maxpagesize={self._settings.max_page_size}",
                },
                timeout=httpx.Timeout(30.0),
            )
        return self._client

    async def _refresh_token(self) -> None:
        token = await self._auth.get_access_token()
        if self._client:
            self._client.headers["Authorization"] = f"Bearer {token}"

    async def _request(
        self, method: str, url: str, params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        client = await self._get_client()

        for attempt in range(2):
            try:
                response = await client.request(method, url, params=params)

                if response.status_code == 401 and attempt == 0:
                    self._logger.info("token_expired_retry")
                    await self._refresh_token()
                    continue

                if response.status_code >= 400:
                    error_body = response.text
                    self._logger.error("api_error", status=response.status_code, body=error_body[:500])
                    raise DataverseError(
                        f"Dataverse API error ({response.status_code}): {error_body[:200]}",
                        status_code=response.status_code,
                    )
                return response.json()

            except httpx.HTTPError as e:
                self._logger.error("http_error", error=str(e))
                raise DataverseError(f"HTTP error: {e}") from e

        raise DataverseError("Maximum retry count reached")

    # ── Public API ───────────────────────────────────────

    async def list_tables(self) -> list[dict[str, Any]]:
        """Lists all tables in Dataverse."""
        self._logger.info("list_tables")
        result = await self._request(
            "GET", "/EntityDefinitions",
            params={
                "$select": "LogicalName,DisplayName,EntitySetName,Description,PrimaryIdAttribute,PrimaryNameAttribute",
                "$filter": "IsCustomizable/Value eq true",
            },
        )
        entities = result.get("value", [])
        self._logger.info("tables_listed", count=len(entities))
        return entities

    async def get_table_schema(self, table_name: str) -> dict[str, Any]:
        """Retrieves the schema information for a specific table."""
        self._logger.info("get_schema", table=table_name)
        entity_result = await self._request(
            "GET", f"/EntityDefinitions(LogicalName='{table_name}')",
            params={"$select": "LogicalName,DisplayName,EntitySetName,Description,PrimaryIdAttribute,PrimaryNameAttribute"},
        )
        attrs_result = await self._request(
            "GET", f"/EntityDefinitions(LogicalName='{table_name}')/Attributes",
            params={
                "$select": "LogicalName,DisplayName,AttributeType,IsValidForRead,IsValidForCreate,RequiredLevel",
                "$filter": "IsValidForRead eq true",
            },
        )
        entity_result["Attributes"] = attrs_result.get("value", [])
        return entity_result

    async def query_table(
        self, entity_set: str, *,
        select: str | None = None, filter_query: str | None = None,
        orderby: str | None = None, top: int | None = None, expand: str | None = None,
    ) -> list[dict[str, Any]]:
        """Fetches table data using an OData query."""
        params: dict[str, Any] = {}
        if select: params["$select"] = select
        if filter_query: params["$filter"] = filter_query
        if orderby: params["$orderby"] = orderby
        if top: params["$top"] = str(top)
        if expand: params["$expand"] = expand

        self._logger.info("query_table", entity_set=entity_set, params=params)
        all_records: list[dict[str, Any]] = []
        url = f"/{entity_set}"
        max_records = top or self._settings.max_total_records

        while url and len(all_records) < max_records:
            result = await self._request("GET", url, params=params if url.startswith("/") else None)
            all_records.extend(result.get("value", []))
            next_link = result.get("@odata.nextLink")
            if next_link and len(all_records) < max_records:
                url = next_link
                params = {}
            else:
                break

        self._logger.info("query_complete", total_records=len(all_records))
        return all_records[:max_records]

    async def get_record(
        self, entity_set: str, record_id: str, *,
        select: str | None = None, expand: str | None = None,
    ) -> dict[str, Any]:
        """Retrieves a single record by ID."""
        params: dict[str, Any] = {}
        if select: params["$select"] = select
        if expand: params["$expand"] = expand
        self._logger.info("get_record", entity_set=entity_set, record_id=record_id)
        return await self._request("GET", f"/{entity_set}({record_id})", params=params or None)

    async def get_record_count(self, entity_set: str, filter_query: str | None = None) -> int:
        """Returns the count of records in a table."""
        params: dict[str, Any] = {"$count": "true", "$top": "0"}
        if filter_query: params["$filter"] = filter_query
        result = await self._request("GET", f"/{entity_set}", params=params)
        return result.get("@odata.count", 0)

    async def search_records(
        self, entity_set: str, search_field: str, search_term: str, *,
        select: str | None = None, top: int = 20,
    ) -> list[dict[str, Any]]:
        """Searches for a keyword in a specific field."""
        filter_query = f"contains({search_field}, '{search_term}')"
        return await self.query_table(entity_set, select=select, filter_query=filter_query, top=top)

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._logger.info("client_closed")
