"""Real Dataverse client using MSAL for authentication and httpx for Web API calls."""

from __future__ import annotations

import logging
import urllib.parse
from typing import Any

import httpx
import msal
import structlog

logger = structlog.get_logger(__name__)

class DataverseClient:
    """Client for interacting with Microsoft Dataverse Web API."""

    def __init__(
        self,
        dataverse_url: str,
        client_id: str,
        client_secret: str,
        tenant_id: str,
    ) -> None:
        self.dataverse_url = dataverse_url.rstrip("/")
        self.api_url = f"{self.dataverse_url}/api/data/v9.2"
        self._client_id = client_id
        self._client_secret = client_secret
        self._tenant_id = tenant_id
        
        self._authority = f"https://login.microsoftonline.com/{tenant_id}"
        self._scope = [f"{self.dataverse_url}/.default"]
        
        self._msal_app = msal.ConfidentialClientApplication(
            client_id,
            authority=self._authority,
            client_credential=client_secret,
        )
        
        self._http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            headers={"Accept": "application/json", "OData-MaxVersion": "4.0", "OData-Version": "4.0"},
        )
        self._logger = logger.bind(component="dataverse_client")

    async def _get_access_token(self) -> str:
        """Acquires an access token using MSAL."""
        result = self._msal_app.acquire_token_silent(self._scope, account=None)
        if not result:
            result = self._msal_app.acquire_token_for_client(scopes=self._scope)
        
        if "access_token" in result:
            return result["access_token"]
        
        error_msg = result.get("error_description", result.get("error", "Unknown error"))
        self._logger.error("token_acquisition_failed", error=error_msg)
        raise Exception(f"Failed to acquire access token: {error_msg}")

    async def _request(self, method: str, path: str, **kwargs) -> Any:
        """Sends an authenticated request to the Dataverse Web API."""
        token = await self._get_access_token()
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {token}"
        
        url = f"{self.api_url}/{path.lstrip('/')}"
        
        response = await self._http_client.request(method, url, headers=headers, **kwargs)
        
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            self._logger.error("request_failed", url=url, status=response.status_code, text=response.text)
            raise e
            
        if response.status_code == 204: # No Content
            return None
            
        return response.json()

    async def list_tables(self) -> list[dict[str, Any]]:
        """Lists all tables (entities) in the environment."""
        path = "EntityDefinitions?$select=LogicalName,DisplayName,EntitySetName,PrimaryIdAttribute,PrimaryNameAttribute"
        result = await self._request("GET", path)
        return result.get("value", [])

    async def get_table_schema(self, table_name: str) -> dict[str, Any]:
        """Gets the schema (attributes) for a specific table."""
        path = f"EntityDefinitions(LogicalName='{table_name}')?$expand=Attributes($select=LogicalName,AttributeType,DisplayName)"
        return await self._request("GET", path)

    async def query_table(self, entity_set: str, **kwargs) -> list[dict[str, Any]]:
        """Queries records from an entity set with support for pagination."""
        params = []
        if select := kwargs.get("select"):
            params.append(f"$select={select}")
        if filter_query := kwargs.get("filter_query"):
            safe_chars = "() eqgtnl'"
            encoded_filter = urllib.parse.quote(filter_query, safe=safe_chars)
            params.append(f"$filter={encoded_filter}")
        if orderby := kwargs.get("orderby"):
            params.append(f"$orderby={orderby}")
        
        top = kwargs.get("top")
        fetch_all = kwargs.get("fetch_all", False)
        
        if top and not fetch_all:
            params.append(f"$top={top}")
        if skip := kwargs.get("skip"):
            params.append(f"$skip={skip}")
            
        query_string = "&".join(params)
        path = f"{entity_set}?{query_string}" if query_string else entity_set
        
        if fetch_all:
            return await self.fetch_all_records(path, max_records=kwargs.get("max_records", 5000))
        
        result = await self._request("GET", path)
        return result.get("value", [])

    async def fetch_all_records(self, initial_path: str, max_records: int = 5000) -> list[dict[str, Any]]:
        """Fetches all records by following pagination links up to max_records."""
        all_records = []
        current_path = initial_path
        
        while current_path and len(all_records) < max_records:
            result = await self._request("GET", current_path)
            batch = result.get("value", [])
            all_records.extend(batch)
            
            # Check for next page link
            next_link = result.get("@odata.nextLink")
            if next_link:
                # nextLink is usually a full URL, we need to extract the relative part for _request 
                # or modify _request to handle full URLs. 
                # Let's handle it by extracting everything after /api/data/v9.2/
                marker = "/api/data/v9.2/"
                if marker in next_link:
                    current_path = next_link.split(marker)[1]
                else:
                    current_path = None # Shouldn't happen with Dataverse
            else:
                current_path = None
                
        return all_records[:max_records]

    async def aggregate_table(self, entity_set: str, numeric_field: str = "", agg_type: str = "sum", filter_query: str = "", group_by: str = "") -> dict[str, Any] | list[dict[str, Any]]:
        """Performs server-side aggregation (Sum, Avg, Min, Max, Count) on a field using $apply.
        
        Args:
            entity_set: The table to query.
            numeric_field: The column name to aggregate. Not required for 'count'.
            agg_type: The type of aggregation ('sum', 'average', 'min', 'max', 'count').
            filter_query: Optional OData filter to apply before aggregating.
            group_by: Optional field to group results by.
            
        Returns:
            A dict (or list of dicts if group_by) containing the aggregated result(s).
        """
        # Handle count separately — it uses $count instead of field aggregation
        valid_aggs = {"sum": "sum", "avg": "average", "average": "average", "min": "min", "max": "max", "count": "count"}
        odata_agg = valid_aggs.get(agg_type.lower())
        if not odata_agg:
            raise ValueError(f"Unsupported aggregation type: {agg_type}")

        if odata_agg == "count":
            alias = "record_count"
            aggregate_expr = f"aggregate($count as {alias})"
        else:
            if not numeric_field:
                raise ValueError("numeric_field is required for non-count aggregations")
            alias = f"{numeric_field}_{odata_agg}"
            aggregate_expr = f"aggregate({numeric_field} with {odata_agg} as {alias})"
        
        if filter_query:
            safe_chars = "() eqgtnl'"
            encoded_filter = urllib.parse.quote(filter_query, safe=safe_chars)
            filter_prefix = f"filter({encoded_filter})/"
        else:
            filter_prefix = ""
        
        if group_by:
            apply_clause = f"{filter_prefix}groupby(({group_by}),{aggregate_expr})"
        else:
            apply_clause = f"{filter_prefix}{aggregate_expr}"
        
        path = f"{entity_set}?$apply={apply_clause}"
        
        result = await self._request("GET", path)
        value_list = result.get("value", [])
        
        if group_by:
            return value_list  # Return full list for grouped results
        
        if not value_list:
            return {alias: 0}
            
        return value_list[0]

    async def get_record(self, entity_set: str, record_id: str, **kwargs) -> dict[str, Any]:
        """Retrieves a single record by its ID."""
        params = []
        if select := kwargs.get("select"):
            params.append(f"$select={select}")
        if expand := kwargs.get("expand"):
            params.append(f"$expand={expand}")
            
        query_string = "&".join(params)
        path = f"{entity_set}({record_id}){('?' + query_string) if query_string else ''}"
        
        return await self._request("GET", path)

    async def get_record_count(self, entity_set: str, **kwargs) -> int:
        """Returns the total number of records in an entity set."""
        path = f"{entity_set}/$count"
        # Re-using _request but $count returns a raw number string
        token = await self._get_access_token()
        url = f"{self.api_url}/{path}"
        response = await self._http_client.get(url, headers={"Authorization": f"Bearer {token}"})
        response.raise_for_status()
        return int(response.text.strip().lstrip('\ufeff'))

    async def search_records(self, entity_set: str, search_field: str, search_term: str, **kwargs) -> list[dict[str, Any]]:
        """Searches for records using a filter."""
        filter_query = f"contains({search_field}, '{search_term}')"
        if existing_filter := kwargs.get("filter_query"):
            filter_query = f"({existing_filter}) and ({filter_query})"
            
        return await self.query_table(entity_set, filter_query=filter_query, **kwargs)

    async def close(self) -> None:
        """Closes the HTTP client."""
        await self._http_client.aclose()
