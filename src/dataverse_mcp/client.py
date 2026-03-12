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

    async def query_table(self, entity_set: str, **kwargs) -> dict[str, Any]:
        """Queries records from an entity set with support for pagination (skiptoken)."""
        if next_link := kwargs.get("next_link"):
            # If a full nextLink or skiptoken path is provided, use it
            marker = "/api/data/v9.2/"
            current_path = next_link.split(marker)[1] if marker in next_link else next_link
            return await self._request("GET", current_path)
            
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
            
        query_string = "&".join(params)
        path = f"{entity_set}?{query_string}" if query_string else entity_set
        
        if fetch_all:
            return {"value": await self.fetch_all_records(path, max_records=kwargs.get("max_records", 5000))}
        
        return await self._request("GET", path)

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
            # Add comma to safe chars so contains(a, b) isn't mangled into contains(a%2C b)
            safe_chars = "() eqgtnl',"
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

    async def calculate_weighted_average(
        self,
        entity_set: str,
        value_field: str,
        weight_field: str,
        filter_query: str = "",
        group_by: str = ""
    ) -> float | list[dict[str, Any]]:
        """Calculates weighted average server-side by grouping and summing.
        
        Args:
            entity_set: The table to query.
            value_field: The field to average (e.g., age).
            weight_field: The field to use as weight (e.g., quantity).
            filter_query: Optional filter.
            group_by: Optional grouping (e.g., site). If provided, returns a list of results per group.
        """
        # Always group by value_field to get counts/sums for each unique value
        # If the user also wants to group by something else (e.g., site), we include it
        inner_groups = [value_field]
        if group_by:
            inner_groups.append(group_by)
        
        inner_group_str = ",".join(inner_groups)
        
        # 1. Get sum of weights grouped by value (and optional extra group)
        results = await self.aggregate_table(
            entity_set,
            numeric_field=weight_field,
            agg_type="sum",
            group_by=inner_group_str,
            filter_query=filter_query
        )
        
        if not isinstance(results, list):
            results = [results]

        weight_sum_alias = f"{weight_field}_sum"

        if group_by:
            # Handle grouped results (e.g., weighted average per site)
            grouped_data = {}
            for row in results:
                g_val = row.get(group_by)
                if g_val not in grouped_data:
                    grouped_data[g_val] = {"weighted_sum": 0, "total_weight": 0}
                
                val = row.get(value_field, 0)
                weight = row.get(weight_sum_alias, 0)
                
                grouped_data[g_val]["weighted_sum"] += (val * weight)
                grouped_data[g_val]["total_weight"] += weight
            
            final_results = []
            for g_val, data in grouped_data.items():
                avg = data["weighted_sum"] / data["total_weight"] if data["total_weight"] > 0 else 0
                final_results.append({
                    group_by: g_val,
                    f"{value_field}_weighted_avg": avg,
                    "total_weight": data["total_weight"]
                })
            return final_results
        else:
            # Handle single result
            total_weighted_sum = 0
            total_weight = 0
            for row in results:
                val = row.get(value_field, 0)
                weight = row.get(weight_sum_alias, 0)
                total_weighted_sum += (val * weight)
                total_weight += weight
            
            return total_weighted_sum / total_weight if total_weight > 0 else 0.0

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

    async def search_records(self, entity_set: str, search_field: str, search_term: str, **kwargs) -> dict[str, Any]:
        """Searches for records using a filter."""
        filter_query = f"contains({search_field}, '{search_term}')"
        if existing_filter := kwargs.get("filter_query"):
            filter_query = f"({existing_filter}) and ({filter_query})"
            
        return await self.query_table(entity_set, filter_query=filter_query, **kwargs)

    async def close(self) -> None:
        """Closes the HTTP client."""
        await self._http_client.aclose()
