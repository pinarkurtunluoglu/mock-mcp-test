"""OData query builder service."""

from __future__ import annotations

from typing import Any


class ODataQuery:
    """Builds OData query strings for Dataverse Web API requests."""

    def __init__(self, select: str | None = None, filter_query: str | None = None) -> None:
        self._select = select
        self._filter = filter_query
        self._orderby: str | None = None
        self._expand: str | None = None
        self._top: int | None = None

    def select(self, fields: str) -> ODataQuery:
        """Sets the $select parameter."""
        self._select = fields
        return self

    def filter(self, query: str) -> ODataQuery:
        """Sets the $filter parameter."""
        self._filter = query
        return self

    def orderby(self, order: str) -> ODataQuery:
        """Sets the $orderby parameter."""
        self._orderby = order
        return self

    def expand(self, relation: str) -> ODataQuery:
        """Sets the $expand parameter."""
        self._expand = relation
        return self

    def top(self, limit: int) -> ODataQuery:
        """Sets the $top parameter."""
        self._top = limit
        return self

    def build(self) -> dict[str, Any]:
        """Builds the dictionary of query parameters."""
        params: dict[str, Any] = {}
        if self._select: params["$select"] = self._select
        if self._filter: params["$filter"] = self._filter
        if self._orderby: params["$orderby"] = self._orderby
        if self._expand: params["$expand"] = self._expand
        if self._top: params["$top"] = self._top
        return params

    @classmethod
    def from_params(
        cls, select: str | None = None, filter_query: str | None = None,
        orderby: str | None = None, top: int | None = None, expand: str | None = None,
    ) -> dict[str, Any]:
        """Directly builds parameters from individual arguments."""
        query = cls(select, filter_query)
        if orderby: query.orderby(orderby)
        if top: query.top(top)
        if expand: query.expand(expand)
        return query.build()
