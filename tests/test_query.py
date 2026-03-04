"""OData query builder testleri."""

from dataverse_mcp.services.query import ODataQuery


class TestODataQuery:
    """ODataQuery builder testleri."""

    def test_basic_query(self) -> None:
        """Temel sorgu parametrelerini test eder."""
        query = ODataQuery("accounts")
        result = query.select("name", "revenue").top(10).build()
        assert result["select"] == "name,revenue"
        assert result["top"] == 10

    def test_filter(self) -> None:
        """Filtre parametresini test eder."""
        query = ODataQuery("accounts")
        result = query.filter("revenue gt 1000000").build()
        assert result["filter_query"] == "revenue gt 1000000"

    def test_multiple_filters(self) -> None:
        """Birden fazla filtrenin 'and' ile birleştirildiğini test eder."""
        query = ODataQuery("accounts")
        result = (
            query.filter("revenue gt 1000000").filter("statecode eq 0").build()
        )
        assert "and" in result["filter_query"]

    def test_orderby(self) -> None:
        """Sıralama parametresini test eder."""
        query = ODataQuery("contacts")
        result = query.orderby("createdon desc").build()
        assert result["orderby"] == "createdon desc"

    def test_expand(self) -> None:
        """Expand parametresini test eder."""
        query = ODataQuery("accounts")
        result = query.expand("primarycontactid").build()
        assert result["expand"] == "primarycontactid"

    def test_empty_query(self) -> None:
        """Boş sorgunun boş dict döndürdüğünü test eder."""
        query = ODataQuery("accounts")
        result = query.build()
        assert result == {}

    def test_chaining(self) -> None:
        """Builder pattern zincirleme çağrılarını test eder."""
        result = (
            ODataQuery("contacts")
            .select("fullname", "emailaddress1")
            .filter("statecode eq 0")
            .orderby("createdon desc")
            .top(50)
            .expand("parentcustomerid")
            .build()
        )
        assert "fullname,emailaddress1" == result["select"]
        assert result["top"] == 50
        assert "statecode eq 0" in result["filter_query"]
