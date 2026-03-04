"""Formatter modülü testleri."""

import pytest

from dataverse_mcp.services.formatter import DataFormatter


class TestDataFormatter:
    """DataFormatter testleri."""

    def test_format_record(self) -> None:
        """Tek kayıt formatlama."""
        record = {"name": "Acme Corp", "revenue": 1500000, "city": "İstanbul"}
        result = DataFormatter.format_record(record, "accounts")
        assert "Acme Corp" in result
        assert "accounts" in result

    def test_format_record_filters_metadata(self) -> None:
        """OData metadata alanlarının filtrelendiğini test eder."""
        record = {
            "@odata.etag": "W/123",
            "_ownerid_value": "some-guid",
            "name": "Test",
        }
        result = DataFormatter.format_record(record)
        assert "@odata.etag" not in result
        assert "_ownerid_value" not in result
        assert "name" in result

    def test_format_records_table_empty(self) -> None:
        """Boş kayıt listesinde uygun mesaj döndüğünü test eder."""
        result = DataFormatter.format_records_table([])
        assert "bulunamadı" in result

    def test_format_records_table(self) -> None:
        """Markdown tablosu oluşturulduğunu test eder."""
        records = [
            {"name": "Acme", "city": "İstanbul"},
            {"name": "Beta", "city": "Ankara"},
        ]
        result = DataFormatter.format_records_table(records, columns=["name", "city"])
        assert "| name | city |" in result
        assert "Acme" in result
        assert "Beta" in result

    def test_format_table_list(self) -> None:
        """Tablo listesinin formatlandığını test eder."""
        entities = [
            {
                "LogicalName": "account",
                "DisplayName": {"UserLocalizedLabel": {"Label": "Firma"}},
                "EntitySetName": "accounts",
            },
        ]
        result = DataFormatter.format_table_list(entities)
        assert "account" in result
        assert "accounts" in result

    def test_format_schema(self) -> None:
        """Şema formatlama."""
        schema = {
            "LogicalName": "contact",
            "EntitySetName": "contacts",
            "PrimaryIdAttribute": "contactid",
            "PrimaryNameAttribute": "fullname",
            "Attributes": [
                {
                    "LogicalName": "fullname",
                    "AttributeType": "String",
                    "RequiredLevel": {"Value": "Required"},
                },
            ],
        }
        result = DataFormatter.format_schema(schema)
        assert "contact" in result
        assert "fullname" in result
        assert "String" in result

    def test_format_value_none(self) -> None:
        """None değerin '—' olarak formatlandığını test eder."""
        assert DataFormatter._format_value(None) == "—"

    def test_format_value_bool(self) -> None:
        """Boolean değerlerin emoji ile formatlandığını test eder."""
        assert "✅" in DataFormatter._format_value(True)
        assert "❌" in DataFormatter._format_value(False)

    def test_format_cell_truncation(self) -> None:
        """Uzun hücrelerin kısaltıldığını test eder."""
        long_text = "A" * 100
        result = DataFormatter._format_cell(long_text, max_length=20)
        assert len(result) <= 20
        assert result.endswith("…")
