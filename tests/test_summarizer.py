"""Summarizer modülü testleri."""

import pytest

from dataverse_mcp.services.summarizer import DataSummarizer


@pytest.fixture
def summarizer() -> DataSummarizer:
    return DataSummarizer(max_tokens=2000)


@pytest.fixture
def sample_records() -> list[dict]:
    return [
        {"name": "Acme Corp", "revenue": 1500000, "city": "İstanbul", "status": "Active"},
        {"name": "Beta Ltd", "revenue": 750000, "city": "Ankara", "status": "Active"},
        {"name": "Gamma Inc", "revenue": 2200000, "city": "İstanbul", "status": "Inactive"},
        {"name": "Delta GmbH", "revenue": 500000, "city": "İzmir", "status": "Active"},
        {"name": "Epsilon SA", "revenue": 3100000, "city": "İstanbul", "status": "Active"},
    ]


class TestDataSummarizer:

    def test_empty_records(self, summarizer: DataSummarizer) -> None:
        result = summarizer.summarize_records([], "accounts")
        assert "bulunamadı" in result

    def test_summarize_with_records(self, summarizer: DataSummarizer, sample_records: list[dict]) -> None:
        result = summarizer.summarize_records(sample_records, "accounts")
        assert "accounts" in result
        assert "5" in result

    def test_numeric_stats(self, summarizer: DataSummarizer, sample_records: list[dict]) -> None:
        result = summarizer.summarize_records(sample_records, "accounts", key_fields=["revenue"])
        assert "revenue" in result
        assert "min" in result or "max" in result

    def test_text_distribution(self, summarizer: DataSummarizer, sample_records: list[dict]) -> None:
        result = summarizer.summarize_records(sample_records, "accounts", key_fields=["city", "status"])
        assert "İstanbul" in result or "benzersiz" in result

    def test_sample_records_displayed(self, summarizer: DataSummarizer, sample_records: list[dict]) -> None:
        result = summarizer.summarize_records(sample_records, "accounts", sample_size=3)
        assert "Kayıt" in result

    def test_token_limit_truncation(self) -> None:
        s = DataSummarizer(max_tokens=50)
        records = [{"name": f"Company {i}", "desc": "x" * 100} for i in range(20)]
        result = s.summarize_records(records, "test_table")
        assert "kısaltıldı" in result

    def test_table_stats_summary(self, summarizer: DataSummarizer) -> None:
        result = summarizer.summarize_table_stats(
            record_count=12500, table_name="contacts",
            schema={
                "PrimaryNameAttribute": "fullname",
                "Attributes": [
                    {"LogicalName": "fullname", "AttributeType": "String"},
                    {"LogicalName": "emailaddress1", "AttributeType": "String"},
                    {"LogicalName": "age", "AttributeType": "Integer"},
                ],
            },
        )
        assert "12,500" in result
        assert "contacts" in result
        assert "3" in result

    def test_key_field_detection(self, summarizer: DataSummarizer) -> None:
        records = [{
            "@odata.etag": "W/123", "_ownerid_value": "guid",
            "accountid": "00000000-0000-0000-0000-000000000000",
            "name": "Test", "revenue": 100,
        }]
        result = summarizer.summarize_records(records, "accounts")
        assert "@odata.etag" not in result
        assert "_ownerid_value" not in result
