"""Integration tests for db_tools."""

from __future__ import annotations

import asyncio
import sqlite3

import pytest

from src.tools import db_tools

_query_db = db_tools.query_db.handler
_list_tables = db_tools.list_tables.handler
_describe_table = db_tools.describe_table.handler
_fetch_ctx = db_tools.fetch_service_context.handler


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def _configure_db(realistic_db):
    db_tools.configure(realistic_db)
    yield


class TestListTables:
    def test_returns_all_tables(self):
        result = _run(_list_tables({}))
        text = result["content"][0]["text"]
        assert "service_master_active" in text
        assert "ref_sites" in text
        assert "agent_resolutions" in text

    def test_returns_row_counts(self):
        result = _run(_list_tables({}))
        text = result["content"][0]["text"]
        # service_master_active has 5 rows
        assert "service_master_active | 5" in text


class TestDescribeTable:
    def test_returns_columns_and_stats(self):
        result = _run(_describe_table({"table_name": "service_master_active"}))
        text = result["content"][0]["text"]
        assert "service_id" in text
        assert "principal_client" in text
        assert "5 rows" in text

    def test_rejects_malicious_table_name(self):
        result = _run(_describe_table({"table_name": "x; DROP TABLE y"}))
        text = result["content"][0]["text"]
        assert "ERROR" in text
        assert "invalid table name" in text

    def test_rejects_table_with_special_chars(self):
        result = _run(_describe_table({"table_name": "table--name"}))
        text = result["content"][0]["text"]
        assert "ERROR" in text


class TestQueryDb:
    def test_select_works(self):
        result = _run(_query_db({"sql": "SELECT service_id FROM service_master_active LIMIT 2"}))
        text = result["content"][0]["text"]
        assert "SVC-00" in text

    def test_write_blocked_by_guard(self):
        result = _run(_query_db({"sql": "DELETE FROM service_master_active"}))
        text = result["content"][0]["text"]
        assert "BLOCKED" in text

    def test_pragma_query_only_blocks_writes(self, realistic_db):
        """Even if guard is bypassed, PRAGMA query_only blocks writes."""
        con = db_tools._connect(read_only=True)
        try:
            with pytest.raises(sqlite3.OperationalError):
                con.execute("INSERT INTO ref_sites VALUES ('X','X','x')")
        finally:
            con.close()


class TestFetchServiceContext:
    def test_returns_expected_structure(self):
        result = _run(_fetch_ctx({"service_id": "SVC-001"}))
        text = result["content"][0]["text"]
        assert "evidences" in text
        assert "review_items" in text
        assert "top_sites" in text
        assert "parties" in text

    def test_missing_service_returns_empty(self):
        result = _run(_fetch_ctx({"service_id": "NONEXISTENT"}))
        text = result["content"][0]["text"]
        # Should return valid JSON with empty lists
        assert "evidences" in text
