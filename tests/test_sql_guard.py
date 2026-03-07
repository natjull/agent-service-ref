"""Tests for the read-only SQL guard in db_tools."""

from src.tools.db_tools import _guard_sql


class TestGuardSql:
    def test_select_allowed(self):
        assert _guard_sql("SELECT * FROM ref_sites") is None

    def test_select_with_where(self):
        assert _guard_sql("SELECT site_id FROM ref_sites WHERE reference = 'X'") is None

    def test_pragma_allowed(self):
        assert _guard_sql("PRAGMA table_info('ref_sites')") is None

    def test_explain_allowed(self):
        assert _guard_sql("EXPLAIN QUERY PLAN SELECT 1") is None

    def test_with_cte_allowed(self):
        assert _guard_sql("WITH cte AS (SELECT 1) SELECT * FROM cte") is None

    def test_drop_blocked(self):
        result = _guard_sql("DROP TABLE ref_sites")
        assert result is not None
        assert "BLOCKED" in result

    def test_delete_blocked(self):
        result = _guard_sql("DELETE FROM agent_resolutions")
        assert result is not None
        assert "BLOCKED" in result

    def test_insert_blocked(self):
        result = _guard_sql("INSERT INTO agent_resolutions VALUES ('a','b')")
        assert result is not None
        assert "BLOCKED" in result

    def test_update_blocked(self):
        result = _guard_sql("UPDATE ref_sites SET reference = 'X'")
        assert result is not None
        assert "BLOCKED" in result

    def test_alter_blocked(self):
        result = _guard_sql("ALTER TABLE ref_sites ADD COLUMN foo TEXT")
        assert result is not None
        assert "BLOCKED" in result

    def test_create_blocked(self):
        result = _guard_sql("CREATE TABLE evil (id INT)")
        assert result is not None
        assert "BLOCKED" in result

    def test_empty_blocked(self):
        result = _guard_sql("")
        assert result is not None
        assert "BLOCKED" in result

    def test_whitespace_only_blocked(self):
        result = _guard_sql("   ")
        assert result is not None
        assert "BLOCKED" in result

    def test_case_insensitive(self):
        assert _guard_sql("select * from ref_sites") is None
        result = _guard_sql("drop table ref_sites")
        assert result is not None

    def test_leading_whitespace_select(self):
        assert _guard_sql("  SELECT 1") is None

    def test_subquery_select(self):
        assert _guard_sql("SELECT * FROM (SELECT 1)") is None
