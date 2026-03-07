"""Tests for benchmark helpers."""

from __future__ import annotations

import sqlite3

import pytest

from src.benchmark import (
    BenchmarkMetrics,
    _backup_agent_tables,
    _clear_agent_tables,
    _collect_metrics,
    _fetch_service_ids,
    _print_comparison,
    _restore_agent_tables,
)


def test_backup_restore_roundtrip(realistic_db):
    con = sqlite3.connect(str(realistic_db))
    con.execute(
        """
        INSERT INTO agent_resolutions
        (resolution_id, service_id, confidence, party_final_id, justification, status, evidence_count)
        VALUES ('R1', 'SVC-001', 'high', 'P-ACME', 'ok justification', 'validated', 3)
        """
    )
    con.execute(
        """
        INSERT INTO agent_evidence
        (evidence_id, resolution_id, service_id, evidence_type, description, score)
        VALUES ('E1', 'R1', 'SVC-001', 'party', 'party ok', 80)
        """
    )
    con.commit()
    con.close()

    backup = _backup_agent_tables(realistic_db)
    _clear_agent_tables(realistic_db)
    _restore_agent_tables(realistic_db, backup)

    con = sqlite3.connect(str(realistic_db))
    try:
        assert con.execute("SELECT COUNT(*) FROM agent_resolutions").fetchone()[0] == 1
        assert con.execute("SELECT COUNT(*) FROM agent_evidence").fetchone()[0] == 1
        assert con.execute(
            "SELECT party_final_id FROM agent_resolutions WHERE service_id = 'SVC-001'"
        ).fetchone()[0] == "P-ACME"
    finally:
        con.close()


def test_clear_agent_tables(realistic_db):
    con = sqlite3.connect(str(realistic_db))
    con.execute(
        """
        INSERT INTO agent_resolutions
        (resolution_id, service_id, confidence, justification, status, evidence_count)
        VALUES ('R1', 'SVC-001', 'low', 'cleanup check', 'proposed', 1)
        """
    )
    con.commit()
    con.close()

    _clear_agent_tables(realistic_db)

    con = sqlite3.connect(str(realistic_db))
    try:
        assert con.execute("SELECT COUNT(*) FROM agent_resolutions").fetchone()[0] == 0
        assert con.execute("SELECT COUNT(*) FROM agent_evidence").fetchone()[0] == 0
    finally:
        con.close()


def test_collect_metrics(realistic_db):
    con = sqlite3.connect(str(realistic_db))
    con.execute(
        """
        INSERT INTO agent_resolutions
        (resolution_id, service_id, confidence, party_final_id, justification, status, evidence_count)
        VALUES
        ('R1', 'SVC-001', 'high', 'P-ACME', 'ok justification', 'validated', 3),
        ('R2', 'SVC-002', 'medium', NULL, 'party unresolved', 'needs_review', 2),
        ('R3', 'SVC-003', 'low', NULL, 'rejected loop', 'rejected', 1)
        """
    )
    con.commit()
    con.close()

    metrics = _collect_metrics(
        realistic_db,
        model="sonnet",
        service_ids=["SVC-001", "SVC-002", "SVC-003", "SVC-004"],
        duration_seconds=1.2,
        total_cost_usd=0.42,
        num_turns=9,
    )

    assert metrics == BenchmarkMetrics(
        model="sonnet",
        duration_seconds=1.2,
        num_turns=9,
        total_cost_usd=0.42,
        services_targeted=4,
        services_touched=3,
        services_proposed=0,
        services_validated=1,
        services_needs_review=1,
        services_rejected=1,
        confidence_high=1,
        confidence_medium=1,
        confidence_low=1,
        party_final_coverage=25.0,
        party_final_gaps=3,
    )


def test_fetch_service_ids_requires_service_id_column(realistic_db):
    with pytest.raises(ValueError, match="service_id"):
        _fetch_service_ids(realistic_db, "SELECT principal_client FROM service_master_active")


def test_fetch_service_ids_rejects_empty_result(realistic_db):
    with pytest.raises(ValueError, match="no service_id"):
        _fetch_service_ids(
            realistic_db,
            "SELECT service_id FROM service_master_active WHERE service_id = 'NOPE'",
        )


def test_print_comparison_no_crash(capsys):
    metrics = [
        BenchmarkMetrics(
            model="opus",
            duration_seconds=1.0,
            num_turns=10,
            total_cost_usd=1.5,
            services_targeted=5,
            services_touched=5,
            services_proposed=0,
            services_validated=3,
            services_needs_review=2,
            services_rejected=0,
            confidence_high=2,
            confidence_medium=2,
            confidence_low=1,
            party_final_coverage=80.0,
            party_final_gaps=1,
        )
    ]

    _print_comparison(metrics)
    captured = capsys.readouterr()
    assert "model" in captured.out
    assert "opus" in captured.out
