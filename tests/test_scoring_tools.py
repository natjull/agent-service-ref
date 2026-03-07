"""Integration tests for scoring_tools."""

from __future__ import annotations

import sqlite3

import pytest

from src.tools.scoring_tools import compute_scorecard
from src.tools.resolution_tools import ensure_agent_tables


class TestComputeScorecard:
    def test_reports_agent_quality_metrics(self, realistic_db):
        con = sqlite3.connect(str(realistic_db))
        con.execute(
            """
            INSERT INTO agent_resolutions
            (resolution_id, service_id, confidence, party_final_id, justification, status, evidence_count)
            VALUES
            ('R1', 'SVC-001', 'high', 'P-ACME', 'ok justification', 'validated', 3),
            ('R2', 'SVC-002', 'medium', NULL, 'missing final party', 'needs_review', 2),
            ('R3', 'SVC-003', 'low', NULL, 'party search unresolved', 'proposed', 1)
            """
        )
        con.execute(
            """
            INSERT INTO agent_evidence
            (evidence_id, resolution_id, service_id, evidence_type, description, score)
            VALUES
            ('E1', 'R1', 'SVC-001', 'site', 'site ok', 80),
            ('E2', 'R1', 'SVC-001', 'party', 'party ok', 90),
            ('E3', 'R1', 'SVC-001', 'network_interface', 'network ok', 75),
            ('E4', 'R2', 'SVC-002', 'site', 'site ok', 60),
            ('E5', 'R2', 'SVC-002', 'party_search', 'party lookup failed', 10),
            ('E6', 'R3', 'SVC-003', 'party', 'contract party only', 100)
            """
        )
        con.commit()
        con.close()

        result = compute_scorecard(realistic_db)
        assert "PROPOSED RESTANTS: 1" in result
        assert "PARTY_FINAL ABSENT: 2" in result
        assert "MEDIUM/HIGH SANS PARTY_FINAL: 1" in result
        assert "LOW A 1 EVIDENCE: 1" in result
        assert "LOW A 1 EVIDENCE PARTY ONLY: 1" in result

    def test_returns_expected_sections(self, realistic_db):
        result = compute_scorecard(realistic_db)
        assert "SCORECARD" in result
        assert "SERVICES ACTIFS: 5" in result
        assert "Auto-valides (pipeline): 2" in result
        assert "En attente de review:    3" in result
        assert "COUVERTURE PAR AXE" in result
        assert "PAR NATURE DE SERVICE" in result
        assert "TOP CLIENTS RESTANTS" in result

    def test_focus_client(self, realistic_db):
        result = compute_scorecard(realistic_db, focus="client:ACME")
        assert "FOCUS: client:ACME" in result
        assert "SVC-001" in result
        assert "SVC-002" in result
        # GLOBEX services should not appear in focus section
        # (they may appear in the overall stats though)

    def test_focus_unresolved(self, realistic_db):
        result = compute_scorecard(realistic_db, focus="unresolved")
        assert "FOCUS: unresolved" in result
        assert "non resolus" in result

    def test_focus_auto_valid(self, realistic_db):
        result = compute_scorecard(realistic_db, focus="auto_valid")
        assert "FOCUS: auto_valid" in result
        assert "auto-valides" in result

    def test_focus_party_gaps(self, realistic_db):
        con = sqlite3.connect(str(realistic_db))
        ensure_agent_tables(realistic_db)
        con.execute(
            """
            INSERT INTO agent_resolutions
            (resolution_id, service_id, confidence, justification, status, evidence_count)
            VALUES ('R4', 'SVC-002', 'low', 'party unresolved', 'needs_review', 1)
            """
        )
        con.commit()
        con.close()

        result = compute_scorecard(realistic_db, focus="party_gaps")
        assert "FOCUS: party_gaps" in result
        assert "SVC-002" in result
