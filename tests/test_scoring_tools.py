"""Integration tests for scoring_tools."""

from __future__ import annotations

import pytest

from src.tools.scoring_tools import compute_scorecard


class TestComputeScorecard:
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
