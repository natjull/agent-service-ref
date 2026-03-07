"""Tests for consolidated token-saving tools."""

from __future__ import annotations

import asyncio
import json
import sqlite3

import pytest

from src.tools import db_tools, resolution_tools, scoring_tools
from src.tools.scoring_tools import compute_compact_scorecard

_decision_pack = db_tools.get_service_decision_pack.handler
_submit_and_validate = resolution_tools.submit_and_validate.handler


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def _configure_tools(realistic_db):
    db_tools.configure(realistic_db)
    resolution_tools.configure(realistic_db)
    scoring_tools.configure(realistic_db)
    yield


def test_get_service_decision_pack_returns_bundle_and_party_candidates():
    result = _run(_decision_pack({"service_id": "SVC-001"}))
    text = result["content"][0]["text"]
    assert "service" in text
    assert "review_items" in text
    assert "pipeline_evidences" in text
    assert "party_rows" in text
    assert "endpoint_rows" in text
    assert "network_support_rows" in text
    assert "optical_support_rows" in text
    assert "gold_row" in text
    assert "party_candidates" in text


def test_get_service_decision_pack_missing_service():
    result = _run(_decision_pack({"service_id": "NOPE"}))
    text = result["content"][0]["text"]
    assert '"service": null' in text
    assert '"party_candidates"' in text
    assert '"recommended_final_party_id": null' in text


def test_submit_and_validate_submits_then_validates(realistic_db):
    resolution = {
        "confidence": "medium",
        "site_a": "SITE-PN",
        "site_z": "SITE-LS",
        "party_final": "ACME Corp",
        "party_final_id": "P-ACME",
        "justification": "sites and final party match deterministically",
        "evidences": [
            {"evidence_type": "site", "description": "A/Z sites matched", "score": 90},
            {"evidence_type": "party", "description": "final party matched", "score": 95},
        ],
    }

    result = _run(
        _submit_and_validate(
            {"service_id": "SVC-001", "resolution_json": json.dumps(resolution)}
        )
    )
    text = result["content"][0]["text"]
    assert "Resolution submitted" in text
    assert "## Validation: SVC-001" in text

    con = sqlite3.connect(str(realistic_db))
    try:
        status = con.execute(
            "SELECT status FROM agent_resolutions WHERE service_id = 'SVC-001' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()[0]
        assert status == "validated"
    finally:
        con.close()


def test_submit_and_validate_stops_on_submit_error(realistic_db):
    resolution = {
        "confidence": "medium",
        "justification": "party final search unresolved after review",
        "evidences": [
            {"evidence_type": "site", "description": "site candidate", "score": 70},
            {"evidence_type": "party_search", "description": "search failed", "score": 10},
        ],
    }

    result = _run(
        _submit_and_validate(
            {"service_id": "SVC-002", "resolution_json": json.dumps(resolution)}
        )
    )
    text = result["content"][0]["text"]
    assert "ERROR:" in text
    assert "Validation" not in text

    con = sqlite3.connect(str(realistic_db))
    try:
        assert con.execute("SELECT COUNT(*) FROM agent_resolutions").fetchone()[0] == 0
    finally:
        con.close()


def test_compact_scorecard_is_short_and_contains_kpis(realistic_db):
    con = sqlite3.connect(str(realistic_db))
    con.execute(
        """
        INSERT INTO agent_resolutions
        (resolution_id, service_id, confidence, party_final_id, justification, status, evidence_count)
        VALUES ('R1', 'SVC-001', 'high', 'P-ACME', 'ok justification', 'validated', 3)
        """
    )
    con.commit()
    con.close()

    result = compute_compact_scorecard(realistic_db)
    assert len(result) < 300
    assert "SCORECARD:" in result
    assert "resolved=1" in result
    assert "party_gaps=" in result
