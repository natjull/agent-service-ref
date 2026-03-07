"""Tests for evidence quality gates and validation logic in resolution_tools."""

import asyncio
import json
import sqlite3

import pytest

from src.tools import resolution_tools

# Access underlying async handlers behind @tool decorator
_submit = resolution_tools.submit_resolution.handler
_validate = resolution_tools.validate_resolution.handler


@pytest.fixture
def db(tmp_path):
    """Create a temp DB with required schema."""
    db_path = tmp_path / "test.sqlite"
    con = sqlite3.connect(str(db_path))
    con.execute("""
        CREATE TABLE service_master_active (
            service_id TEXT PRIMARY KEY,
            principal_client TEXT,
            nature_service TEXT,
            principal_offer TEXT,
            principal_external_ref TEXT,
            endpoint_a_raw TEXT,
            endpoint_z_raw TEXT,
            client_final TEXT
        )
    """)
    con.execute("""
        INSERT INTO service_master_active (service_id, principal_client, nature_service)
        VALUES ('SVC-001', 'ACME', 'Lan To Lan')
    """)
    con.execute("""
        CREATE TABLE ref_sites (
            site_id TEXT PRIMARY KEY,
            reference TEXT,
            normalized_reference TEXT
        )
    """)
    con.execute("INSERT INTO ref_sites VALUES ('S1', 'Paris Nord', 'paris nord')")
    con.execute("INSERT INTO ref_sites VALUES ('S2', 'Lyon Sud', 'lyon sud')")

    con.execute("CREATE TABLE ref_network_devices (device_name TEXT PRIMARY KEY)")
    con.execute("CREATE TABLE ref_network_interfaces (device_name TEXT, interface_name TEXT)")
    con.execute("CREATE TABLE ref_routes (route_id TEXT PRIMARY KEY, route_ref TEXT)")
    con.execute("""
        CREATE TABLE party_master (
            party_id TEXT PRIMARY KEY,
            canonical_name TEXT,
            normalized_name TEXT,
            party_type TEXT,
            source_priority INTEGER
        )
    """)
    con.execute("INSERT INTO party_master VALUES ('P1', 'ACME Corp', 'ACME CORP', 'customer', 1)")
    con.execute("""
        CREATE TABLE agent_evidence (
            evidence_id TEXT PRIMARY KEY,
            resolution_id TEXT NOT NULL,
            service_id TEXT NOT NULL,
            evidence_type TEXT NOT NULL,
            source_table TEXT,
            source_key TEXT,
            description TEXT NOT NULL,
            score INTEGER DEFAULT 0,
            payload_json TEXT
        )
    """)

    con.commit()
    con.close()

    resolution_tools.ensure_agent_tables(db_path)
    resolution_tools.configure(db_path)
    return db_path


def _make_resolution(confidence, evidences, **kwargs):
    data = {
        "confidence": confidence,
        "justification": "test justification",
        "evidences": evidences,
        **kwargs,
    }
    return json.dumps(data)


def _run(coro):
    return asyncio.run(coro)


def test_high_confidence_requires_2_evidence_types(db):
    evidences = [
        {"evidence_type": "site_match", "description": "d1", "score": 80},
        {"evidence_type": "site_match", "description": "d2", "score": 70},
        {"evidence_type": "site_match", "description": "d3", "score": 90},
    ]
    result = _run(_submit({
        "service_id": "SVC-001",
        "resolution_json": _make_resolution("high", evidences),
    }))
    text = result["content"][0]["text"]
    assert "ERROR" in text
    assert "party_final_id" in text or "distinct evidence_types" in text


def test_high_confidence_requires_avg_score_60(db):
    evidences = [
        {"evidence_type": "site_match", "description": "d1", "score": 40},
        {"evidence_type": "device_match", "description": "d2", "score": 30},
        {"evidence_type": "route_match", "description": "d3", "score": 20},
    ]
    result = _run(_submit({
        "service_id": "SVC-001",
        "resolution_json": _make_resolution("high", evidences),
    }))
    text = result["content"][0]["text"]
    assert "ERROR" in text
    assert "average evidence score" in text


def test_high_confidence_passes(db):
    evidences = [
        {"evidence_type": "site_match", "description": "d1", "score": 80},
        {"evidence_type": "device_match", "description": "d2", "score": 70},
        {"evidence_type": "route_match", "description": "d3", "score": 90},
    ]
    result = _run(_submit({
        "service_id": "SVC-001",
        "resolution_json": _make_resolution("high", evidences, party_final_id="P1"),
    }))
    text = result["content"][0]["text"]
    assert "Resolution submitted" in text


def test_medium_confidence_requires_2_evidence_types(db):
    evidences = [
        {"evidence_type": "site_match", "description": "d1", "score": 60},
        {"evidence_type": "site_match", "description": "d2", "score": 55},
    ]
    result = _run(_submit({
        "service_id": "SVC-001",
        "resolution_json": _make_resolution("medium", evidences),
    }))
    text = result["content"][0]["text"]
    assert "ERROR" in text
    assert "party_final_id" in text or "distinct evidence_types" in text


def test_medium_confidence_requires_score_50(db):
    evidences = [
        {"evidence_type": "site_match", "description": "d1", "score": 30},
        {"evidence_type": "device_match", "description": "d2", "score": 40},
    ]
    result = _run(_submit({
        "service_id": "SVC-001",
        "resolution_json": _make_resolution("medium", evidences),
    }))
    text = result["content"][0]["text"]
    assert "ERROR" in text
    assert "score >= 50" in text


def test_medium_confidence_passes(db):
    evidences = [
        {"evidence_type": "site_match", "description": "d1", "score": 55},
        {"evidence_type": "device_match", "description": "d2", "score": 40},
    ]
    result = _run(_submit({
        "service_id": "SVC-001",
        "resolution_json": _make_resolution("medium", evidences, party_final_id="P1"),
    }))
    text = result["content"][0]["text"]
    assert "Resolution submitted" in text


def test_low_confidence_requires_score_gt_0(db):
    evidences = [
        {"evidence_type": "site_match", "description": "d1", "score": 0},
    ]
    result = _run(_submit({
        "service_id": "SVC-001",
        "resolution_json": _make_resolution("low", evidences),
    }))
    text = result["content"][0]["text"]
    assert "ERROR" in text
    assert "score > 0" in text


def test_low_confidence_passes(db):
    evidences = [
        {"evidence_type": "site_match", "description": "d1", "score": 10},
    ]
    result = _run(_submit({
        "service_id": "SVC-001",
        "resolution_json": _make_resolution(
            "low",
            [{"evidence_type": "party_search", "description": "party lookup exhausted", "score": 10}],
            justification="final party search attempted but unresolved",
        ),
    }))
    text = result["content"][0]["text"]
    assert "Resolution submitted" in text


def test_medium_confidence_requires_party_final_id(db):
    evidences = [
        {"evidence_type": "site_match", "description": "d1", "score": 55},
        {"evidence_type": "device_match", "description": "d2", "score": 52},
    ]
    result = _run(_submit({
        "service_id": "SVC-001",
        "resolution_json": _make_resolution("medium", evidences),
    }))
    text = result["content"][0]["text"]
    assert "party_final_id" in text


def test_low_without_party_final_requires_search_evidence_or_explanation(db):
    evidences = [
        {"evidence_type": "site_match", "description": "d1", "score": 15},
    ]
    result = _run(_submit({
        "service_id": "SVC-001",
        "resolution_json": _make_resolution("low", evidences, justification="weak low without party context"),
    }))
    text = result["content"][0]["text"]
    assert "party_search" in text


def test_self_loop_rejected(db):
    evidences = [
        {"evidence_type": "site_match", "description": "d1", "score": 80},
        {"evidence_type": "device_match", "description": "d2", "score": 70},
        {"evidence_type": "route_match", "description": "d3", "score": 90},
    ]
    result = _run(_submit({
        "service_id": "SVC-001",
        "resolution_json": _make_resolution(
            "high", evidences,
            site_a="Paris Nord", site_z="Paris Nord",
            party_final_id="P1",
        ),
    }))
    assert "Resolution submitted" in result["content"][0]["text"]

    result = _run(_validate({"service_id": "SVC-001"}))
    text = result["content"][0]["text"]
    assert "self-loop" in text
    assert "rejected" in text


def test_needs_review_on_single_warning(db):
    """Any warning (even just 1) now triggers needs_review."""
    evidences = [
        {"evidence_type": "site_match", "description": "d1", "score": 80},
        {"evidence_type": "device_match", "description": "d2", "score": 70},
        {"evidence_type": "route_match", "description": "d3", "score": 90},
    ]
    result = _run(_submit({
        "service_id": "SVC-001",
        "resolution_json": _make_resolution(
            "high", evidences,
            site_a="Paris Nord",
            site_z="Unknown City Y",
            party_final_id="P1",
        ),
    }))
    assert "Resolution submitted" in result["content"][0]["text"]

    result = _run(_validate({"service_id": "SVC-001"}))
    text = result["content"][0]["text"]
    assert "needs_review" in text


def test_low_single_party_evidence_becomes_needs_review(db):
    evidences = [
        {"evidence_type": "party", "description": "contract party only", "score": 100},
    ]
    result = _run(_submit({
        "service_id": "SVC-001",
        "resolution_json": _make_resolution(
            "low",
            evidences,
            justification="final party search attempted but unresolved after contract party lookup",
        ),
    }))
    assert "Resolution submitted" in result["content"][0]["text"]

    result = _run(_validate({"service_id": "SVC-001"}))
    text = result["content"][0]["text"]
    assert "needs_review" in text


def test_missing_party_final_becomes_needs_review(db):
    evidences = [
        {"evidence_type": "site_match", "description": "d1", "score": 20},
        {"evidence_type": "party_search", "description": "final party unresolved", "score": 10},
    ]
    result = _run(_submit({
        "service_id": "SVC-001",
        "resolution_json": _make_resolution(
            "low",
            evidences,
            justification="final party search unresolved after checking aliases and pipeline parties",
        ),
    }))
    assert "Resolution submitted" in result["content"][0]["text"]

    result = _run(_validate({"service_id": "SVC-001"}))
    text = result["content"][0]["text"]
    assert "party_final_id missing" in text
    assert "needs_review" in text
