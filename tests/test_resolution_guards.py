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
    con.execute("CREATE TABLE party_master (party_id TEXT PRIMARY KEY, canonical_name TEXT)")
    con.execute("INSERT INTO party_master VALUES ('P1', 'ACME Corp')")

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
    assert "distinct evidence_types" in text


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
        "resolution_json": _make_resolution("high", evidences),
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
    assert "distinct evidence_types" in text


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
        "resolution_json": _make_resolution("medium", evidences),
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
        "resolution_json": _make_resolution("low", evidences),
    }))
    text = result["content"][0]["text"]
    assert "Resolution submitted" in text


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
        ),
    }))
    assert "Resolution submitted" in result["content"][0]["text"]

    result = _run(_validate({"service_id": "SVC-001"}))
    text = result["content"][0]["text"]
    assert "needs_review" in text
