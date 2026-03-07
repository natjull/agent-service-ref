"""Tests for deterministic validation helpers in validation_lib."""

import sqlite3

import pytest

from src.tools.validation_lib import (
    validate_site,
    validate_device_pop,
    validate_route_endpoints,
)


@pytest.fixture
def con():
    """In-memory SQLite with ref tables."""
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row

    c.execute("""
        CREATE TABLE ref_sites (
            site_id TEXT PRIMARY KEY,
            reference TEXT,
            normalized_reference TEXT
        )
    """)
    c.executemany(
        "INSERT INTO ref_sites VALUES (?, ?, ?)",
        [
            ("S001", "Paris Nord", "paris nord"),
            ("S002", "Lyon Part-Dieu", "lyon part-dieu"),
            ("S003", "Marseille Vieux-Port", "marseille vieux-port"),
            ("POP-TLS1", "Toulouse POP TLS1", "toulouse pop tls1"),
        ],
    )

    c.execute("""
        CREATE TABLE ref_routes (
            route_id TEXT PRIMARY KEY,
            route_ref TEXT
        )
    """)
    c.executemany(
        "INSERT INTO ref_routes VALUES (?, ?)",
        [
            ("R001", "ROUTE-PAR-LYO-001"),
            ("R002", "ROUTE-MRS-TLS-002"),
        ],
    )

    c.execute("""
        CREATE TABLE ref_route_parcours (
            route_id TEXT,
            site TEXT,
            site_detail TEXT,
            step_type TEXT
        )
    """)
    c.executemany(
        "INSERT INTO ref_route_parcours VALUES (?, ?, ?, ?)",
        [
            ("R001", "Paris Nord", "", "DEPART"),
            ("R001", "Dijon", "", "PASSAGE"),
            ("R001", "Lyon Part-Dieu", "", "ARRIVEE"),
        ],
    )

    c.commit()
    yield c
    c.close()


class TestValidateSite:
    def test_exact_match_by_site_id(self, con):
        r = validate_site(con, "S001")
        assert r.passed
        assert r.score == 100

    def test_exact_match_by_normalized_reference(self, con):
        r = validate_site(con, "Paris Nord")
        # Should match either exact or fuzzy with high score
        assert r.passed

    def test_no_match(self, con):
        r = validate_site(con, "Berlin Hauptbahnhof")
        assert not r.passed
        assert r.score < 72

    def test_empty_value(self, con):
        r = validate_site(con, "")
        assert not r.passed


class TestValidateDevicePop:
    def test_pop_matches_site(self, con):
        r = validate_device_pop(con, "tls1-router-01", "", "Toulouse POP TLS1")
        assert r.passed
        assert "TLS1" in r.detail

    def test_pop_no_match(self, con):
        r = validate_device_pop(con, "bor1-switch-01", "Paris Nord", "Lyon Part-Dieu")
        assert not r.passed

    def test_cannot_extract_pop(self, con):
        r = validate_device_pop(con, "router", "Paris Nord", "Lyon Part-Dieu")
        assert not r.passed
        assert "cannot extract" in r.detail

    def test_empty_device(self, con):
        r = validate_device_pop(con, "", "Paris Nord", "Lyon Part-Dieu")
        assert not r.passed

    def test_no_sites_provided(self, con):
        r = validate_device_pop(con, "tls1-router-01", "", "")
        assert not r.passed


class TestValidateRouteEndpoints:
    def test_both_endpoints_match(self, con):
        r = validate_route_endpoints(con, "R001", "Paris Nord", "Lyon Part-Dieu")
        assert r.passed
        assert r.score >= 90
        assert "both" in r.detail

    def test_one_endpoint_matches(self, con):
        r = validate_route_endpoints(con, "R001", "Paris Nord", "Marseille Vieux-Port")
        assert r.passed
        assert r.score >= 60
        assert "site_a" in r.detail

    def test_no_endpoints_match(self, con):
        r = validate_route_endpoints(con, "R001", "Berlin", "Munich")
        assert not r.passed

    def test_route_not_found(self, con):
        r = validate_route_endpoints(con, "R999", "Paris Nord", "Lyon Part-Dieu")
        assert not r.passed

    def test_empty_route_ref(self, con):
        r = validate_route_endpoints(con, "", "Paris Nord", "Lyon Part-Dieu")
        assert not r.passed

    def test_route_without_parcours(self, con):
        """Route R002 exists but has no parcours data."""
        r = validate_route_endpoints(con, "R002", "Marseille Vieux-Port", "Toulouse POP TLS1")
        assert r.passed
        assert r.score == 60
