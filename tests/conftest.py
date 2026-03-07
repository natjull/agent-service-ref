"""Shared test fixtures — realistic SQLite database for integration tests."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.tools.resolution_tools import ensure_agent_tables


@pytest.fixture
def realistic_db(tmp_path: Path) -> Path:
    """Create a realistic temp SQLite with the full minimal schema."""
    db_path = tmp_path / "service_referential.sqlite"
    con = sqlite3.connect(str(db_path))

    # --- service_master_active ---
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
    services = [
        ("SVC-001", "ACME", "Lan To Lan", "L2L-100M", "EXT-001", "Paris Nord", "Lyon Sud", "ACME Corp"),
        ("SVC-002", "ACME", "FON", "FON-1G", "EXT-002", "Paris Nord", "Marseille Est", "ACME Corp"),
        ("SVC-003", "GLOBEX", "Lan To Lan", "L2L-10M", "EXT-003", "Lyon Sud", "Nice Ouest", "Globex Inc"),
        ("SVC-004", "GLOBEX", "FON", "FON-10G", "EXT-004", "Marseille Est", "Nice Ouest", "Globex Inc"),
        ("SVC-005", "INITECH", "Transit IP", "IP-TR-1G", "EXT-005", "Paris Nord", "Toulouse Centre", "Initech SA"),
    ]
    con.executemany(
        "INSERT INTO service_master_active VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        services,
    )

    # --- gold_service_active ---
    con.execute("""
        CREATE TABLE gold_service_active (
            service_id TEXT PRIMARY KEY,
            match_state TEXT,
            confidence_band TEXT
        )
    """)
    gold = [
        ("SVC-001", "auto_valid", "high"),
        ("SVC-002", "review_required", "medium"),
        ("SVC-003", "review_required", "low"),
        ("SVC-004", "auto_valid", "high"),
        ("SVC-005", "review_required", "low"),
    ]
    con.executemany("INSERT INTO gold_service_active VALUES (?, ?, ?)", gold)

    # --- ref_sites ---
    con.execute("""
        CREATE TABLE ref_sites (
            site_id TEXT PRIMARY KEY,
            reference TEXT,
            normalized_reference TEXT
        )
    """)
    sites = [
        ("SITE-PN", "Paris Nord", "paris nord"),
        ("SITE-LS", "Lyon Sud", "lyon sud"),
        ("SITE-ME", "Marseille Est", "marseille est"),
        ("SITE-NO", "Nice Ouest", "nice ouest"),
    ]
    con.executemany("INSERT INTO ref_sites VALUES (?, ?, ?)", sites)

    # --- ref_routes + ref_route_parcours ---
    con.execute("CREATE TABLE ref_routes (route_id TEXT PRIMARY KEY, route_ref TEXT)")
    con.executemany("INSERT INTO ref_routes VALUES (?, ?)", [
        ("RT-001", "ROUTE-PN-LS"),
        ("RT-002", "ROUTE-ME-NO"),
    ])

    con.execute("""
        CREATE TABLE ref_route_parcours (
            route_id TEXT,
            site TEXT,
            step_type TEXT
        )
    """)
    parcours = [
        ("RT-001", "Paris Nord", "origin"),
        ("RT-001", "Lyon Sud", "destination"),
        ("RT-002", "Marseille Est", "origin"),
        ("RT-002", "Nice Ouest", "destination"),
    ]
    con.executemany("INSERT INTO ref_route_parcours VALUES (?, ?, ?)", parcours)

    # --- ref_network_devices + ref_network_interfaces ---
    con.execute("CREATE TABLE ref_network_devices (device_name TEXT PRIMARY KEY, pop_site TEXT)")
    con.executemany("INSERT INTO ref_network_devices VALUES (?, ?)", [
        ("par1-co-1", "SITE-PN"),
        ("lyo1-co-2", "SITE-LS"),
        ("mar1-sec-1", "SITE-ME"),
    ])

    con.execute("CREATE TABLE ref_network_interfaces (device_name TEXT, interface_name TEXT, description TEXT)")
    con.executemany("INSERT INTO ref_network_interfaces VALUES (?, ?, ?)", [
        ("par1-co-1", "Gi0/0/1", "ACME L2L"),
        ("lyo1-co-2", "Gi0/0/2", "ACME L2L"),
        ("mar1-sec-1", "Gi0/1/0", "GLOBEX FON"),
    ])

    # --- party_master + party_alias ---
    con.execute("CREATE TABLE party_master (party_id TEXT PRIMARY KEY, canonical_name TEXT)")
    con.executemany("INSERT INTO party_master VALUES (?, ?)", [
        ("P-ACME", "ACME Corp"),
        ("P-GLOBEX", "Globex Inc"),
        ("P-INITECH", "Initech SA"),
    ])

    con.execute("CREATE TABLE party_alias (alias TEXT, party_id TEXT)")
    con.executemany("INSERT INTO party_alias VALUES (?, ?)", [
        ("ACME", "P-ACME"),
        ("GLOBEX", "P-GLOBEX"),
        ("INITECH", "P-INITECH"),
    ])

    # --- service_match_evidence ---
    con.execute("""
        CREATE TABLE service_match_evidence (
            service_id TEXT,
            evidence_type TEXT,
            rule_name TEXT,
            score INTEGER,
            source_table TEXT,
            source_key TEXT,
            payload_json TEXT
        )
    """)
    con.executemany(
        "INSERT INTO service_match_evidence VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("SVC-001", "site_match", "exact", 100, "ref_sites", "SITE-PN", '{"match":"exact"}'),
            ("SVC-001", "party_match", "alias", 90, "party_alias", "ACME", '{"alias":"ACME"}'),
            ("SVC-002", "site_match", "fuzzy", 72, "ref_sites", "SITE-PN", '{"match":"fuzzy"}'),
        ],
    )

    # --- service_review_queue ---
    con.execute("""
        CREATE TABLE service_review_queue (
            service_id TEXT,
            review_type TEXT,
            severity TEXT,
            reason TEXT,
            context_json TEXT
        )
    """)
    con.executemany(
        "INSERT INTO service_review_queue VALUES (?, ?, ?, ?, ?)",
        [
            ("SVC-002", "missing_site_z", "warning", "Site Z not matched", '{"raw":"Marseille Est"}'),
            ("SVC-003", "missing_final_party", "warning", "No party resolved", '{"client":"GLOBEX"}'),
            ("SVC-005", "missing_site_z", "error", "Site Z unknown", '{"raw":"Toulouse Centre"}'),
            ("SVC-005", "missing_final_party", "warning", "No party resolved", '{"client":"INITECH"}'),
        ],
    )

    # --- service_endpoint ---
    con.execute("""
        CREATE TABLE service_endpoint (
            service_id TEXT,
            endpoint_label TEXT,
            raw_value TEXT,
            matched_site_id TEXT,
            matched_site_name TEXT,
            score INTEGER,
            rule_name TEXT
        )
    """)
    con.executemany(
        "INSERT INTO service_endpoint VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("SVC-001", "A", "Paris Nord", "SITE-PN", "Paris Nord", 100, "exact"),
            ("SVC-001", "Z", "Lyon Sud", "SITE-LS", "Lyon Sud", 100, "exact"),
            ("SVC-002", "A", "Paris Nord", "SITE-PN", "Paris Nord", 100, "exact"),
            ("SVC-002", "Z", "Marseille Est", None, None, 0, None),
        ],
    )

    # --- service_party ---
    con.execute("""
        CREATE TABLE service_party (
            service_id TEXT,
            role_name TEXT,
            party_id TEXT,
            rule_name TEXT,
            score INTEGER
        )
    """)
    con.executemany(
        "INSERT INTO service_party VALUES (?, ?, ?, ?, ?)",
        [
            ("SVC-001", "client_final", "P-ACME", "alias", 90),
        ],
    )

    # --- service_support_reseau ---
    con.execute("""
        CREATE TABLE service_support_reseau (
            service_id TEXT,
            device_name TEXT,
            interface_name TEXT
        )
    """)

    # --- service_support_optique ---
    con.execute("""
        CREATE TABLE service_support_optique (
            service_id TEXT,
            support_type TEXT,
            support_ref TEXT
        )
    """)

    con.commit()
    con.close()

    # Create agent tables
    ensure_agent_tables(db_path)

    return db_path
