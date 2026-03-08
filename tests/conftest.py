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
    con.execute("""
        CREATE TABLE party_master (
            party_id TEXT PRIMARY KEY,
            canonical_name TEXT,
            normalized_name TEXT,
            party_type TEXT,
            source_priority INTEGER
        )
    """)
    con.executemany("INSERT INTO party_master VALUES (?, ?, ?, ?, ?)", [
        ("P-ACME", "ACME Corp", "ACME CORP", "customer", 1),
        ("P-GLOBEX", "Globex Inc", "GLOBEX INC", "customer", 1),
        ("P-INITECH", "Initech SA", "INITECH SA", "customer", 1),
    ])

    con.execute("""
        CREATE TABLE party_alias (
            alias_id TEXT PRIMARY KEY,
            party_id TEXT,
            alias_value TEXT,
            normalized_alias TEXT,
            source_table TEXT,
            source_key TEXT
        )
    """)
    con.executemany("INSERT INTO party_alias VALUES (?, ?, ?, ?, ?, ?)", [
        ("A-ACME", "P-ACME", "ACME", "ACME", "lea_active_lines", "ACME"),
        ("A-ACME-CORP", "P-ACME", "ACME Corp", "ACME CORP", "lea_active_lines", "ACME Corp"),
        ("A-GLOBEX", "P-GLOBEX", "GLOBEX", "GLOBEX", "lea_active_lines", "GLOBEX"),
        ("A-GLOBEX-INC", "P-GLOBEX", "Globex Inc", "GLOBEX INC", "lea_active_lines", "Globex Inc"),
        ("A-INITECH", "P-INITECH", "INITECH", "INITECH", "lea_active_lines", "INITECH"),
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
            ("SVC-001", "final_party", "P-ACME", "alias", 90),
            ("SVC-001", "contract_party", "P-ACME", "alias", 90),
            ("SVC-002", "contract_party", "P-ACME", "alias", 90),
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
    con.executemany(
        "INSERT INTO service_support_reseau VALUES (?, ?, ?)",
        [
            ("SVC-001", "par1-co-1", "Gi0/0/1"),
            ("SVC-003", "mar1-sec-1", "Gi0/1/0"),
        ],
    )

    # --- service_support_optique ---
    con.execute("""
        CREATE TABLE service_support_optique (
            service_id TEXT,
            support_type TEXT,
            support_ref TEXT
        )
    """)
    con.executemany(
        "INSERT INTO service_support_optique VALUES (?, ?, ?)",
        [
            ("SVC-001", "route", "ROUTE-PN-LS"),
            ("SVC-004", "route", "ROUTE-ME-NO"),
        ],
    )

    con.execute("""
        CREATE TABLE service_spatial_seed (
            service_id TEXT,
            seed_type TEXT,
            seed_priority INTEGER,
            raw_value TEXT,
            normalized_value TEXT,
            street_hint TEXT,
            house_number_hint TEXT,
            city_hint TEXT,
            postcode_hint TEXT,
            insee_hint TEXT,
            ban_id TEXT,
            match_rule TEXT,
            match_score INTEGER,
            x_l93 REAL,
            y_l93 REAL,
            source_table TEXT,
            source_column TEXT,
            source_signal_kind TEXT,
            source_signal_score INTEGER,
            source_semantic_strength TEXT,
            xy_precision_class TEXT,
            xy_discriminance_score INTEGER,
            same_xy_count_in_city INTEGER,
            is_reused_xy INTEGER,
            is_heavily_reused_xy INTEGER
        )
    """)
    con.executemany(
        "INSERT INTO service_spatial_seed VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("SVC-001", "postal_address_precise", 70, "Paris Nord", "PARIS NORD", None, None, "Paris", "75000", None, "BAN-001", "ban_full_label_exact", 94, 651000.0, 6861000.0, "service_lea_signal", "endpoint_a_raw", "postal_address_precise", 100, "strong", "precise", 100, 1, 0, 0),
            ("SVC-002", "city_only", 90, "Marseille", "MARSEILLE", None, None, "Marseille", "13000", None, None, "ban_city_only", 35, None, None, "service_lea_signal", "Commune site", "city_only", 35, "weak", "weak_reused_point", 30, 12, 1, 1),
        ],
    )

    con.execute("""
        CREATE TABLE service_spatial_evidence (
            service_id TEXT,
            evidence_type TEXT,
            seed_type TEXT,
            target_table TEXT,
            target_id TEXT,
            distance_meters REAL,
            score INTEGER,
            rule_name TEXT,
            context_json TEXT,
            seed_discriminance_score INTEGER,
            adjusted_score INTEGER
        )
    """)
    con.executemany(
        "INSERT INTO service_spatial_evidence VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("SVC-001", "site_spatial", "postal_address_precise", "ref_sites", "SITE-PN", 42.0, 98, "site_spatial_proximity", '{"seed_city":"Paris"}', 100, 98),
            ("SVC-001", "optical_spatial", "postal_address_precise", "ref_optical_cable", "CAB-001", 84.0, 88, "address_to_cable_endpoint", '{"reference":"CAB-PN"}', 100, 88),
        ],
    )

    con.execute("""
        CREATE TABLE service_lea_signal (
            service_id TEXT,
            lea_line_id TEXT,
            source_column TEXT,
            source_priority INTEGER,
            raw_value TEXT,
            normalized_value TEXT,
            signal_kind TEXT,
            signal_subkind TEXT,
            signal_score INTEGER,
            semantic_strength TEXT,
            is_ban_candidate INTEGER,
            is_site_candidate INTEGER,
            is_optical_candidate INTEGER,
            is_route_candidate INTEGER,
            is_noise INTEGER,
            street_hint TEXT,
            house_number_hint TEXT,
            city_hint TEXT,
            postcode_hint TEXT,
            insee_hint TEXT,
            route_refs_json TEXT,
            service_refs_json TEXT,
            site_tokens_json TEXT,
            technical_tokens_json TEXT,
            extraction_rule TEXT,
            classification_reason_json TEXT
        )
    """)
    con.executemany(
        "INSERT INTO service_lea_signal VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("SVC-001", "LEA-001", "CMD - Secteur géographique2", 100, "43 Avenue d'Italie 80000 Amiens", "43 AVENUE D ITALIE 80000 AMIENS", "postal_address_precise", "street_house_city", 100, "strong", 1, 1, 0, 0, 0, "AVENUE D ITALIE", "43", "AMIENS", "80000", None, "[]", "[]", '["AMIENS"]', "[]", "content_classification", '{"reasons":["street_house_city"]}'),
            ("SVC-001", "LEA-001", "CMD - Secteur géographique1", 95, "POP AMIENS", "POP AMIENS", "technical_site_anchor", "technical_tokens", 82, "strong", 0, 1, 1, 0, 0, None, None, None, None, None, "[]", "[]", '["AMIENS"]', '["POP"]', "content_classification", '{"reasons":["technical_tokens"]}'),
        ],
    )

    con.commit()
    con.close()

    # Create agent tables
    ensure_agent_tables(db_path)

    return db_path
