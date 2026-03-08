"""Tests for the final billing-oriented publication layer."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from service_ref import build_service_referential as legacy
from src.tools.resolution_tools import ensure_agent_tables


@pytest.fixture
def publication_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "service_referential.sqlite"
    con = sqlite3.connect(str(db_path))
    legacy.create_schema(con)

    con.executemany(
        """
        insert into service_master_active
        (service_id, service_key, nature_service, principal_client, principal_offer,
         principal_external_ref, principal_internal_ref, route_refs_json, service_refs_json,
         endpoint_a_raw, endpoint_z_raw, client_final, line_count, active_line_count)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("SVC-001", "KEY-001", "Lan To Lan", "ACME", "L2L-100M", "EXT-001", "INT-001", "[]", "[]", "Paris Nord", "Lyon Sud", "ACME Corp", 2, 2),
            ("SVC-002", "KEY-002", "IRU FON", "ACME", "FON-1G", "EXT-002", "INT-002", "[]", "[]", "Paris Nord", "Marseille Est", "Ville de Marseille", 1, 1),
            ("SVC-003", "KEY-003", "Lan To Lan", "GLOBEX", "L2L-10M", "EXT-003", "INT-003", "[]", "[]", "Lyon Sud", "Nice Ouest", "Globex Inc", 1, 1),
            ("SVC-004", "KEY-004", "Lan To Lan", "INITECH", "L2L-10M", "EXT-004", "INT-004", "[]", "[]", "Paris Nord", "Unknown Site", "Initech SA", 1, 1),
        ],
    )

    con.executemany(
        "insert into service_bss_line (service_id, lea_line_id, role_ligne, is_principal) values (?, ?, ?, ?)",
        [
            ("SVC-001", "LEA-001A", "principal", 1),
            ("SVC-001", "LEA-001B", "annexe", 0),
            ("SVC-002", "LEA-002", "principal", 1),
            ("SVC-003", "LEA-003", "principal", 1),
        ],
    )

    con.executemany(
        """
        insert into party_master (party_id, canonical_name, normalized_name, party_type, source_priority)
        values (?, ?, ?, ?, ?)
        """,
        [
            ("P-ACME", "ACME Corp", "ACME CORP", "customer", 1),
            ("P-GLOBEX", "Globex Inc", "GLOBEX INC", "customer", 1),
            ("P-INITECH", "Initech SA", "INITECH SA", "customer", 1),
            ("P-MRS", "Ville de Marseille", "VILLE DE MARSEILLE", "customer", 1),
        ],
    )

    con.executemany(
        """
        insert into ref_sites
        (site_id, reference, userreference, normalized_reference, normalized_userreference, normalized_address)
        values (?, ?, ?, ?, ?, ?)
        """,
        [
            ("SITE-PN", "Paris Nord", "Paris Nord", "PARIS NORD", "PARIS NORD", "PARIS NORD"),
            ("SITE-LS", "Lyon Sud", "Lyon Sud", "LYON SUD", "LYON SUD", "LYON SUD"),
            ("SITE-ME", "Marseille Est", "Marseille Est", "MARSEILLE EST", "MARSEILLE EST", "MARSEILLE EST"),
            ("SITE-NO", "Nice Ouest", "Nice Ouest", "NICE OUEST", "NICE OUEST", "NICE OUEST"),
        ],
    )

    con.executemany(
        """
        insert into service_party
        (service_id, role_name, party_id, rule_name, score, source_table, source_key)
        values (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("SVC-001", "contract_party", "P-ACME", "party_exact", 100, "lea_active_lines", "ACME"),
            ("SVC-001", "final_party", "P-ACME", "party_exact", 95, "lea_active_lines", "ACME Corp"),
            ("SVC-002", "contract_party", "P-ACME", "party_exact", 100, "lea_active_lines", "ACME"),
            ("SVC-003", "contract_party", "P-GLOBEX", "party_exact", 100, "lea_active_lines", "GLOBEX"),
            ("SVC-003", "final_party", "P-GLOBEX", "party_exact", 95, "lea_active_lines", "Globex Inc"),
            ("SVC-004", "contract_party", "P-INITECH", "party_exact", 100, "lea_active_lines", "INITECH"),
        ],
    )

    con.executemany(
        """
        insert into service_endpoint
        (service_id, endpoint_label, raw_value, matched_site_id, matched_site_name, score, rule_name)
        values (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("SVC-001", "A", "Paris Nord", "SITE-PN", "Paris Nord", 100, "exact"),
            ("SVC-001", "Z", "Lyon Sud", "SITE-LS", "Lyon Sud", 100, "exact"),
            ("SVC-002", "A", "Paris Nord", "SITE-PN", "Paris Nord", 100, "exact"),
            ("SVC-002", "Z", "Marseille Est", "SITE-ME", "Marseille Est", 90, "exact"),
            ("SVC-003", "A", "Lyon Sud", "SITE-LS", "Lyon Sud", 100, "exact"),
            ("SVC-003", "Z", "Nice Ouest", "SITE-NO", "Nice Ouest", 100, "exact"),
            ("SVC-004", "A", "Paris Nord", "SITE-PN", "Paris Nord", 100, "exact"),
        ],
    )

    con.executemany(
        """
        insert into service_support_optique
        (service_id, route_ref, route_id, route_match_rule, route_score, lease_ref, lease_id,
         lease_match_rule, lease_score, fiber_lease_id, fiber_lease_match_rule, fiber_lease_score,
         isp_lease_id, isp_lease_match_rule, isp_lease_score)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("SVC-002", "ROUTE-PN-ME", "RT-002", "route_exact", 95, None, None, None, None, None, None, None, None, None, None),
        ],
    )

    con.executemany(
        """
        insert into gold_service_active
        (service_id, match_state, confidence_band, contract_party_id, final_party_id,
         endpoint_a_site_id, endpoint_z_site_id, route_ref, route_id, lease_id,
         fiber_lease_id, isp_lease_id, interface_id, network_interface_id, network_vlan_id,
         cpe_id, config_id, inferred_vlans_json, strong_evidence_count, evidence_count, summary_json)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("SVC-001", "auto_valid", "high", "P-ACME", "P-ACME", "SITE-PN", "SITE-LS", None, None, None, None, None, "IF-SVC1", "NIF-SVC1", "NVL-100", "CPE-1", "CFG-1", "[100]", 3, 4, "{}"),
            ("SVC-002", "review_required", "medium", "P-ACME", None, "SITE-PN", None, None, None, None, None, None, None, None, None, None, None, "[]", 1, 2, "{}"),
            ("SVC-003", "auto_valid", "high", "P-GLOBEX", "P-GLOBEX", "SITE-LS", "SITE-NO", None, None, None, None, None, "IF-SVC3", "NIF-SVC3", "NVL-300", None, "CFG-3", "[300]", 3, 4, "{}"),
            ("SVC-004", "review_required", "low", "P-INITECH", None, "SITE-PN", None, None, None, None, None, None, None, None, None, None, None, "[]", 0, 1, "{}"),
        ],
    )

    con.commit()
    con.close()
    return db_path


def _row_by_service(con: sqlite3.Connection, service_id: str) -> sqlite3.Row:
    con.row_factory = sqlite3.Row
    return con.execute(
        "select * from service_facturable_final where service_id = ?",
        (service_id,),
    ).fetchone()


def test_build_facturable_publication_without_agent_tables_uses_gold(publication_db):
    con = sqlite3.connect(str(publication_db))
    try:
        legacy.build_facturable_publication(con)
        row = _row_by_service(con, "SVC-001")
        assert row["selected_truth_source"] == "gold_auto_valid"
        assert row["publication_status"] == "published_from_gold"
        assert row["principal_lea_line_id"] == "LEA-001A"
        assert json.loads(row["lea_line_ids_json"]) == ["LEA-001A", "LEA-001B"]
        assert row["final_party_id"] == "P-ACME"
        assert row["network_vlan_id"] == "NVL-100"
        assert row["spatial_confidence_band"] == "none"
        assert "seed_a" in json.loads(row["spatial_summary_json"])
    finally:
        con.close()


def test_validated_agent_overrides_party_site_and_optical_when_remappable(publication_db):
    ensure_agent_tables(publication_db)
    con = sqlite3.connect(str(publication_db))
    try:
        con.execute(
            """
            insert into agent_resolutions
            (resolution_id, service_id, confidence, site_a, site_z, optical_support_ref,
             party_final, party_final_id, justification, status, evidence_count)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "AR-002",
                "SVC-002",
                "medium",
                "Paris Nord",
                "Marseille Est",
                "ROUTE-PN-ME",
                "Ville de Marseille",
                "P-MRS",
                "validated agent override",
                "validated",
                2,
            ),
        )
        con.commit()

        legacy.build_facturable_publication(con)
        row = _row_by_service(con, "SVC-002")
        assert row["selected_truth_source"] == "agent_validated"
        assert row["publication_status"] == "published_validated"
        assert row["final_party_id"] == "P-MRS"
        assert row["final_party_source"] == "agent_validated"
        assert row["site_z_id"] == "SITE-ME"
        assert row["site_z_source"] == "agent_validated"
        assert row["route_ref"] == "ROUTE-PN-ME"
        assert row["route_id"] == "RT-002"
        assert row["optical_source"] == "agent_validated"
    finally:
        con.close()


def test_non_validated_agent_does_not_override_gold(publication_db):
    ensure_agent_tables(publication_db)
    con = sqlite3.connect(str(publication_db))
    try:
        con.execute(
            """
            insert into agent_resolutions
            (resolution_id, service_id, confidence, site_a, site_z, party_final, party_final_id,
             justification, status, evidence_count)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "AR-003",
                "SVC-003",
                "low",
                "Paris Nord",
                "Marseille Est",
                "Wrong Party",
                "P-MRS",
                "should not override gold",
                "needs_review",
                1,
            ),
        )
        con.commit()

        legacy.build_facturable_publication(con)
        row = _row_by_service(con, "SVC-003")
        assert row["selected_truth_source"] == "gold_auto_valid"
        assert row["publication_status"] == "published_from_gold"
        assert row["final_party_id"] == "P-GLOBEX"
        assert row["site_a_id"] == "SITE-LS"
        assert row["site_z_id"] == "SITE-NO"
        assert row["agent_resolution_status"] == "needs_review"
    finally:
        con.close()


def test_needs_review_when_bss_link_missing_and_network_is_only_agent_hint(publication_db):
    ensure_agent_tables(publication_db)
    con = sqlite3.connect(str(publication_db))
    try:
        con.execute(
            """
            insert into agent_resolutions
            (resolution_id, service_id, confidence, site_a, network_support_id, justification,
             status, evidence_count)
            values (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "AR-004",
                "SVC-004",
                "low",
                "Paris Nord",
                "agg-sw-99",
                "validated but network support remains generic",
                "validated",
                2,
            ),
        )
        con.commit()

        legacy.build_facturable_publication(con)
        row = _row_by_service(con, "SVC-004")
        gaps = json.loads(row["gap_flags_json"])
        assert row["selected_truth_source"] == "agent_validated"
        assert row["publication_status"] == "needs_review"
        assert row["network_source"] == "agent_hint_unmapped"
        assert row["agent_network_support_hint"] == "agg-sw-99"
        assert "missing_bss_link" in gaps
        assert "missing_network_support" in gaps
        assert "missing_final_party" in gaps
    finally:
        con.close()


def test_structured_agent_fields_feed_final_publication(publication_db):
    ensure_agent_tables(publication_db)
    con = sqlite3.connect(str(publication_db))
    try:
        con.execute(
            """
            insert into agent_resolutions
            (resolution_id, service_id, confidence, party_final, party_final_id,
             resolved_site_a_id, resolved_site_z_id, route_ref, route_id,
             network_interface_id, network_vlan_id, config_id, inferred_vlans_json,
             justification, status, evidence_count)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "AR-002-STRUCT",
                "SVC-002",
                "high",
                "Ville de Marseille",
                "P-MRS",
                "SITE-PN",
                "SITE-ME",
                "ROUTE-PN-ME",
                "RT-002",
                "NIF-200",
                "NVL-200",
                "CFG-200",
                "[200]",
                "structured validated override",
                "validated",
                3,
            ),
        )
        con.commit()

        legacy.build_facturable_publication(con)
        row = _row_by_service(con, "SVC-002")
        assert row["site_a_id"] == "SITE-PN"
        assert row["site_z_id"] == "SITE-ME"
        assert row["route_id"] == "RT-002"
        assert row["network_interface_id"] == "NIF-200"
        assert row["network_vlan_id"] == "NVL-200"
        assert row["network_source"] == "agent_validated"
        assert row["optical_source"] == "agent_validated"
        assert row["publication_status"] == "published_validated"
    finally:
        con.close()


def test_export_outputs_writes_facturable_csv_and_report(publication_db, monkeypatch, tmp_path):
    con = sqlite3.connect(str(publication_db))
    try:
        legacy.build_facturable_publication(con)
        monkeypatch.setattr(legacy, "OUT_DIR", tmp_path)
        legacy.export_outputs(con)
        legacy.build_report(con)
    finally:
        con.close()

    csv_path = tmp_path / "service_facturable_final.csv"
    report_path = tmp_path / "service_referential_report.md"
    assert csv_path.exists()
    assert report_path.exists()
    csv_text = csv_path.read_text(encoding="utf-8")
    report_text = report_path.read_text(encoding="utf-8")
    assert "publication_status" in csv_text
    assert "service_facturable_final" in report_text or "Final facturable publication" in report_text
    assert "published_from_gold" in report_text
