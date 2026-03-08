from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from service_ref import build_service_referential as legacy


def _make_db() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    legacy.create_schema(con)
    return con


def test_parse_address_seed_extracts_number_street_and_city() -> None:
    parsed = legacy.parse_address_seed("114 RUE DE MORIENVAL PIERREFONDS")
    assert parsed["house_number"] == "114"
    assert parsed["street_name"] == "RUE DE MORIENVAL"
    assert parsed["city"] == "PIERREFONDS"


def test_load_ban_addresses_loads_csv(monkeypatch, tmp_path: Path) -> None:
    csv_path = tmp_path / "ban-60.csv"
    csv_path.write_text(
        "id,numero,nom_voie,code_postal,nom_commune,lon,lat,x,y\n"
        "BAN-1,114,Rue de Morienval,60350,Pierrefonds,2.9801,49.3462,700000,6900000\n",
        encoding="utf-8",
    )

    con = _make_db()
    try:
        monkeypatch.setattr(legacy, "BAN_60_PATH", csv_path)
        legacy.load_ban_addresses(con)
        row = con.execute("select * from ref_ban_address").fetchone()
        assert row["ban_id"] == "BAN-1"
        assert row["city"] == "Pierrefonds"
        assert row["street_name"] == "Rue de Morienval"
        assert row["x_l93"] == 700000
        assert row["y_l93"] == 6900000
        assert row["same_xy_count_in_city"] == 1
        assert row["xy_precision_class"] == "precise"
    finally:
        con.close()


def test_build_service_spatial_seeds_uses_classified_lea_signals(monkeypatch, tmp_path: Path) -> None:
    csv_path = tmp_path / "ban-60.csv"
    csv_path.write_text(
        "id,numero,nom_voie,code_postal,nom_commune,lon,lat,x,y\n"
        "BAN-1,114,Rue de Morienval,60350,Pierrefonds,2.9801,49.3462,700000,6900000\n",
        encoding="utf-8",
    )

    con = _make_db()
    try:
        monkeypatch.setattr(legacy, "BAN_60_PATH", csv_path)
        legacy.load_ban_addresses(con)
        con.execute(
            """
            insert into service_master_active
            (service_id, service_key, nature_service, principal_client, principal_offer,
             principal_external_ref, principal_internal_ref, route_refs_json, service_refs_json,
             endpoint_a_raw, endpoint_z_raw, client_final, line_count, active_line_count)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "SVC-001",
                "KEY-1",
                "IRU FON",
                "ACME",
                "FON",
                "",
                "",
                "[]",
                "[]",
                "Pierrefonds",
                "",
                "ACME",
                1,
                1,
            ),
        )
        con.execute(
            """
            insert into lea_active_lines
            values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "LEA-1",
                "ACME",
                "FON",
                "IRU FON",
                "principal",
                "INT-1",
                "EXT-1",
                "FILE-1",
                "Pierrefonds",
                "",
                "ACME",
                "",
                "",
                "",
                "",
                "40",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "[]",
                "[]",
                0,
                "KEY",
                1,
                json.dumps({"Adresse site": "114 Rue de Morienval Pierrefonds"}, ensure_ascii=True),
            ),
        )
        con.execute(
            "insert into service_bss_line (service_id, lea_line_id, role_ligne, is_principal) values (?, ?, ?, ?)",
            ("SVC-001", "LEA-1", "principal", 1),
        )
        con.commit()

        seeds = legacy.build_service_spatial_seeds(con)
        assert "SVC-001" in seeds
        signal_rows = con.execute(
            "select signal_kind, raw_value from service_lea_signal where service_id = ?",
            ("SVC-001",),
        ).fetchall()
        assert any(row["signal_kind"] == "postal_address_precise" for row in signal_rows)
        rows = con.execute(
            "select seed_type, raw_value, ban_id, source_signal_kind from service_spatial_seed where service_id = ?",
            ("SVC-001",),
        ).fetchall()
        assert any(row["seed_type"] == "postal_address_precise" for row in rows)
        assert any(row["ban_id"] == "BAN-1" for row in rows)
        assert any(row["source_signal_kind"] == "postal_address_precise" for row in rows)
    finally:
        con.close()


def test_reconcile_services_persists_spatial_endpoint_and_evidence(monkeypatch, tmp_path: Path) -> None:
    csv_path = tmp_path / "ban-60.csv"
    csv_path.write_text(
        "id,numero,nom_voie,code_postal,nom_commune,lon,lat,x,y\n"
        "BAN-1,114,Rue de Morienval,60350,Pierrefonds,2.9801,49.3462,700000,6900000\n",
        encoding="utf-8",
    )

    con = _make_db()
    try:
        monkeypatch.setattr(legacy, "BAN_60_PATH", csv_path)
        legacy.load_ban_addresses(con)
        ban_row = con.execute("select x_l93, y_l93 from ref_ban_address where ban_id = 'BAN-1'").fetchone()
        con.execute(
            """
            insert into ref_sites
            (site_id, migration_id, reference, userreference, address1, function_code, reseau_code,
             manager_code, owner_code, precision_code, project_code, normalized_reference,
             normalized_userreference, normalized_address, geom_x, geom_y, geom_source, srid)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "SITE-PF",
                "SIT-1",
                "INSTITUT CHARLES QUENTIN PIERREFONDS",
                "",
                "114 RUE DE MORIENVAL 60350 PIERREFONDS",
                None,
                None,
                None,
                None,
                None,
                "",
                legacy.norm_text("INSTITUT CHARLES QUENTIN PIERREFONDS"),
                "",
                legacy.norm_text("114 RUE DE MORIENVAL 60350 PIERREFONDS"),
                    ban_row["x_l93"],
                    ban_row["y_l93"],
                    "test",
                    "EPSG:2154",
                ),
        )
        con.execute(
            """
            insert into service_master_active
            (service_id, service_key, nature_service, principal_client, principal_offer,
             principal_external_ref, principal_internal_ref, route_refs_json, service_refs_json,
             endpoint_a_raw, endpoint_z_raw, client_final, line_count, active_line_count)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "SVC-001",
                "KEY-1",
                "IRU FON",
                "ACME",
                "FON",
                "",
                "",
                "[]",
                "[]",
                "114 Rue de Morienval Pierrefonds",
                "",
                "ACME",
                1,
                1,
            ),
        )
        con.execute(
            """
            insert into lea_active_lines
            values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "LEA-1",
                "ACME",
                "FON",
                "IRU FON",
                "principal",
                "INT-1",
                "EXT-1",
                "FILE-1",
                "114 Rue de Morienval Pierrefonds",
                "",
                "ACME",
                "",
                "",
                "",
                "",
                "40",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "[]",
                "[]",
                0,
                "KEY",
                1,
                json.dumps({"Adresse site": "114 Rue de Morienval Pierrefonds"}, ensure_ascii=True),
            ),
        )
        con.execute(
            "insert into service_bss_line (service_id, lea_line_id, role_ligne, is_principal) values (?, ?, ?, ?)",
            ("SVC-001", "LEA-1", "principal", 1),
        )
        con.commit()

        legacy.reconcile_services(con)

        endpoint = con.execute(
            "select matched_site_id, spatial_score, spatial_adjusted_score, spatial_rule, selected_signal_kind from service_endpoint where service_id = ? and endpoint_label = 'A'",
            ("SVC-001",),
        ).fetchone()
        assert endpoint["matched_site_id"] == "SITE-PF"
        assert endpoint["spatial_score"] is not None
        assert endpoint["spatial_adjusted_score"] is not None
        assert endpoint["spatial_rule"] == "site_spatial_proximity"
        assert endpoint["selected_signal_kind"] in {"postal_address_precise", "mixed_site_address", None}

        spatial_evidence = con.execute(
            "select evidence_type, target_table, target_id, adjusted_score from service_spatial_evidence where service_id = ?",
            ("SVC-001",),
        ).fetchall()
        assert any(row["target_table"] == "ref_sites" for row in spatial_evidence)
        assert any(row["adjusted_score"] is not None for row in spatial_evidence)
    finally:
        con.close()
