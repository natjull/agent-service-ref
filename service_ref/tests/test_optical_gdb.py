from __future__ import annotations

import sqlite3

from service_ref import build_service_referential as legacy


def _make_db() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    legacy.create_schema(con)
    return con


def _insert_ref_site(
    con: sqlite3.Connection,
    *,
    site_id: str,
    migration_id: str,
    reference: str,
    userreference: str = "",
    address1: str = "",
) -> None:
    con.execute(
        """
        insert into ref_sites values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            site_id,
            migration_id,
            reference,
            userreference,
            address1,
            None,
            None,
            None,
            None,
            None,
            "",
            legacy.norm_text(reference),
            legacy.norm_text(userreference),
            legacy.norm_text(address1),
        ),
    )


def test_extract_place_tokens_keeps_site_like_words() -> None:
    tokens = legacy.extract_place_tokens(
        "TELOISE - 24FO - POP TELOISE CHG BEAUVAIS / RUE DU BRULET",
        "BAIE XX POP HOTEL DE POLICE COMPIEGNE",
    )

    assert "BEAUVAIS" in tokens
    assert "COMPIEGNE" in tokens
    assert "TELOISE" not in tokens
    assert "POP" not in tokens


def test_extract_fiber_count_reads_fo_pattern() -> None:
    assert legacy.extract_fiber_count("TELOISE - 24FO - POP BEAUVAIS") == 24
    assert legacy.extract_fiber_count("BAGUETTE - 144 FO - RUE DU MOULIN") == 144
    assert legacy.extract_fiber_count("NO FIBER COUNT HERE") is None


def test_hubsite_lookup_by_oid_returns_site_id_and_name() -> None:
    con = _make_db()
    try:
        _insert_ref_site(
            con,
            site_id="NW_HUBSITE_1",
            migration_id="SIT-001",
            reference="POP BEAUVAIS",
        )

        lookup = legacy._hubsite_lookup_by_oid(con)

        assert lookup == {"NW_HUBSITE_1": ("NW_HUBSITE_1", "POP BEAUVAIS")}
    finally:
        con.close()


def test_load_routes_builds_optical_tables_from_gdb(monkeypatch) -> None:
    con = _make_db()
    try:
        _insert_ref_site(
            con,
            site_id="NW_HUBSITE_BEAUVAIS",
            migration_id="SIT-BEA",
            reference="POP BEAUVAIS",
        )
        _insert_ref_site(
            con,
            site_id="NW_HUBSITE_COMPIEGNE",
            migration_id="SIT-COM",
            reference="POP COMPIEGNE",
        )
        con.commit()

        fake_layers = {
            "IMPORT_ISP_TEMPLATE": [
                {
                    "ISPMIGOID": "NW_RACK_1",
                    "PARENTTYPE": "Room",
                    "PARENTMIGOID": "NW_ROOM_1",
                }
            ],
            "Chamber__Hubsite": [
                {
                    "CHAMBER_MIGRATION_OID": "NW_CHAMBER_1",
                    "HUBSITE_MIGRATION_OID": "NW_HUBSITE_BEAUVAIS",
                }
            ],
            "SupportStructure__Hubsite": [],
            "Rack": [
                {
                    "MIGRATION_OID": "NW_RACK_1",
                    "REFERENCE": "BAIE XX POP BEAUVAIS",
                    "USERREFERENCE": "RACK-BEA",
                    "DESCRIPTION": "COFFRET OPTIQUE",
                    "COMMENTS": "",
                    "LOCATION": "",
                }
            ],
            "OptPatchPanel": [
                {
                    "MIGRATION_OID": "NW_OPTPATCHPANEL_1",
                    "REFERENCE": "PATCH POP COMPIEGNE",
                    "USERREFERENCE": "",
                    "DESCRIPTION": "TIROIR",
                    "COMMENTS": "",
                    "LOCATION": "",
                }
            ],
            "Room": [],
            "Chamber": [
                {
                    "MIGRATION_OID": "NW_CHAMBER_1",
                    "REFERENCE": "CHAMBRE POP BEAUVAIS",
                    "USERREFERENCE": "",
                    "DESCRIPTION": "",
                    "COMMENTS": "",
                    "LOCATION": "",
                }
            ],
            "Enclosure": [],
            "Pedestal": [],
            "ISPEnclosure": [],
            "ISPManifold": [],
            "CONNEXION_TEMPLATE": [
                {
                    "HOUSING_TYPE": "Pedestal",
                    "HOUSINGMIGOID": "NW_PEDESTAL_1",
                    "OBJ1_TYPE": "Fiber_Cable",
                    "OBJ1_MIGOID": "NW_FIBER_CABLE_1",
                    "OBJ1_CONNECTOR1": 1,
                    "OBJ1_CONNECTOR2": 1,
                    "OBJ2_TYPE": "OptPatchPanel",
                    "OBJ2_MIGOID": "NW_OPTPATCHPANEL_1",
                    "OBJ2_CONNECTOR1": 1,
                    "OBJ2_CONNECTOR2": 1,
                    "TYPE_BRANCHEMENT": "Implicit",
                    "TRAY_MIGRATIONID": "",
                    "ID_RATTACHEMENT": None,
                }
            ],
            "Fiber_Cable": [
                {
                    "MIGRATION_OID": "NW_FIBER_CABLE_1",
                    "MIGRATION_ID": "CAB-001",
                    "REFERENCE": "TELOISE - 24FO - POP BEAUVAIS / POP COMPIEGNE",
                    "USERREFERENCE": "CAB-BEA-COM",
                    "COMMENTS": "RACCORDEMENT CLIENT",
                    "LABELTEXT": "",
                    "LOCATION": "",
                    "CABLETYPE": "1",
                    "RESEAU": "1",
                    "CODE_PROJET": "PRJ",
                    "STATUS": "2",
                }
            ],
        }

        monkeypatch.setattr(
            legacy,
            "iter_gdb_records",
            lambda layer: iter(fake_layers.get(layer, [])),
        )

        legacy.load_routes(con)

        assert con.execute("select count(*) from ref_optical_cable").fetchone()[0] == 1
        assert con.execute("select count(*) from ref_optical_housing").fetchone()[0] == 3
        assert con.execute("select count(*) from ref_optical_connection").fetchone()[0] == 1
        assert con.execute("select count(*) from ref_optical_site_link").fetchone()[0] == 1

        cable = con.execute(
            "select reference, number_of_fibers, site_tokens_json from ref_optical_cable"
        ).fetchone()
        assert cable["reference"] == "TELOISE - 24FO - POP BEAUVAIS / POP COMPIEGNE"
        assert cable["number_of_fibers"] == 24
        assert "BEAUVAIS" in cable["site_tokens_json"]
        assert "COMPIEGNE" in cable["site_tokens_json"]
    finally:
        con.close()


def test_load_lease_tables_builds_logical_routes_and_endpoints(monkeypatch) -> None:
    con = _make_db()
    try:
        _insert_ref_site(
            con,
            site_id="NW_HUBSITE_BEAUVAIS",
            migration_id="SIT-BEA",
            reference="POP BEAUVAIS",
        )
        _insert_ref_site(
            con,
            site_id="NW_HUBSITE_COMPIEGNE",
            migration_id="SIT-COM",
            reference="POP COMPIEGNE",
        )
        con.execute(
            """
            insert into ref_optical_site_link values (?,?,?,?,?,?,?)
            """,
            (
                "Rack",
                "NW_RACK_1",
                "NW_HUBSITE_BEAUVAIS",
                "POP BEAUVAIS",
                "gdb_relation",
                100,
                "SupportStructure__Hubsite",
            ),
        )
        con.execute(
            """
            insert into ref_optical_site_link values (?,?,?,?,?,?,?)
            """,
            (
                "Rack",
                "NW_RACK_2",
                "NW_HUBSITE_COMPIEGNE",
                "POP COMPIEGNE",
                "gdb_relation",
                100,
                "SupportStructure__Hubsite",
            ),
        )
        con.commit()

        fake_layers = {
            "LEASE_TEMPLATE": [
                {
                    "REF_EXPLOIT": "TOIP 0181",
                    "RESEAU": "1",
                    "LESSEE": "SFR",
                    "CLIENTS": "ADISTA",
                    "HOUSINGTYPEL1": "Rack",
                    "HOUSINGMIGOIDL1": "NW_RACK_1",
                    "TYPEL1": "Rack",
                    "MIGOIDL1": "NW_RACK_1",
                    "L1_CONN1": 1,
                    "L1_CONN2": 1,
                    "REFERENCEL1": "BAIE POP BEAUVAIS",
                    "HOUSINGTYPEL2": "Rack",
                    "HOUSINGMIGOIDL2": "NW_RACK_2",
                    "TYPEL2": "Rack",
                    "MIGOIDL2": "NW_RACK_2",
                    "L2_CONN1": 2,
                    "L2_CONN2": 2,
                    "REFERENCEL2": "BAIE POP COMPIEGNE",
                    "COMMENTS": "CODE:TOFO10",
                }
            ],
            "Fiber_Lease": [
                {
                    "MIGRATION_OID": "NW_FLEASE_1",
                    "FEATURE": "Fiber_Cable",
                    "OID": 11,
                    "STARTRANGE": 1,
                    "ENDRANGE": 2,
                    "REFERENCE": "TOIP 0181 - POP BEAUVAIS/POP COMPIEGNE",
                    "LESSEE": "SFR",
                    "SERVICE": 1,
                    "STATUS": 2,
                    "CLIENT": "ADISTA",
                    "RESEAU": 1,
                    "REF_EXPLOIT": "TOIP 0181",
                    "COMMENTS": "COMMENTAIRE TOIP 0181",
                    "PATHID": "PATH-001",
                    "PAIROID": 44,
                }
            ],
            "ISPLease": [
                {
                    "MIGRATION_OID": "NW_ISPLEASE_1",
                    "FEATURE": "Connector",
                    "OID": 22,
                    "REFERENCE": "TOIP 0181 - POP BEAUVAIS/POP COMPIEGNE",
                    "LESSEE": "SFR",
                    "SERVICE": 1,
                    "STATUS": 2,
                    "CLIENT": "ADISTA",
                    "RESEAU": 1,
                    "REF_EXPLOIT": "TOIP 0181",
                    "COMMENTS": "CODE:TOFO10",
                    "PAIROID": 55,
                    "ISPCONTAINERID": 66,
                    "PATHID": "PATH-002",
                }
            ],
        }

        monkeypatch.setattr(
            legacy,
            "iter_gdb_records",
            lambda layer: iter(fake_layers.get(layer, [])),
        )

        legacy.load_lease_tables(con)

        assert con.execute("select count(*) from ref_optical_logical_route").fetchone()[0] >= 1
        assert con.execute("select count(*) from ref_optical_lease").fetchone()[0] == 3
        assert con.execute("select count(*) from ref_optical_lease_endpoint").fetchone()[0] == 2

        logical = con.execute(
            "select route_ref, source_layer from ref_optical_logical_route where route_ref = 'TOIP 0181' order by source_layer"
        ).fetchall()
        assert [row["source_layer"] for row in logical] == ["Fiber_Lease", "ISPLease", "LEASE_TEMPLATE"]

        endpoints = con.execute(
            """
            select endpoint_label, site_id, site_name
            from ref_optical_lease_endpoint
            order by endpoint_label
            """
        ).fetchall()
        assert endpoints[0]["endpoint_label"] == "L1"
        assert endpoints[0]["site_id"] == "NW_HUBSITE_BEAUVAIS"
        assert endpoints[1]["endpoint_label"] == "L2"
        assert endpoints[1]["site_id"] == "NW_HUBSITE_COMPIEGNE"

        route = con.execute(
            "select route_id, route_ref from ref_routes where route_ref = 'TOIP 0181'"
        ).fetchone()
        assert route is not None

        parcours = con.execute(
            "select step_type, site from ref_route_parcours where route_ref = 'TOIP 0181' order by step_no"
        ).fetchall()
        assert [row["step_type"] for row in parcours] == ["origin", "destination"]
        assert parcours[0]["site"] == "POP BEAUVAIS"
        assert parcours[1]["site"] == "POP COMPIEGNE"
    finally:
        con.close()
