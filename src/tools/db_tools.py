"""MCP tools for querying the service referential SQLite database."""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any

from ..sdk_compat import tool
from .text_utils import normalize_alias

_db_path: Path | None = None

_ALLOWED_FIRST_WORDS = frozenset({"SELECT", "PRAGMA", "EXPLAIN", "WITH"})
_MAX_ROWS = 100


def _strip_leading_sql_comments(sql: str) -> str:
    text = sql.lstrip()
    while True:
        if text.startswith("--"):
            newline = text.find("\n")
            text = "" if newline == -1 else text[newline + 1 :].lstrip()
            continue
        if text.startswith("/*"):
            end = text.find("*/")
            text = "" if end == -1 else text[end + 2 :].lstrip()
            continue
        return text


def configure(db_path: Path) -> None:
    global _db_path
    _db_path = Path(db_path)


def _connect(*, read_only: bool = False) -> sqlite3.Connection:
    if _db_path is None or not _db_path.exists():
        raise RuntimeError(f"Database not configured or missing: {_db_path}")
    con = sqlite3.connect(str(_db_path))
    con.execute("PRAGMA journal_mode=WAL")
    if read_only:
        con.execute("PRAGMA query_only = ON")
    con.row_factory = sqlite3.Row
    return con


def _guard_sql(sql: str) -> str | None:
    """Return an error message if the SQL is not a read-only statement."""
    stripped = _strip_leading_sql_comments(sql)
    if not stripped:
        return "BLOCKED: empty query."
    first_word = stripped.split()[0].upper()
    if first_word not in _ALLOWED_FIRST_WORDS:
        return f"BLOCKED: query_db is read-only ({first_word} not allowed). Use submit_resolution to write."
    return None


def _text(content: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": content}]}


def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


def _rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [_row_to_dict(row) for row in rows if row is not None]


def _fetch_service_bundle(con: sqlite3.Connection, service_id: str) -> dict[str, Any]:
    service = con.execute(
        "SELECT * FROM service_master_active WHERE service_id = ?",
        (service_id,),
    ).fetchone()

    evidences = con.execute(
        """
        SELECT evidence_type, rule_name, score, source_table, source_key, payload_json
        FROM service_match_evidence
        WHERE service_id = ?
        ORDER BY score DESC, evidence_type, rule_name
        LIMIT 10
        """,
        (service_id,),
    ).fetchall()
    review_items = con.execute(
        """
        SELECT review_type, severity, reason, context_json
        FROM service_review_queue
        WHERE service_id = ?
        ORDER BY severity DESC, review_type
        """,
        (service_id,),
    ).fetchall()
    party_rows = con.execute(
        """
        SELECT service_id, role_name, party_id, rule_name, score
        FROM service_party
        WHERE service_id = ?
        ORDER BY score DESC, role_name
        """,
        (service_id,),
    ).fetchall()
    endpoint_rows = con.execute(
        """
        SELECT service_id, endpoint_label, raw_value, matched_site_id, matched_site_name, score, rule_name
        FROM service_endpoint
        WHERE service_id = ?
        ORDER BY score DESC, endpoint_label
        """,
        (service_id,),
    ).fetchall()
    network_support_rows = con.execute(
        """
        SELECT *
        FROM service_support_reseau
        WHERE service_id = ?
        """,
        (service_id,),
    ).fetchall()
    optical_support_rows = con.execute(
        """
        SELECT *
        FROM service_support_optique
        WHERE service_id = ?
        """,
        (service_id,),
    ).fetchall()
    gold_row = con.execute(
        "SELECT * FROM gold_service_active WHERE service_id = ?",
        (service_id,),
    ).fetchone()
    spatial_seed_rows = (
        con.execute(
            """
            SELECT service_id, seed_type, seed_priority, raw_value, normalized_value,
                   street_hint, house_number_hint, city_hint, postcode_hint, insee_hint,
                   ban_id, match_rule, match_score, x_l93, y_l93, source_table, source_column,
                   source_signal_kind, source_signal_score, source_semantic_strength,
                   xy_precision_class, xy_discriminance_score, same_xy_count_in_city,
                   is_reused_xy, is_heavily_reused_xy
            FROM service_spatial_seed
            WHERE service_id = ?
            ORDER BY seed_priority DESC, match_score DESC, source_column
            """,
            (service_id,),
        ).fetchall()
        if con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='service_spatial_seed'"
        ).fetchone()
        else []
    )
    spatial_evidence_rows = (
        con.execute(
            """
            SELECT service_id, evidence_type, seed_type, target_table, target_id,
                   distance_meters, score, rule_name, context_json
                   , seed_discriminance_score, adjusted_score
            FROM service_spatial_evidence
            WHERE service_id = ?
            ORDER BY score DESC, distance_meters ASC
            """,
            (service_id,),
        ).fetchall()
        if con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='service_spatial_evidence'"
        ).fetchone()
        else []
    )
    lea_signal_rows = (
        con.execute(
            """
            SELECT *
            FROM service_lea_signal
            WHERE service_id = ?
            ORDER BY source_priority DESC, signal_score DESC, source_column
            """,
            (service_id,),
        ).fetchall()
        if con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='service_lea_signal'"
        ).fetchone()
        else []
    )

    # Fetch raw LEA lines linked to this service via service_key = grouping_key
    service_key = service["service_key"] if service else None
    lea_raw_rows: list[dict[str, Any]] = []
    if service_key:
        lea_raw_rows = _rows_to_dicts(
            con.execute(
                "SELECT * FROM lea_active_lines WHERE grouping_key = ?",
                (service_key,),
            ).fetchall()
        )

    return {
        "service": _row_to_dict(service),
        "lea_raw_lines": lea_raw_rows,
        "review_items": [
            {
                "review_type": row["review_type"],
                "severity": row["severity"],
                "reason": row["reason"],
                "context": json.loads(row["context_json"])
                if row["context_json"]
                else None,
            }
            for row in review_items
        ],
        "pipeline_evidences": [
            {
                "evidence_type": row["evidence_type"],
                "rule_name": row["rule_name"],
                "score": row["score"],
                "source_table": row["source_table"],
                "source_key": row["source_key"],
                "payload": json.loads(row["payload_json"])
                if row["payload_json"]
                else None,
            }
            for row in evidences
        ],
        "party_rows": _rows_to_dicts(party_rows),
        "endpoint_rows": _rows_to_dicts(endpoint_rows),
        "network_pipeline_hints": _rows_to_dicts(network_support_rows),
        "optical_pipeline_hints": _rows_to_dicts(optical_support_rows),
        "lea_signal_rows": _rows_to_dicts(lea_signal_rows),
        "spatial_seed_rows": _rows_to_dicts(spatial_seed_rows),
        "spatial_evidence_rows": [
            {
                **_row_to_dict(row),
                "context": json.loads(row["context_json"])
                if row["context_json"]
                else None,
            }
            for row in spatial_evidence_rows
        ],
        "gold_row": _row_to_dict(gold_row),
    }


def _resolve_party_candidates(
    con: sqlite3.Connection, service_id: str
) -> dict[str, Any]:
    service = con.execute(
        """
        SELECT service_id, principal_client, client_final
        FROM service_master_active
        WHERE service_id = ?
        """,
        (service_id,),
    ).fetchone()
    if service is None:
        return {
            "service_id": service_id,
            "principal_client_raw": "",
            "client_final_raw": "",
            "pipeline_contract_parties": [],
            "pipeline_final_parties": [],
            "principal_client_alias_matches": [],
            "client_final_alias_matches": [],
            "recommended_final_party_id": None,
            "recommended_final_party_name": None,
            "recommendation_confidence": "none",
            "reason": "service_id not found in service_master_active",
        }

    pipeline_contract = con.execute(
        """
        SELECT sp.party_id, pm.canonical_name, sp.rule_name, sp.score
        FROM service_party sp
        LEFT JOIN party_master pm ON pm.party_id = sp.party_id
        WHERE sp.service_id = ? AND sp.role_name = 'contract_party'
        ORDER BY sp.score DESC, sp.party_id
        """,
        (service_id,),
    ).fetchall()
    pipeline_final = con.execute(
        """
        SELECT sp.party_id, pm.canonical_name, sp.rule_name, sp.score
        FROM service_party sp
        LEFT JOIN party_master pm ON pm.party_id = sp.party_id
        WHERE sp.service_id = ? AND sp.role_name = 'final_party'
        ORDER BY sp.score DESC, sp.party_id
        """,
        (service_id,),
    ).fetchall()

    def alias_matches(raw_value: object) -> list[dict[str, Any]]:
        normalized = normalize_alias(raw_value)
        if not normalized:
            return []
        rows = con.execute(
            """
            SELECT pa.party_id, pm.canonical_name, pa.alias_value, pa.normalized_alias,
                   pa.source_table, pa.source_key
            FROM party_alias pa
            LEFT JOIN party_master pm ON pm.party_id = pa.party_id
            WHERE pa.normalized_alias = ?
            ORDER BY pm.canonical_name, pa.alias_value
            """,
            (normalized,),
        ).fetchall()
        matches = []
        for row in rows:
            matches.append(
                {
                    "party_id": row["party_id"],
                    "canonical_name": row["canonical_name"],
                    "alias_value": row["alias_value"],
                    "normalized_alias": row["normalized_alias"],
                    "source_table": row["source_table"],
                    "source_key": row["source_key"],
                }
            )
        return matches

    principal_alias_matches = alias_matches(service["principal_client"])
    client_final_alias_matches = alias_matches(service["client_final"])

    return {
        "service_id": service_id,
        "principal_client_raw": service["principal_client"] or "",
        "client_final_raw": service["client_final"] or "",
        "pipeline_contract_parties": _rows_to_dicts(pipeline_contract),
        "pipeline_final_parties": _rows_to_dicts(pipeline_final),
        "principal_client_alias_matches": principal_alias_matches,
        "client_final_alias_matches": client_final_alias_matches,
    }


def _resolve_optical_candidates(
    con: sqlite3.Connection, service_id: str
) -> dict[str, Any]:
    gold_cols = {
        row["name"]
        for row in con.execute('PRAGMA table_info("gold_service_active")').fetchall()
    }
    optical_cols = {
        row["name"]
        for row in con.execute(
            'PRAGMA table_info("service_support_optique")'
        ).fetchall()
    }
    gold_select = [
        col
        for col in (
            "route_ref",
            "route_id",
            "lease_id",
            "fiber_lease_id",
            "isp_lease_id",
        )
        if col in gold_cols
    ]
    optical_select = [
        col
        for col in (
            "route_ref",
            "route_id",
            "route_match_rule",
            "route_score",
            "lease_ref",
            "lease_id",
            "lease_match_rule",
            "lease_score",
            "fiber_lease_id",
            "fiber_lease_match_rule",
            "fiber_lease_score",
            "isp_lease_id",
            "isp_lease_match_rule",
            "isp_lease_score",
            "support_type",
            "support_ref",
            "logical_route_id",
            "cable_id",
            "cable_match_rule",
            "cable_score",
            "housing_id",
            "housing_match_rule",
            "housing_score",
            "site_a_optical_id",
            "site_z_optical_id",
            "optical_context_json",
            "spatial_match_rule",
            "spatial_distance_meters",
            "spatial_score",
        )
        if col in optical_cols
    ]

    gold_row = (
        con.execute(
            f"SELECT {', '.join(gold_select)} FROM gold_service_active WHERE service_id = ?",
            (service_id,),
        ).fetchone()
        if gold_select
        else None
    )
    support_rows = (
        con.execute(
            f"SELECT {', '.join(optical_select)} FROM service_support_optique WHERE service_id = ?",
            (service_id,),
        ).fetchall()
        if optical_select
        else []
    )
    logical_routes = (
        con.execute(
            """
            SELECT logical_route_id, route_ref, source_layer, reference, path_id, pair_oid,
                   feature_type, lessee, client, network, status
            FROM ref_optical_logical_route
            WHERE route_ref IN (
                SELECT route_ref FROM service_support_optique WHERE service_id = ? AND route_ref IS NOT NULL
            )
            ORDER BY route_ref, source_layer
            LIMIT 10
            """,
            (service_id,),
        ).fetchall()
        if _row_to_dict(
            con.execute(
                "SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name='ref_optical_logical_route'"
            ).fetchone()
        )
        else []
    )
    lease_endpoints = (
        con.execute(
            """
            SELECT ole.optical_lease_id, ole.lease_kind, ole.ref_exploit, ole.reference,
                   ep.endpoint_label, ep.reference_label, ep.site_id, ep.site_name, ep.site_score
            FROM ref_optical_lease ole
            JOIN ref_optical_lease_endpoint ep ON ep.optical_lease_id = ole.optical_lease_id
            WHERE ole.optical_lease_id IN (
                SELECT lease_id FROM service_support_optique WHERE service_id = ? AND lease_id IS NOT NULL
                UNION
                SELECT fiber_lease_id FROM service_support_optique WHERE service_id = ? AND fiber_lease_id IS NOT NULL
                UNION
                SELECT isp_lease_id FROM service_support_optique WHERE service_id = ? AND isp_lease_id IS NOT NULL
            )
            ORDER BY ole.optical_lease_id, ep.endpoint_label
            LIMIT 20
            """,
            (service_id, service_id, service_id),
        ).fetchall()
        if _row_to_dict(
            con.execute(
                "SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name='ref_optical_lease_endpoint'"
            ).fetchone()
        )
        else []
    )
    cable_candidates = (
        con.execute(
            """
            SELECT DISTINCT c.cable_id, c.reference, c.userreference, c.number_of_fibers,
                   c.site_tokens_json
            FROM ref_optical_cable c
            JOIN service_support_optique s ON s.cable_id = c.cable_id
            WHERE s.service_id = ?
            ORDER BY c.number_of_fibers ASC, c.reference
            LIMIT 10
            """,
            (service_id,),
        ).fetchall()
        if _row_to_dict(
            con.execute(
                "SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name='ref_optical_cable'"
            ).fetchone()
        )
        else []
    )
    housing_candidates = (
        con.execute(
            """
            SELECT DISTINCT h.housing_id, h.housing_type, h.reference, h.description,
                   h.site_id, h.site_name
            FROM ref_optical_housing h
            JOIN service_support_optique s ON s.housing_id = h.housing_id
            WHERE s.service_id = ?
            ORDER BY h.housing_type, h.reference
            LIMIT 10
            """,
            (service_id,),
        ).fetchall()
        if _row_to_dict(
            con.execute(
                "SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name='ref_optical_housing'"
            ).fetchone()
        )
        else []
    )
    # Search cables by site tokens matching service endpoints A/Z
    nearby_cables: list[dict[str, Any]] = []
    has_cable_table = _row_to_dict(
        con.execute(
            "SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name='ref_optical_cable'"
        ).fetchone()
    )
    if has_cable_table:
        endpoint_sites = con.execute(
            "SELECT matched_site_name FROM service_endpoint WHERE service_id = ? AND matched_site_name IS NOT NULL",
            (service_id,),
        ).fetchall()
        site_tokens: list[str] = []
        for row in endpoint_sites:
            name = (row["matched_site_name"] or "").upper()
            for token in re.split(r"[^A-Z0-9]+", name):
                if len(token) >= 4 and token not in {
                    "SITE",
                    "POP",
                    "NRA",
                    "TELOISE",
                    "FRANCE",
                    "NORD",
                    "SUD",
                    "EST",
                    "OUEST",
                    "SAINT",
                    "COMMUNE",
                    "VILLE",
                    "MAIRIE",
                    "AGENCE",
                    "HOTEL",
                }:
                    site_tokens.append(token)
        matched_cable_ids = (
            {r["cable_id"] for r in cable_candidates} if cable_candidates else set()
        )
        for token in dict.fromkeys(site_tokens):  # deduplicate preserving order
            if len(nearby_cables) >= 10:
                break
            rows = con.execute(
                "SELECT cable_id, reference, userreference, number_of_fibers, site_tokens_json "
                "FROM ref_optical_cable WHERE site_tokens_json LIKE ?",
                (f'%"{token}"%',),
            ).fetchall()
            for r in rows:
                if r["cable_id"] not in matched_cable_ids:
                    nearby_cables.append(_row_to_dict(r))
                    matched_cable_ids.add(r["cable_id"])

    # Route parcours: routes passing through the service's resolved sites
    route_parcours: list[dict[str, Any]] = []
    has_parcours_table = _row_to_dict(
        con.execute(
            "SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name='ref_route_parcours'"
        ).fetchone()
    )
    if has_parcours_table and site_tokens:
        seen_route_refs: set[str] = set()
        for token in dict.fromkeys(site_tokens):
            if len(route_parcours) >= 15:
                break
            rows = con.execute(
                "SELECT route_ref, step_type, site, bpe, cable_in, cable_out "
                "FROM ref_route_parcours WHERE UPPER(site) LIKE ? "
                "ORDER BY route_ref, step_no LIMIT 20",
                (f"%{token}%",),
            ).fetchall()
            for r in rows:
                rr = (r["route_ref"] or "").strip()
                if rr and rr not in seen_route_refs:
                    route_parcours.append(_row_to_dict(r))
                    seen_route_refs.add(rr)

    return {
        "service_id": service_id,
        "gold_optical": _row_to_dict(gold_row),
        "pipeline_hints": _rows_to_dicts(
            support_rows
        ),  # candidats pipeline (hints, pas verites)
        "logical_routes": _rows_to_dicts(logical_routes),
        "lease_endpoints": _rows_to_dicts(lease_endpoints),
        "cable_candidates": _rows_to_dicts(cable_candidates),
        "housing_candidates": _rows_to_dicts(housing_candidates),
        "nearby_cables_by_site": nearby_cables,
        "route_parcours_by_site": route_parcours,
    }


def _resolve_network_candidates(
    con: sqlite3.Connection, service_id: str
) -> dict[str, Any]:
    # 1. Gold et hints pipeline (service_support_reseau apres Phase 2.1 = multi-VLAN)
    gold_cols = {
        row["name"]
        for row in con.execute('PRAGMA table_info("gold_service_active")').fetchall()
    }
    network_cols = {
        row["name"]
        for row in con.execute('PRAGMA table_info("service_support_reseau")').fetchall()
    }
    gold_select = [
        col
        for col in (
            "interface_id",
            "network_interface_id",
            "network_vlan_id",
            "cpe_id",
            "config_id",
            "inferred_vlans_json",
        )
        if col in gold_cols
    ]
    network_select = [
        col
        for col in (
            "service_ref",
            "interface_id",
            "interface_match_rule",
            "interface_score",
            "network_interface_id",
            "network_interface_match_rule",
            "network_interface_score",
            "network_vlan_id",
            "network_vlan_match_rule",
            "network_vlan_score",
            "cpe_id",
            "cpe_match_rule",
            "cpe_score",
            "config_id",
            "config_match_rule",
            "config_score",
            "inferred_vlans_json",
        )
        if col in network_cols
    ]
    gold_row = (
        con.execute(
            f"SELECT {', '.join(gold_select)} FROM gold_service_active WHERE service_id = ?",
            (service_id,),
        ).fetchone()
        if gold_select
        else None
    )
    support_rows = (
        con.execute(
            f"SELECT {', '.join(network_select)} FROM service_support_reseau WHERE service_id = ?",
            (service_id,),
        ).fetchall()
        if network_select
        else []
    )

    payload: dict[str, Any] = {
        "service_id": service_id,
        "gold_network": _row_to_dict(gold_row),
        "network_candidates": _rows_to_dicts(support_rows),
    }
    if gold_row and gold_row["inferred_vlans_json"]:
        payload["gold_network"]["inferred_vlans"] = json.loads(
            gold_row["inferred_vlans_json"]
        )
    for row in payload["network_candidates"]:
        if row.get("inferred_vlans_json"):
            row["inferred_vlans"] = json.loads(row["inferred_vlans_json"])

    # 2. Tokens client pour les sections de recherche directe
    svc = con.execute(
        "SELECT client_final, endpoint_z_raw, endpoint_a_raw FROM service_master_active WHERE service_id = ?",
        (service_id,),
    ).fetchone()
    _NOISE = {
        "SAINT",
        "ROUTE",
        "AVENUE",
        "FRANCE",
        "AGENCE",
        "COMPLETEL",
        "ADISTA",
        "HEXANET",
        "TELOISE",
        "TELECOM",
    }
    client_tokens: list[str] = []
    if svc:
        for field in (svc["client_final"], svc["endpoint_z_raw"]):
            if field:
                for tok in re.split(r"[^A-Za-z0-9]+", field.upper()):
                    if len(tok) >= 5 and tok not in _NOISE and tok not in client_tokens:
                        client_tokens.append(tok)

    # 3. vlans_by_label : recherche directe dans ref_network_vlans par tokens client
    vlans_by_label: list[dict[str, Any]] = []
    seen_vlan_ids: set[str] = set()
    for tok in dict.fromkeys(client_tokens):
        if len(vlans_by_label) >= 20:
            break
        rows = con.execute(
            "SELECT network_vlan_id, device_name, device_id, vlan_id, label, service_refs_json, route_refs_json "
            "FROM ref_network_vlans WHERE UPPER(normalized_label) LIKE ? LIMIT 15",
            (f"%{tok}%",),
        ).fetchall()
        for r in rows:
            vid = r["network_vlan_id"]
            if vid not in seen_vlan_ids:
                vlans_by_label.append(_row_to_dict(r))
                seen_vlan_ids.add(vid)
    payload["vlans_by_label"] = vlans_by_label

    # 4. co_subinterfaces : CO dot1Q portant les VLANs candidats OU circuit_id LEA
    co_subinterfaces: list[dict[str, Any]] = []
    seen_subif: set[str] = set()

    # 4a. Par VLANs de service_support_reseau → ref_network_vlans.vlan_id → ref_co_subinterface
    rows = con.execute(
        """
        SELECT cs.subif_id, cs.device_name, cs.interface_name, cs.vlan_id, cs.description,
               cs.site_code, cs.xconnect_ip, cs.xconnect_circuit_id
        FROM ref_co_subinterface cs
        JOIN ref_network_vlans nv ON nv.vlan_id = cs.vlan_id
        JOIN service_support_reseau sr ON sr.network_vlan_id = nv.network_vlan_id
        WHERE sr.service_id = ?
        LIMIT 30
        """,
        (service_id,),
    ).fetchall()
    for r in rows:
        if r["subif_id"] not in seen_subif:
            co_subinterfaces.append(_row_to_dict(r))
            seen_subif.add(r["subif_id"])

    # 4b. Par circuit_id depuis signals LEA (route_refs_json)
    # Fix: xconnect_circuit_id stocke uniquement le numero (ex: "2079"), pas le prefixe "TOIP "
    if len(co_subinterfaces) < 15:
        circuit_ids: set[str] = set()
        for sig in con.execute(
            "SELECT route_refs_json FROM service_lea_signal WHERE service_id = ? AND route_refs_json IS NOT NULL",
            (service_id,),
        ).fetchall():
            try:
                for ref in json.loads(sig["route_refs_json"]):
                    if ref and len(ref) >= 4:
                        raw = ref.strip()
                        # Strip "TOIP " prefix so "TOIP 2079" matches xconnect_circuit_id="2079"
                        numeric = raw.upper().removeprefix("TOIP ").strip()
                        circuit_ids.add(numeric)
                        circuit_ids.add(raw)  # also try exact match as fallback
            except (json.JSONDecodeError, TypeError):
                pass
        for cid in circuit_ids:
            if len(co_subinterfaces) >= 20:
                break
            for r in con.execute(
                "SELECT subif_id, device_name, interface_name, vlan_id, description, "
                "site_code, xconnect_ip, xconnect_circuit_id "
                "FROM ref_co_subinterface WHERE xconnect_circuit_id = ? LIMIT 5",
                (cid,),
            ).fetchall():
                if r["subif_id"] not in seen_subif:
                    co_subinterfaces.append(_row_to_dict(r))
                    seen_subif.add(r["subif_id"])
    payload["co_subinterfaces"] = co_subinterfaces

    # 5. cpe_candidates : recherche directe ref_cpe_inventory par tokens client
    cpe_candidates: list[dict[str, Any]] = []
    seen_cpe: set[str] = set()
    for tok in dict.fromkeys(client_tokens):
        if len(cpe_candidates) >= 10:
            break
        for r in con.execute(
            "SELECT cpe_id, hostname, normalized_hostname, source "
            "FROM ref_cpe_inventory WHERE normalized_hostname LIKE ? LIMIT 5",
            (f"%{tok.lower()}%",),
        ).fetchall():
            if r["cpe_id"] not in seen_cpe:
                cpe_candidates.append(_row_to_dict(r))
                seen_cpe.add(r["cpe_id"])
    payload["cpe_candidates"] = cpe_candidates

    return payload


def _resolve_spatial_candidates(
    con: sqlite3.Connection, service_id: str
) -> dict[str, Any]:
    seeds = (
        con.execute(
            """
            SELECT seed_type, seed_priority, raw_value, city_hint, match_rule,
                   match_score, x_l93, y_l93, source_column,
                   source_signal_kind, source_signal_score, source_semantic_strength,
                   xy_precision_class, xy_discriminance_score, same_xy_count_in_city,
                   is_reused_xy, is_heavily_reused_xy
            FROM service_spatial_seed
            WHERE service_id = ?
            ORDER BY seed_priority DESC, match_score DESC
            """,
            (service_id,),
        ).fetchall()
        if con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='service_spatial_seed'"
        ).fetchone()
        else []
    )
    evidences = (
        con.execute(
            """
            SELECT evidence_type, seed_type, target_table, target_id,
                   distance_meters, score, rule_name, context_json,
                   seed_discriminance_score, adjusted_score
            FROM service_spatial_evidence
            WHERE service_id = ?
            ORDER BY adjusted_score DESC, distance_meters ASC
            LIMIT 20
            """,
            (service_id,),
        ).fetchall()
        if con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='service_spatial_evidence'"
        ).fetchone()
        else []
    )
    site_candidates = [row for row in evidences if row["target_table"] == "ref_sites"]
    housing_candidates = [
        row for row in evidences if row["target_table"] == "ref_optical_housing"
    ]
    cable_candidates = [
        row for row in evidences if row["target_table"] == "ref_optical_cable"
    ]
    best = evidences[0] if evidences else None
    return {
        "service_id": service_id,
        "spatial_seeds": _rows_to_dicts(seeds),
        "site_candidates": [
            {
                **_row_to_dict(row),
                "context": json.loads(row["context_json"])
                if row["context_json"]
                else None,
            }
            for row in site_candidates
        ],
        "housing_candidates": [
            {
                **_row_to_dict(row),
                "context": json.loads(row["context_json"])
                if row["context_json"]
                else None,
            }
            for row in housing_candidates
        ],
        "cable_candidates": [
            {
                **_row_to_dict(row),
                "context": json.loads(row["context_json"])
                if row["context_json"]
                else None,
            }
            for row in cable_candidates
        ],
        "best_spatial_evidence": (
            {
                **_row_to_dict(best),
                "context": json.loads(best["context_json"])
                if best and best["context_json"]
                else None,
            }
            if best is not None
            else None
        ),
        "reasoning_summary": (
            f"Best spatial evidence: {best['target_table']} {best['target_id']} at {best['distance_meters']}m "
            f"(adjusted={best['adjusted_score']}, seed={best['seed_type']})"
            if best is not None
            else "No spatial evidence available"
        ),
    }


def _resolve_lea_signal_candidates(
    con: sqlite3.Connection, service_id: str
) -> dict[str, Any]:
    rows = (
        con.execute(
            """
            SELECT *
            FROM service_lea_signal
            WHERE service_id = ?
            ORDER BY source_priority DESC, signal_score DESC, source_column
            """,
            (service_id,),
        ).fetchall()
        if con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='service_lea_signal'"
        ).fetchone()
        else []
    )
    payload_rows = _rows_to_dicts(rows)
    return {
        "service_id": service_id,
        "raw_lea_values": [
            {"source_column": row["source_column"], "raw_value": row["raw_value"]}
            for row in payload_rows
        ],
        "classified_signals": payload_rows,
        "recommended_address_signals": [
            row
            for row in payload_rows
            if row["signal_kind"]
            in {
                "postal_address_precise",
                "mixed_site_address",
                "postal_address_partial",
                "postcode_city",
                "city_only",
            }
        ][:5],
        "recommended_site_signals": [
            row
            for row in payload_rows
            if row["signal_kind"]
            in {
                "technical_site_anchor",
                "site_label_business",
                "mixed_site_address",
                "postal_address_precise",
                "postal_address_partial",
            }
        ][:5],
        "recommended_technical_signals": [
            row for row in payload_rows if row["signal_kind"] == "technical_site_anchor"
        ][:5],
        "recommended_route_signals": [
            row
            for row in payload_rows
            if row["signal_kind"] == "route_or_service_reference"
        ][:5],
        "noise_signals": [row for row in payload_rows if row["is_noise"]][:5],
        "reasoning_summary": (
            f"{len(payload_rows)} LEA signals classified; top="
            f"{', '.join(row['signal_kind'] for row in payload_rows[:3])}"
            if payload_rows
            else "No LEA signals available"
        ),
    }


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


@tool(
    "query_db",
    "Execute une requete SQL en lecture seule (SELECT/PRAGMA/EXPLAIN/WITH) "
    "sur le SQLite du referentiel de services. Max 100 lignes retournees. "
    "Pour ecrire des resolutions, utiliser submit_resolution.",
    {"sql": str},
)
async def query_db(args: dict[str, Any]) -> dict[str, Any]:
    sql = args.get("sql", "").strip()
    if not sql:
        return _text("ERROR: No SQL provided.")

    err = _guard_sql(sql)
    if err:
        return _text(err)

    con = _connect(read_only=True)
    try:
        cursor = con.execute(sql)
        if cursor.description is None:
            return _text("Statement executed (no result set).")

        cols = [d[0] for d in cursor.description]
        rows = cursor.fetchmany(_MAX_ROWS + 1)
        truncated = len(rows) > _MAX_ROWS
        rows = rows[:_MAX_ROWS]

        if not rows:
            return _text(f"Columns: {', '.join(cols)}\n\n(no rows)")

        # Format as markdown table
        lines = [" | ".join(cols), " | ".join("---" for _ in cols)]
        for row in rows:
            lines.append(" | ".join(str(v) if v is not None else "" for v in row))

        result = "\n".join(lines)
        if truncated:
            result += (
                f"\n\n... (truncated at {_MAX_ROWS} rows, add LIMIT to your query)"
            )
        return _text(result)
    except Exception as e:
        return _text(f"SQL ERROR: {e}")
    finally:
        con.close()


@tool(
    "list_tables",
    "Liste toutes les tables du SQLite avec leur nombre de lignes.",
    {},
)
async def list_tables(args: dict[str, Any]) -> dict[str, Any]:
    con = _connect()
    try:
        tables = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()

        lines = ["Table | Rows", "--- | ---"]
        for (name,) in tables:
            try:
                count = con.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
            except Exception:
                count = "?"
            lines.append(f"{name} | {count}")

        return _text("\n".join(lines))
    finally:
        con.close()


@tool(
    "describe_table",
    "Affiche le schema d'une table du SQLite avec des statistiques : "
    "type, nb nulls, nb valeurs distinctes, et echantillon de valeurs.",
    {"table_name": str},
)
async def describe_table(args: dict[str, Any]) -> dict[str, Any]:
    table_name = args.get("table_name", "").strip()
    if not table_name:
        return _text("ERROR: No table_name provided.")

    if not re.match(r"^[A-Za-z0-9_]+$", table_name):
        return _text(
            f"ERROR: invalid table name '{table_name}' (only alphanumeric and _ allowed)."
        )

    con = _connect()
    try:
        # Check table exists
        exists = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        if not exists:
            return _text(f"Table '{table_name}' not found.")

        row_count = con.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]

        # Get schema via PRAGMA
        columns = con.execute(f'PRAGMA table_info("{table_name}")').fetchall()

        lines = [
            f"## {table_name} ({row_count} rows)",
            "",
            "Column | Type | Nulls | Distinct | Sample",
            "--- | --- | --- | --- | ---",
        ]

        for col in columns:
            col_name = col["name"]
            col_type = col["type"] or "TEXT"

            if row_count == 0:
                lines.append(f"{col_name} | {col_type} | - | - | -")
                continue

            try:
                nulls = con.execute(
                    f'SELECT COUNT(*) FROM "{table_name}" WHERE "{col_name}" IS NULL'
                ).fetchone()[0]
                distinct = con.execute(
                    f'SELECT COUNT(DISTINCT "{col_name}") FROM "{table_name}" '
                    f'WHERE "{col_name}" IS NOT NULL'
                ).fetchone()[0]
                samples = con.execute(
                    f'SELECT DISTINCT "{col_name}" FROM "{table_name}" '
                    f'WHERE "{col_name}" IS NOT NULL LIMIT 5'
                ).fetchall()
                sample_str = ", ".join(str(s[0])[:40] for s in samples)
            except Exception:
                nulls = "?"
                distinct = "?"
                sample_str = ""

            lines.append(
                f"{col_name} | {col_type} | {nulls} | {distinct} | {sample_str}"
            )

        return _text("\n".join(lines))
    finally:
        con.close()


@tool(
    "fetch_service_context",
    "Retourne le bundle complet d'un service : ligne pivot, review items, "
    "evidences pipeline, parties, endpoints, supports reseau/optiques et etat gold. "
    "Ideal pour comprendre un service sans multiplier les requetes exploratoires.",
    {"service_id": str},
)
async def fetch_service_context(args: dict[str, Any]) -> dict[str, Any]:
    service_id = args.get("service_id", "").strip()
    if not service_id:
        return _text("ERROR: No service_id provided.")

    con = _connect(read_only=True)
    try:
        ctx = _fetch_service_bundle(con, service_id)
        return _text(json.dumps(ctx, indent=2, ensure_ascii=False, default=str))
    except Exception as e:
        return _text(f"ERROR: {e}")
    finally:
        con.close()


@tool(
    "get_service_decision_pack",
    "Retourne en un seul appel le contexte complet d'un service et "
    "les candidats deterministes pour party_final. A privilegier comme "
    "point d'entree nominal avant exploration libre.",
    {"service_id": str},
)
async def get_service_decision_pack(args: dict[str, Any]) -> dict[str, Any]:
    service_id = args.get("service_id", "").strip()
    if not service_id:
        return _text("ERROR: No service_id provided.")

    con = _connect(read_only=True)
    try:
        payload = _fetch_service_bundle(con, service_id)
        payload["lea_signal_pack"] = _resolve_lea_signal_candidates(con, service_id)
        payload["party_candidates"] = _resolve_party_candidates(con, service_id)
        payload["spatial_candidates"] = _resolve_spatial_candidates(con, service_id)
        return _text(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    except Exception as e:
        return _text(f"ERROR: {e}")
    finally:
        con.close()


@tool(
    "resolve_optical_candidates",
    "Retourne les supports optiques candidats pour un service, plus le support optique Gold actuel. "
    "A utiliser quand l'agent doit publier un support optique structure.",
    {"service_id": str},
)
async def resolve_optical_candidates(args: dict[str, Any]) -> dict[str, Any]:
    service_id = args.get("service_id", "").strip()
    if not service_id:
        return _text("ERROR: No service_id provided.")

    con = _connect(read_only=True)
    try:
        payload = _resolve_optical_candidates(con, service_id)
        return _text(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    except Exception as e:
        return _text(f"ERROR: {e}")
    finally:
        con.close()


@tool(
    "resolve_network_candidates",
    "Retourne les supports reseau candidats pour un service, plus le support reseau Gold actuel. "
    "A utiliser quand l'agent doit publier un support reseau structure.",
    {"service_id": str},
)
async def resolve_network_candidates(args: dict[str, Any]) -> dict[str, Any]:
    service_id = args.get("service_id", "").strip()
    if not service_id:
        return _text("ERROR: No service_id provided.")

    con = _connect(read_only=True)
    try:
        payload = _resolve_network_candidates(con, service_id)
        return _text(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    except Exception as e:
        return _text(f"ERROR: {e}")
    finally:
        con.close()


@tool(
    "resolve_lea_signal_candidates",
    "Retourne les valeurs LEA brutes d'un service, leur classification interpretee "
    "et les signaux recommandes pour adresse, site, technique et route.",
    {"service_id": str},
)
async def resolve_lea_signal_candidates(args: dict[str, Any]) -> dict[str, Any]:
    service_id = args.get("service_id", "").strip()
    if not service_id:
        return _text("ERROR: No service_id provided.")

    con = _connect(read_only=True)
    try:
        payload = _resolve_lea_signal_candidates(con, service_id)
        return _text(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    except Exception as e:
        return _text(f"ERROR: {e}")
    finally:
        con.close()


@tool(
    "resolve_spatial_candidates",
    "Retourne les seeds geocodes, les meilleures evidences spatiales et les candidats site/housing/cable "
    "pour un service. A utiliser pour departager des cas textuels ambigus.",
    {"service_id": str},
)
async def resolve_spatial_candidates(args: dict[str, Any]) -> dict[str, Any]:
    service_id = args.get("service_id", "").strip()
    if not service_id:
        return _text("ERROR: No service_id provided.")

    con = _connect(read_only=True)
    try:
        payload = _resolve_spatial_candidates(con, service_id)
        return _text(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    except Exception as e:
        return _text(f"ERROR: {e}")
    finally:
        con.close()


@tool(
    "resolve_party_candidates",
    "Retourne les candidats party pour un service : contract_party pipeline, "
    "final_party pipeline, matches d'alias sur principal_client et client_final, "
    "et une recommandation deterministe quand un final_party est prouvable.",
    {"service_id": str},
)
async def resolve_party_candidates(args: dict[str, Any]) -> dict[str, Any]:
    service_id = args.get("service_id", "").strip()
    if not service_id:
        return _text("ERROR: No service_id provided.")

    con = _connect(read_only=True)
    try:
        payload = _resolve_party_candidates(con, service_id)
        return _text(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    except Exception as e:
        return _text(f"ERROR: {e}")
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Hunt tools — chasse aux attributs cibles (route_ref + network_vlan_id)
# ---------------------------------------------------------------------------

# Mapping transitoire CO prefix → nom de ville (sera remplace par ref_co_site_crosswalk)
# Ne pas traiter comme verite absolue — certains CO couvrent plusieurs villes
_CO_PREFIX_MAP: dict[str, str] = {
    "COMPIEGNE": "COM1",
    "CREIL": "CRL1",
    "BEAUVAIS": "BEA1",
    "AMIENS": "AMI3",
}

_DEVICE_PREFIX_HINTS: dict[str, tuple[str, ...]] = {
    "COMPIEGNE": ("COM1", "CHV1", "VTT1", "THO1", "MOY1"),
    "CREIL": ("CRL1", "CHV1", "MOY1"),
    "BEAUVAIS": ("BEA1", "AVR1"),
    "AMIENS": ("AMI3",),
    "SENLIS": ("NAN1", "FTC1", "NET1"),
    "CHANTILLY": ("NET1", "FTC1", "CRL1"),
    "CHAMBLY": ("NET1", "MRU1"),
    "RANTIGNY": ("AVR1",),
    "CLAIROIX": ("VTT1",),
    "ETOUY": ("AVR1",),
    "ST JUST": ("AVR1",),
    "SAINT JUST": ("AVR1",),
    "LE MEUX": ("COM1", "MOY1"),
    "LACROIX": ("CRL1", "COM1"),
    "CROIX ST OUEN": ("CRL1", "COM1"),
    "BRETEUIL": ("BEA1", "AMI3"),
}

_VLAN_LEVEL_ORDER = {"strong": 0, "medium": 1, "weak": 2, "context_only": 3}


def _json_array(value: object) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return parsed
    return []


def _extract_service_refs(*values: object) -> list[str]:
    refs: list[str] = []
    seen: set[str] = set()
    pattern = re.compile(r"(OPE\d+/L2L\d+|LOCFON\d+|IRUFON\d+)", re.IGNORECASE)
    for value in values:
        if value is None:
            continue
        for raw in _json_array(value):
            ref = str(raw or "").strip().upper()
            if ref and ref not in seen:
                seen.add(ref)
                refs.append(ref)
        text = str(value or "")
        for match in pattern.findall(text):
            ref = str(match or "").strip().upper()
            if ref and ref not in seen:
                seen.add(ref)
                refs.append(ref)
    return refs


def _extract_client_tokens(*values: object) -> list[str]:
    noise = {
        "SAINT",
        "ROUTE",
        "AVENUE",
        "FRANCE",
        "AGENCE",
        "COMPLETEL",
        "ADISTA",
        "HEXANET",
        "TELOISE",
        "TELECOM",
        "CLIENT",
        "RESEAU",
        "SERVICE",
        "TECHNIQUE",
        "CENTRE",
        "HOSPITALIER",
        "VILLE",
        "COMMUNE",
        "SOCIETE",
        "ASSOCIATION",
    }
    tokens: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = normalize_alias(value)
        if not normalized:
            continue
        for token in normalized.split():
            if len(token) < 4 or token in noise or token.isdigit():
                continue
            if token not in seen:
                seen.add(token)
                tokens.append(token)
    return tokens


def _extract_vlan_ids(*values: object) -> list[str]:
    vlan_ids: list[str] = []
    seen: set[str] = set()
    patterns = [
        re.compile(r"/(\d{2,4})\b"),
        re.compile(r"\bVLAN[ _-]?(\d{2,4})\b", re.IGNORECASE),
        re.compile(r"\b(\d{2,4})\b"),
    ]
    for value in values:
        for item in _json_array(value):
            candidate = str(item or "").strip()
            if (
                candidate.isdigit()
                and 2 <= len(candidate) <= 4
                and candidate not in seen
            ):
                seen.add(candidate)
                vlan_ids.append(candidate)
        text = str(value or "")
        if not text:
            continue
        for pattern in patterns:
            for match in pattern.findall(text):
                candidate = str(match or "").strip()
                if not candidate.isdigit():
                    continue
                if len(candidate) == 4 and candidate.startswith("20"):
                    continue
                if candidate not in seen:
                    seen.add(candidate)
                    vlan_ids.append(candidate)
    return vlan_ids


def _infer_device_prefixes(*values: object) -> list[str]:
    prefixes: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").upper()
        for token, mapped_prefixes in _DEVICE_PREFIX_HINTS.items():
            if token in text:
                for prefix in mapped_prefixes:
                    if prefix not in seen:
                        seen.add(prefix)
                        prefixes.append(prefix)
    return prefixes


def _lookup_vlan_rows(
    con: sqlite3.Connection,
    vlan_id: str,
    *,
    device_name: str | None = None,
    prefixes: list[str] | None = None,
    limit: int = 8,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()

    def collect(query: str, params: tuple[object, ...]) -> None:
        if len(rows) >= limit:
            return
        for row in con.execute(query, params).fetchall():
            row_dict = _row_to_dict(row)
            if row_dict is None:
                continue
            key = (
                row_dict.get("network_vlan_id")
                or f"{row_dict.get('device_name')}:{row_dict.get('vlan_id')}"
            )
            if key in seen:
                continue
            seen.add(str(key))
            rows.append(row_dict)
            if len(rows) >= limit:
                break

    if device_name:
        collect(
            "SELECT network_vlan_id, device_name, vlan_id, label FROM ref_network_vlans "
            "WHERE vlan_id = ? AND device_name = ? LIMIT 4",
            (vlan_id, device_name),
        )
        prefix = (device_name or "")[:4]
        if prefix:
            collect(
                "SELECT network_vlan_id, device_name, vlan_id, label FROM ref_network_vlans "
                "WHERE vlan_id = ? AND device_name LIKE ? LIMIT 6",
                (vlan_id, f"{prefix}%"),
            )

    for prefix in prefixes or []:
        collect(
            "SELECT network_vlan_id, device_name, vlan_id, label FROM ref_network_vlans "
            "WHERE vlan_id = ? AND device_name LIKE ? LIMIT 6",
            (vlan_id, f"{prefix}%"),
        )

    collect(
        "SELECT network_vlan_id, device_name, vlan_id, label FROM ref_network_vlans "
        "WHERE vlan_id = ? LIMIT 6",
        (vlan_id,),
    )
    return rows


def _add_vlan_hypothesis(
    hypotheses: list[dict[str, Any]],
    hypothesis_index: dict[tuple[str, str, str], dict[str, Any]],
    *,
    candidate_vlan_id: str,
    candidate_device: str | None,
    source: str,
    network_vlan_id: str | None,
    evidence_for: list[str],
    evidence_against: list[str],
    proof_level: str,
    extra: dict[str, Any] | None = None,
) -> None:
    key = (
        str(candidate_vlan_id or ""),
        str(candidate_device or ""),
        str(network_vlan_id or ""),
    )
    existing = hypothesis_index.get(key)
    if existing is None:
        payload = {
            "candidate_vlan_id": str(candidate_vlan_id or ""),
            "candidate_device": candidate_device,
            "source": source,
            "network_vlan_id": network_vlan_id,
            "evidence_for": list(dict.fromkeys(evidence_for)),
            "evidence_against": list(dict.fromkeys(evidence_against)),
            "proof_level": proof_level,
        }
        if extra:
            payload.update(extra)
        hypotheses.append(payload)
        hypothesis_index[key] = payload
        return

    existing["evidence_for"] = list(
        dict.fromkeys(existing["evidence_for"] + evidence_for)
    )
    existing["evidence_against"] = list(
        dict.fromkeys(existing["evidence_against"] + evidence_against)
    )
    if _VLAN_LEVEL_ORDER.get(proof_level, 99) < _VLAN_LEVEL_ORDER.get(
        existing.get("proof_level", "weak"), 99
    ):
        existing["proof_level"] = proof_level
    if source not in str(existing.get("source") or ""):
        existing["source"] = f"{existing['source']}+{source}"
    if extra:
        for extra_key, extra_value in extra.items():
            if existing.get(extra_key) in (None, "", []):
                existing[extra_key] = extra_value


def _table_or_view_exists(con: sqlite3.Connection, name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE name = ? AND type IN ('table', 'view')",
        (name,),
    ).fetchone()
    return row is not None


def _best_spatial_seed(seeds: list[sqlite3.Row]) -> dict[str, Any] | None:
    if not seeds:
        return None
    ordered = sorted(
        seeds,
        key=lambda row: (
            int(row["match_score"] or 0),
            int(row["xy_discriminance_score"] or 0),
            -int(row["same_xy_count_in_city"] or 0),
        ),
        reverse=True,
    )
    return _row_to_dict(ordered[0])


def _norm_token_text(value: object) -> str:
    return normalize_alias(value)


def _site_label_tokens(*values: object) -> list[str]:
    tokens: list[str] = []
    seen: set[str] = set()
    excluded = {
        "SITE",
        "POP",
        "NRA",
        "TELOISE",
        "FRANCE",
        "NORD",
        "SUD",
        "EST",
        "OUEST",
        "SAINT",
        "COMMUNE",
        "VILLE",
        "MAIRIE",
        "AGENCE",
        "HOTEL",
        "BASE",
        "LOCAL",
        "CLIENT",
        "SERVICE",
        "MINISTERE",
        "DEFENSE",
    }
    for value in values:
        normalized = _norm_token_text(value)
        if not normalized:
            continue
        for token in normalized.split():
            if len(token) < 4 or token in excluded:
                continue
            if token not in seen:
                seen.add(token)
                tokens.append(token)
    return tokens


def _anchor_geom_from_site_anchor(anchor: dict[str, Any]) -> dict[str, Any] | None:
    endpoints = anchor.get("endpoints") or []
    site_assets = {
        str(asset.get("site_id") or ""): asset
        for asset in (anchor.get("site_assets") or [])
    }
    preferred_site_id = None
    for label in ("Z", "A"):
        preferred_site_id = next(
            (
                row.get("matched_site_id")
                for row in endpoints
                if row.get("endpoint_label") == label and row.get("matched_site_id")
            ),
            None,
        )
        if preferred_site_id:
            break
    if preferred_site_id and preferred_site_id in site_assets:
        asset = site_assets[preferred_site_id]
        x = asset.get("geom_x")
        y = asset.get("geom_y")
        if x is not None and y is not None:
            return {
                "x_l93": x,
                "y_l93": y,
                "source": "site_gdb",
                "site_id": preferred_site_id,
            }
    best_seed = anchor.get("best_spatial_seed") or {}
    if best_seed.get("x_l93") is not None and best_seed.get("y_l93") is not None:
        return {
            "x_l93": best_seed.get("x_l93"),
            "y_l93": best_seed.get("y_l93"),
            "source": "ban_seed",
            "site_id": preferred_site_id,
        }
    return None


def _distance_sq(x1: float, y1: float, x2: float, y2: float) -> float:
    return (x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2)


def _build_site_anchor(con: sqlite3.Connection, service_id: str) -> dict[str, Any]:
    service = con.execute(
        """SELECT service_id, nature_service, principal_client, client_final,
                  principal_external_ref, service_refs_json, endpoint_a_raw, endpoint_z_raw
           FROM service_master_active WHERE service_id = ?""",
        (service_id,),
    ).fetchone()
    if not service:
        return {"error": f"service_id '{service_id}' not found"}

    endpoints = con.execute(
        """SELECT endpoint_label, raw_value, matched_site_id, matched_site_name, score, rule_name,
                  spatial_score, spatial_distance_meters, spatial_rule, spatial_adjusted_score
           FROM service_endpoint WHERE service_id = ? ORDER BY endpoint_label""",
        (service_id,),
    ).fetchall()
    seeds = con.execute(
        """SELECT seed_type, raw_value, city_hint, postcode_hint, match_rule, match_score,
                  x_l93, y_l93, source_table, source_column,
                  xy_discriminance_score, same_xy_count_in_city,
                  is_reused_xy, is_heavily_reused_xy, xy_precision_class
           FROM service_spatial_seed
           WHERE service_id = ?
           ORDER BY seed_priority, match_score DESC
           LIMIT 8""",
        (service_id,),
    ).fetchall()
    spatial = con.execute(
        """SELECT evidence_type, seed_type, target_table, target_id, distance_meters,
                  score, rule_name, adjusted_score, context_json
           FROM service_spatial_evidence
           WHERE service_id = ?
           ORDER BY adjusted_score DESC, score DESC, distance_meters ASC
           LIMIT 10""",
        (service_id,),
    ).fetchall()

    route_refs: list[str] = []
    for sig in con.execute(
        "SELECT route_refs_json FROM service_lea_signal WHERE service_id = ? AND route_refs_json IS NOT NULL",
        (service_id,),
    ).fetchall():
        route_refs.extend(
            [
                str(ref).strip()
                for ref in _json_array(sig["route_refs_json"])
                if str(ref or "").strip()
            ]
        )

    network_hint_counts = con.execute(
        """SELECT
                SUM(CASE WHEN network_vlan_id IS NOT NULL AND TRIM(network_vlan_id) <> '' THEN 1 ELSE 0 END) AS vlan_rows,
                SUM(CASE WHEN network_interface_id IS NOT NULL AND TRIM(network_interface_id) <> '' THEN 1 ELSE 0 END) AS iface_rows,
                SUM(CASE WHEN cpe_id IS NOT NULL AND TRIM(cpe_id) <> '' THEN 1 ELSE 0 END) AS cpe_rows
           FROM service_support_reseau WHERE service_id = ?""",
        (service_id,),
    ).fetchone()

    best_seed = _best_spatial_seed(seeds)
    site_assets: list[dict[str, Any]] = []
    site_ids = []
    for row in endpoints:
        if row["matched_site_id"] and row["matched_site_id"] not in site_ids:
            site_ids.append(row["matched_site_id"])
    for site_id in site_ids:
        if _table_or_view_exists(con, "v_site_passive_assets"):
            asset_row = con.execute(
                "SELECT * FROM v_site_passive_assets WHERE site_id = ?",
                (site_id,),
            ).fetchone()
        else:
            asset_row = None
        if asset_row:
            site_assets.append(_row_to_dict(asset_row) or {})
        else:
            site_ref = con.execute(
                "SELECT reference FROM ref_sites WHERE site_id = ?",
                (site_id,),
            ).fetchone()
            housing_count = con.execute(
                "SELECT COUNT(*) AS c FROM ref_optical_housing WHERE site_id = ?",
                (site_id,),
            ).fetchone()["c"]
            lease_count = con.execute(
                "SELECT COUNT(DISTINCT optical_lease_id) AS c FROM ref_optical_lease_endpoint WHERE site_id = ?",
                (site_id,),
            ).fetchone()["c"]
            site_assets.append(
                {
                    "site_id": site_id,
                    "site_reference": site_ref["reference"] if site_ref else None,
                    "housing_count": housing_count,
                    "lease_endpoint_count": lease_count,
                }
            )

    matched_scores = [
        int(row["score"] or 0) for row in endpoints if row["matched_site_id"]
    ]
    best_site_score = max(matched_scores) if matched_scores else 0
    distinct_sites = {
        row["matched_site_id"] for row in endpoints if row["matched_site_id"]
    }
    has_distinct_pair = len(distinct_sites) >= 2
    device_prefixes = _infer_device_prefixes(
        service["endpoint_a_raw"],
        service["endpoint_z_raw"],
        *(row["matched_site_name"] for row in endpoints),
    )

    seed_heavily_reused = bool(
        best_seed and int(best_seed.get("is_heavily_reused_xy") or 0)
    )
    seed_discriminance = (
        int(best_seed.get("xy_discriminance_score") or 0) if best_seed else 0
    )

    if route_refs:
        recommended_entry_point = "route_ref_first"
        rationale = "route_refs_json LEA present - start by validating route_ref in GDB/leases, then confirm topology"
    elif has_distinct_pair and best_site_score >= 60 and not seed_heavily_reused:
        recommended_entry_point = "site_gdb_first"
        rationale = "distinct site anchors available - start from site GDB then passive topology"
    elif best_site_score >= 60 and not seed_heavily_reused:
        recommended_entry_point = "site_then_network"
        rationale = "single credible site anchor - start from GDB site, then use network evidence to disambiguate"
    elif (
        (network_hint_counts["vlan_rows"] or 0)
        or (network_hint_counts["iface_rows"] or 0)
        or (network_hint_counts["cpe_rows"] or 0)
    ):
        recommended_entry_point = "network_anchor_first"
        rationale = "site anchor weak - start from network hints (service refs, CPE, interfaces), then reconcile to topology"
    elif spatial:
        recommended_entry_point = "site_spatial_first"
        rationale = "no reliable matched site - start from spatial evidence and nearby GDB sites"
    else:
        recommended_entry_point = "manual_broad_exploration"
        rationale = (
            "no strong anchor - explore LEA, party, spatial and network in parallel"
        )

    site_anchor_quality = (
        "high"
        if has_distinct_pair and best_site_score >= 70 and not seed_heavily_reused
        else "medium"
        if best_site_score >= 50
        and not seed_heavily_reused
        and seed_discriminance >= 30
        else "low"
    )

    return {
        "service_id": service_id,
        "nature_service": service["nature_service"],
        "principal_client": service["principal_client"],
        "client_final": service["client_final"],
        "endpoint_a_raw": service["endpoint_a_raw"],
        "endpoint_z_raw": service["endpoint_z_raw"],
        "route_refs_in_lea": route_refs,
        "service_refs_in_lea": _extract_service_refs(
            service["service_refs_json"], service["principal_external_ref"]
        ),
        "endpoints": _rows_to_dicts(endpoints),
        "spatial_seeds": _rows_to_dicts(seeds),
        "spatial_evidence": _rows_to_dicts(spatial),
        "best_spatial_seed": best_seed,
        "anchor_geometry": _anchor_geom_from_site_anchor(
            {
                "endpoints": _rows_to_dicts(endpoints),
                "site_assets": site_assets,
                "best_spatial_seed": best_seed,
            }
        ),
        "site_assets": site_assets,
        "network_hint_counts": dict(network_hint_counts) if network_hint_counts else {},
        "device_prefix_hints": device_prefixes,
        "site_anchor_quality": site_anchor_quality,
        "recommended_entry_point": recommended_entry_point,
        "recommended_rationale": rationale,
    }


def _hunt_vlan(con: sqlite3.Connection, service_id: str) -> dict[str, Any]:
    """Chasse au VLAN pour un service L2L - retourne des hypotheses avec preuves."""
    svc = con.execute(
        """SELECT s.service_id, s.nature_service, s.principal_client, s.client_final,
                  s.principal_external_ref, s.service_refs_json,
                  s.endpoint_a_raw, s.endpoint_z_raw,
                  e_a.matched_site_id AS site_a_id, e_a.matched_site_name AS site_a_name,
                  e_z.matched_site_id AS site_z_id, e_z.matched_site_name AS site_z_name
           FROM service_master_active s
           LEFT JOIN service_endpoint e_a ON e_a.service_id = s.service_id AND e_a.endpoint_label = 'A'
           LEFT JOIN service_endpoint e_z ON e_z.service_id = s.service_id AND e_z.endpoint_label = 'Z'
           WHERE s.service_id = ?""",
        (service_id,),
    ).fetchone()
    if not svc:
        return {"error": f"service_id '{service_id}' not found"}

    hypotheses: list[dict[str, Any]] = []
    hypothesis_index: dict[tuple[str, str, str], dict[str, Any]] = {}
    data_explored: list[str] = []
    service_refs = _extract_service_refs(
        svc["service_refs_json"], svc["principal_external_ref"]
    )
    client_tokens = _extract_client_tokens(
        svc["client_final"],
        svc["principal_client"],
    )
    location_tokens = {part for label in _DEVICE_PREFIX_HINTS for part in label.split()}
    client_tokens = [token for token in client_tokens if token not in location_tokens]
    if not client_tokens:
        client_tokens = [
            token
            for token in _extract_client_tokens(svc["endpoint_z_raw"])
            if token not in location_tokens
        ]
    device_prefixes = _infer_device_prefixes(
        svc["site_a_name"],
        svc["site_z_name"],
        svc["endpoint_a_raw"],
        svc["endpoint_z_raw"],
    )
    if service_refs:
        data_explored.append("service refs LEA -> interfaces/SWAG")

    # --- Source 1 : ancre TOIP via xconnect_circuit_id [strong] ---
    # route_refs_json contient "TOIP 2079" -> xconnect_circuit_id stocke "2079"
    data_explored.append(
        "service_lea_signal.route_refs_json -> ref_co_subinterface.xconnect_circuit_id"
    )
    toip_refs: list[str] = []
    for sig in con.execute(
        "SELECT route_refs_json FROM service_lea_signal WHERE service_id = ? AND route_refs_json IS NOT NULL",
        (service_id,),
    ).fetchall():
        try:
            for ref in json.loads(sig["route_refs_json"]):
                if ref:
                    toip_refs.append(ref.strip())
        except (json.JSONDecodeError, TypeError):
            pass

    for raw_ref in toip_refs:
        numeric = raw_ref.upper().removeprefix("TOIP ").strip()
        for cid in {numeric, raw_ref}:
            for r in con.execute(
                "SELECT cs.subif_id, cs.device_name, cs.interface_name, cs.vlan_id, "
                "cs.description, cs.site_code, cs.xconnect_circuit_id "
                "FROM ref_co_subinterface cs WHERE cs.xconnect_circuit_id = ?",
                (cid,),
            ).fetchall():
                vlan_key = str(r["vlan_id"])
                vlan_rows = _lookup_vlan_rows(
                    con,
                    vlan_key,
                    device_name=r["device_name"],
                    prefixes=device_prefixes,
                )
                if not vlan_rows:
                    _add_vlan_hypothesis(
                        hypotheses,
                        hypothesis_index,
                        candidate_vlan_id=vlan_key,
                        candidate_device=r["device_name"],
                        source="toip_xconnect",
                        network_vlan_id=None,
                        evidence_for=[
                            f"xconnect_circuit_id={r['xconnect_circuit_id']} sur {r['device_name']} {r['interface_name']}",
                            f"LEA route_ref: {raw_ref}",
                        ]
                        + ([f"site_code={r['site_code']}"] if r["site_code"] else []),
                        evidence_against=[
                            f"label client absent dans ref_network_vlans pour vlan_id={vlan_key}"
                        ],
                        proof_level="medium",
                        extra={
                            "co_interface": r["interface_name"],
                            "site_code": r["site_code"],
                            "sw_label": None,
                        },
                    )
                    continue
                for vlan_row in vlan_rows:
                    _add_vlan_hypothesis(
                        hypotheses,
                        hypothesis_index,
                        candidate_vlan_id=vlan_key,
                        candidate_device=vlan_row.get("device_name")
                        or r["device_name"],
                        source="toip_xconnect",
                        network_vlan_id=vlan_row.get("network_vlan_id"),
                        evidence_for=[
                            f"xconnect_circuit_id={r['xconnect_circuit_id']} sur {r['device_name']} {r['interface_name']}",
                            f"LEA route_ref: {raw_ref}",
                            f"vlan_id={vlan_key} retrouve sur {vlan_row.get('device_name')}",
                        ]
                        + ([f"site_code={r['site_code']}"] if r["site_code"] else []),
                        evidence_against=[]
                        if vlan_row.get("label")
                        else [
                            f"label client absent sur {vlan_row.get('device_name')} vlan_id={vlan_key}"
                        ],
                        proof_level="strong" if vlan_row.get("label") else "medium",
                        extra={
                            "co_interface": r["interface_name"],
                            "site_code": r["site_code"],
                            "sw_label": vlan_row.get("label"),
                        },
                    )

    # --- Source 2 : interfaces / SWAG via ref service exact [strong/medium] ---
    if service_refs:
        for service_ref in service_refs[:6]:
            for row in con.execute(
                "SELECT device_name, interface_name, description, vlan_ids_json "
                "FROM ref_network_interfaces WHERE service_refs_json LIKE ? OR UPPER(description) LIKE ? LIMIT 12",
                (f"%{service_ref}%", f"%{service_ref}%"),
            ).fetchall():
                vlan_ids = _extract_vlan_ids(row["vlan_ids_json"], row["description"])
                for vlan_key in vlan_ids[:4]:
                    vlan_rows = _lookup_vlan_rows(
                        con,
                        vlan_key,
                        device_name=row["device_name"],
                        prefixes=device_prefixes,
                    )
                    if vlan_rows:
                        for vlan_row in vlan_rows:
                            _add_vlan_hypothesis(
                                hypotheses,
                                hypothesis_index,
                                candidate_vlan_id=vlan_key,
                                candidate_device=vlan_row.get("device_name")
                                or row["device_name"],
                                source="service_ref_interface",
                                network_vlan_id=vlan_row.get("network_vlan_id"),
                                evidence_for=[
                                    f"description interface contient {service_ref}",
                                    f"interface {row['device_name']} {row['interface_name']}",
                                ],
                                evidence_against=[]
                                if vlan_row.get("label")
                                else ["VLAN retrouve sans label client explicite"],
                                proof_level="strong"
                                if vlan_row.get("label")
                                else "medium",
                                extra={
                                    "interface_name": row["interface_name"],
                                    "interface_description": row["description"],
                                    "sw_label": vlan_row.get("label"),
                                },
                            )
                    else:
                        _add_vlan_hypothesis(
                            hypotheses,
                            hypothesis_index,
                            candidate_vlan_id=vlan_key,
                            candidate_device=row["device_name"],
                            source="service_ref_interface",
                            network_vlan_id=None,
                            evidence_for=[
                                f"description interface contient {service_ref}",
                                f"interface {row['device_name']} {row['interface_name']}",
                            ],
                            evidence_against=[
                                "aucun network_vlan_id structure retrouve pour ce vlan_id"
                            ],
                            proof_level="medium",
                            extra={
                                "interface_name": row["interface_name"],
                                "interface_description": row["description"],
                            },
                        )

            for row in con.execute(
                "SELECT hostname, interface_name, description FROM ref_swag_interfaces "
                "WHERE service_refs_json LIKE ? OR UPPER(description) LIKE ? LIMIT 12",
                (f"%{service_ref}%", f"%{service_ref}%"),
            ).fetchall():
                vlan_ids = _extract_vlan_ids(row["description"])
                for vlan_key in vlan_ids[:4]:
                    vlan_rows = _lookup_vlan_rows(
                        con,
                        vlan_key,
                        device_name=row["hostname"],
                        prefixes=device_prefixes
                        + _infer_device_prefixes(row["hostname"]),
                    )
                    _add_vlan_hypothesis(
                        hypotheses,
                        hypothesis_index,
                        candidate_vlan_id=vlan_key,
                        candidate_device=(
                            vlan_rows[0].get("device_name")
                            if vlan_rows
                            else row["hostname"]
                        ),
                        source="service_ref_swag",
                        network_vlan_id=(
                            vlan_rows[0].get("network_vlan_id") if vlan_rows else None
                        ),
                        evidence_for=[
                            f"description SWAG contient {service_ref}",
                            f"interface {row['hostname']} {row['interface_name']}",
                        ],
                        evidence_against=[]
                        if vlan_rows
                        else [
                            "aucun network_vlan_id structure retrouve pour ce vlan_id"
                        ],
                        proof_level="strong" if vlan_rows else "medium",
                        extra={
                            "interface_name": row["interface_name"],
                            "interface_description": row["description"],
                            "sw_label": vlan_rows[0].get("label")
                            if vlan_rows
                            else None,
                        },
                    )

    # --- Source 3 : labels client dans ref_network_vlans [medium] ---
    data_explored.append("ref_network_vlans (normalized_label LIKE client tokens)")
    for tok in client_tokens[:5]:
        for r in con.execute(
            "SELECT network_vlan_id, device_name, vlan_id, label, route_refs_json "
            "FROM ref_network_vlans WHERE UPPER(normalized_label) LIKE ? LIMIT 10",
            (f"%{tok}%",),
        ).fetchall():
            ev_for = [f"label '{r['label']}' contient token client '{tok}'"]
            ev_against: list[str] = []
            # Check if VREG_ (pool, not assigned)
            if (r["label"] or "").upper().startswith("VREG"):
                ev_against.append(f"label VREG_ = ressource pool non nominalisee")
            _add_vlan_hypothesis(
                hypotheses,
                hypothesis_index,
                candidate_vlan_id=str(r["vlan_id"]),
                candidate_device=r["device_name"],
                source="label_match",
                network_vlan_id=r["network_vlan_id"],
                evidence_for=ev_for,
                evidence_against=ev_against,
                proof_level="weak" if ev_against else "medium",
                extra={"label": r["label"]},
            )

    # --- Source 4 : descriptions interfaces / SWAG par tokens client [medium] ---
    data_explored.append(
        "ref_network_interfaces/ref_swag_interfaces (description LIKE client tokens)"
    )
    for tok in client_tokens[:4]:
        for row in con.execute(
            "SELECT device_name, interface_name, description, vlan_ids_json FROM ref_network_interfaces "
            "WHERE UPPER(description) LIKE ? LIMIT 10",
            (f"%{tok}%",),
        ).fetchall():
            for vlan_key in _extract_vlan_ids(row["vlan_ids_json"], row["description"])[
                :4
            ]:
                vlan_rows = _lookup_vlan_rows(
                    con,
                    vlan_key,
                    device_name=row["device_name"],
                    prefixes=device_prefixes
                    + _infer_device_prefixes(row["device_name"]),
                )
                _add_vlan_hypothesis(
                    hypotheses,
                    hypothesis_index,
                    candidate_vlan_id=vlan_key,
                    candidate_device=(
                        vlan_rows[0].get("device_name")
                        if vlan_rows
                        else row["device_name"]
                    ),
                    source="client_token_interface",
                    network_vlan_id=(
                        vlan_rows[0].get("network_vlan_id") if vlan_rows else None
                    ),
                    evidence_for=[
                        f"description interface contient token client '{tok}'",
                        f"interface {row['device_name']} {row['interface_name']}",
                    ],
                    evidence_against=[]
                    if vlan_rows
                    else ["vlan_id extrait de la description seulement"],
                    proof_level="medium" if vlan_rows else "weak",
                    extra={
                        "interface_name": row["interface_name"],
                        "interface_description": row["description"],
                        "sw_label": vlan_rows[0].get("label") if vlan_rows else None,
                    },
                )

        for row in con.execute(
            "SELECT hostname, interface_name, description FROM ref_swag_interfaces "
            "WHERE UPPER(description) LIKE ? LIMIT 10",
            (f"%{tok}%",),
        ).fetchall():
            for vlan_key in _extract_vlan_ids(row["description"])[:4]:
                vlan_rows = _lookup_vlan_rows(
                    con,
                    vlan_key,
                    device_name=row["hostname"],
                    prefixes=device_prefixes + _infer_device_prefixes(row["hostname"]),
                )
                _add_vlan_hypothesis(
                    hypotheses,
                    hypothesis_index,
                    candidate_vlan_id=vlan_key,
                    candidate_device=(
                        vlan_rows[0].get("device_name")
                        if vlan_rows
                        else row["hostname"]
                    ),
                    source="client_token_swag",
                    network_vlan_id=(
                        vlan_rows[0].get("network_vlan_id") if vlan_rows else None
                    ),
                    evidence_for=[
                        f"description SWAG contient token client '{tok}'",
                        f"interface {row['hostname']} {row['interface_name']}",
                    ],
                    evidence_against=[]
                    if vlan_rows
                    else ["vlan_id extrait de la description SWAG seulement"],
                    proof_level="medium" if vlan_rows else "weak",
                    extra={
                        "interface_name": row["interface_name"],
                        "interface_description": row["description"],
                        "sw_label": vlan_rows[0].get("label") if vlan_rows else None,
                    },
                )

    # --- Source 5 : sub-interfaces CO du site_a / site_z (contexte par prefixes) ---
    co_context: dict[str, Any] = {}
    co_prefix: str | None = None
    for prefix in device_prefixes:
        if prefix in _CO_PREFIX_MAP.values():
            co_prefix = prefix
            break

    if co_prefix:
        data_explored.append(
            f"ref_co_subinterface WHERE device_name LIKE '{co_prefix}-CO-%'"
        )
        active_co = con.execute(
            "SELECT subif_id, device_name, interface_name, vlan_id, description, "
            "site_code, xconnect_circuit_id "
            "FROM ref_co_subinterface "
            "WHERE device_name LIKE ? AND xconnect_circuit_id IS NOT NULL "
            "ORDER BY vlan_id LIMIT 50",
            (f"{co_prefix}-CO-%",),
        ).fetchall()
        co_context = {
            "co_prefix": co_prefix,
            "active_circuits_count": len(active_co),
            "sample": [_row_to_dict(r) for r in active_co[:10]],
        }
        # Cross-ref: VLANs from source 2 that appear in CO context
        active_vlans = {str(r["vlan_id"]) for r in active_co}
        for hyp in hypotheses:
            if hyp["candidate_vlan_id"] in active_vlans:
                hyp["evidence_for"].append(
                    f"vlan_id confirme actif sur {co_prefix}-CO (xconnect present)"
                )
                if _VLAN_LEVEL_ORDER.get("medium", 99) < _VLAN_LEVEL_ORDER.get(
                    hyp.get("proof_level", "weak"), 99
                ):
                    hyp["proof_level"] = "medium"

    # --- Source 6 : CPE hostname -> port access -> VLAN [strong si CPE+port+vlan] ---
    data_explored.append("ref_cpe_inventory + service_support_reseau.cpe_id")
    cpe_rows = con.execute(
        """SELECT cpe_id, hostname FROM ref_cpe_inventory
           WHERE cpe_id IN (
               SELECT cpe_id FROM service_support_reseau WHERE service_id = ? AND cpe_id IS NOT NULL
           )""",
        (service_id,),
    ).fetchall()
    if cpe_rows:
        for cpe in cpe_rows:
            co_context.setdefault("cpe_hints", []).append(
                {
                    "cpe_id": cpe["cpe_id"],
                    "hostname": cpe["hostname"],
                }
            )
            for row in con.execute(
                "SELECT device_name, interface_name, description, vlan_ids_json FROM ref_network_interfaces "
                "WHERE UPPER(description) LIKE ? LIMIT 8",
                (f"%{normalize_alias(cpe['hostname'])[:24]}%",),
            ).fetchall():
                for vlan_key in _extract_vlan_ids(
                    row["vlan_ids_json"], row["description"]
                )[:3]:
                    vlan_rows = _lookup_vlan_rows(
                        con,
                        vlan_key,
                        device_name=row["device_name"],
                        prefixes=device_prefixes,
                    )
                    _add_vlan_hypothesis(
                        hypotheses,
                        hypothesis_index,
                        candidate_vlan_id=vlan_key,
                        candidate_device=(
                            vlan_rows[0].get("device_name")
                            if vlan_rows
                            else row["device_name"]
                        ),
                        source="cpe_hint",
                        network_vlan_id=(
                            vlan_rows[0].get("network_vlan_id") if vlan_rows else None
                        ),
                        evidence_for=[
                            f"CPE hint {cpe['hostname']}",
                            f"interface {row['device_name']} {row['interface_name']} suspecte",
                        ],
                        evidence_against=[]
                        if vlan_rows
                        else ["pas de network_vlan_id structure confirme"],
                        proof_level="medium" if vlan_rows else "weak",
                        extra={
                            "interface_name": row["interface_name"],
                            "interface_description": row["description"],
                        },
                    )

    # Sort hypotheses: strong first
    hypotheses.sort(
        key=lambda h: _VLAN_LEVEL_ORDER.get(h.get("proof_level", "weak"), 2)
    )

    ambiguity_groups: list[dict[str, Any]] = []
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for hyp in hypotheses:
        label = _norm_token_text(hyp.get("label") or hyp.get("sw_label") or "")
        if not label and not hyp.get("network_vlan_id"):
            continue
        key = (str(hyp.get("candidate_device") or ""), label)
        grouped.setdefault(key, []).append(hyp)
    for (device_name, label), items in grouped.items():
        vlan_ids = [
            str(item.get("candidate_vlan_id") or "")
            for item in items
            if item.get("candidate_vlan_id")
        ]
        distinct_vlans = sorted({vlan_id for vlan_id in vlan_ids if vlan_id})
        if len(distinct_vlans) <= 1:
            continue
        proof_levels = {str(item.get("proof_level") or "weak") for item in items}
        ambiguity_groups.append(
            {
                "device_name": device_name,
                "label_hint": label,
                "candidate_vlan_ids": distinct_vlans,
                "proof_levels": sorted(proof_levels),
            }
        )
        for item in items:
            item["disambiguation_needed"] = True
            item["ambiguity_group"] = distinct_vlans
            item.setdefault("evidence_against", []).append(
                f"plusieurs VLANs concurrents sur {device_name or 'device inconnu'}: {', '.join(distinct_vlans)}"
            )
            if item.get("proof_level") == "strong":
                item["proof_level"] = "medium"

    return {
        "service_id": service_id,
        "nature_service": svc["nature_service"],
        "site_a": svc["site_a_name"],
        "site_z": svc["site_z_name"],
        "co_prefix_used": co_prefix,
        "hypotheses": hypotheses,
        "ambiguity_groups": ambiguity_groups,
        "co_context": co_context,
        "data_explored": data_explored,
        "summary": (
            f"{len(hypotheses)} hypothese(s) VLAN : "
            f"{sum(1 for h in hypotheses if h['proof_level'] == 'strong')} strong, "
            f"{sum(1 for h in hypotheses if h['proof_level'] == 'medium')} medium, "
            f"{sum(1 for h in hypotheses if h['proof_level'] == 'weak')} weak"
            if hypotheses
            else "Aucune hypothese VLAN trouvee"
        ),
    }


def _resolve_passive_chain(
    con: sqlite3.Connection, service_id: str, site_id: str, depth: int = 2
) -> dict[str, Any]:
    site_id = (site_id or "").strip()
    if not site_id:
        return {"error": "site_id manquant"}

    site_asset = None
    if _table_or_view_exists(con, "v_site_passive_assets"):
        site_asset = _row_to_dict(
            con.execute(
                "SELECT * FROM v_site_passive_assets WHERE site_id = ?",
                (site_id,),
            ).fetchone()
        )
    if site_asset is None:
        site_asset = _row_to_dict(
            con.execute(
                "SELECT site_id, reference, geom_x, geom_y FROM ref_sites WHERE site_id = ?",
                (site_id,),
            ).fetchone()
        )

    housings = _rows_to_dicts(
        con.execute(
            "SELECT housing_id, migration_oid, housing_type, reference, description, site_id, site_name "
            "FROM ref_optical_housing WHERE site_id = ? ORDER BY reference LIMIT 20",
            (site_id,),
        ).fetchall()
    )
    housing_migoids = [
        row["migration_oid"] for row in housings if row.get("migration_oid")
    ]

    connections: list[dict[str, Any]] = []
    cable_migoids: set[str] = set()
    if housing_migoids and _table_or_view_exists(con, "ref_optical_connection"):
        placeholders = ",".join("?" for _ in housing_migoids)
        connection_rows = con.execute(
            f"SELECT connection_id, housing_migration_oid, obj1_type, obj1_migration_oid, obj2_type, obj2_migration_oid "
            f"FROM ref_optical_connection WHERE housing_migration_oid IN ({placeholders}) LIMIT 60",
            tuple(housing_migoids),
        ).fetchall()
        for row in connection_rows:
            row_dict = _row_to_dict(row) or {}
            connections.append(row_dict)
            for key_type, key_oid in (
                ("obj1_type", "obj1_migration_oid"),
                ("obj2_type", "obj2_migration_oid"),
            ):
                obj_type = str(row_dict.get(key_type) or "").upper()
                obj_oid = str(row_dict.get(key_oid) or "").strip()
                if obj_oid and "CABLE" in obj_type:
                    cable_migoids.add(obj_oid)

    cables: list[dict[str, Any]] = []
    if cable_migoids:
        placeholders = ",".join("?" for _ in cable_migoids)
        cables = _rows_to_dicts(
            con.execute(
                f"SELECT cable_id, migration_oid, reference, userreference, comments, site_tokens_json, geom_centroid_x, geom_centroid_y "
                f"FROM ref_optical_cable WHERE migration_oid IN ({placeholders}) LIMIT 40",
                tuple(cable_migoids),
            ).fetchall()
        )
    if (
        not cables
        and site_asset
        and site_asset.get("geom_x") is not None
        and site_asset.get("geom_y") is not None
    ):
        x = float(site_asset["geom_x"])
        y = float(site_asset["geom_y"])
        for cable in con.execute(
            "SELECT cable_id, migration_oid, reference, userreference, comments, site_tokens_json, geom_start_x, geom_start_y, geom_end_x, geom_end_y, geom_centroid_x, geom_centroid_y "
            "FROM ref_optical_cable "
            "WHERE (geom_centroid_x BETWEEN ? AND ? AND geom_centroid_y BETWEEN ? AND ?) "
            "   OR (geom_start_x BETWEEN ? AND ? AND geom_start_y BETWEEN ? AND ?) "
            "   OR (geom_end_x BETWEEN ? AND ? AND geom_end_y BETWEEN ? AND ?) "
            "LIMIT 20",
            (
                x - 300,
                x + 300,
                y - 300,
                y + 300,
                x - 300,
                x + 300,
                y - 300,
                y + 300,
                x - 300,
                x + 300,
                y - 300,
                y + 300,
            ),
        ).fetchall():
            cable_dict = _row_to_dict(cable) or {}
            best_distance = None
            for key_x, key_y in (
                ("geom_start_x", "geom_start_y"),
                ("geom_end_x", "geom_end_y"),
                ("geom_centroid_x", "geom_centroid_y"),
            ):
                cx = cable_dict.get(key_x)
                cy = cable_dict.get(key_y)
                if cx is None or cy is None:
                    continue
                distance = _distance_sq(x, y, float(cx), float(cy)) ** 0.5
                if best_distance is None or distance < best_distance:
                    best_distance = distance
            if best_distance is not None and best_distance <= 300:
                cable_dict["distance_meters"] = round(best_distance, 1)
                cables.append(cable_dict)

    linked_leases: list[dict[str, Any]] = []
    if _table_or_view_exists(con, "ref_optical_lease_endpoint"):
        linked_leases = _rows_to_dicts(
            con.execute(
                "SELECT l.optical_lease_id, l.ref_exploit, l.reference, l.lease_kind, l.client, l.lessee, l.comments, "
                "ep.endpoint_label, ep.site_name "
                "FROM ref_optical_lease l "
                "JOIN ref_optical_lease_endpoint ep ON ep.optical_lease_id = l.optical_lease_id "
                "WHERE ep.site_id = ? ORDER BY l.ref_exploit LIMIT 20",
                (site_id,),
            ).fetchall()
        )

    nearby_route_endpoints: list[dict[str, Any]] = []
    route_refs = {
        (row.get("ref_exploit") or "").strip()
        for row in linked_leases
        if (row.get("ref_exploit") or "").strip()
    }
    if route_refs and _table_or_view_exists(con, "v_route_endpoint_sites"):
        placeholders = ",".join("?" for _ in route_refs)
        nearby_route_endpoints = _rows_to_dicts(
            con.execute(
                f"SELECT route_ref, route_id, step_type, site_label, bpe, cable_in, cable_out "
                f"FROM v_route_endpoint_sites WHERE route_ref IN ({placeholders}) LIMIT 20",
                tuple(route_refs),
            ).fetchall()
        )

    return {
        "service_id": service_id,
        "site_id": site_id,
        "depth": depth,
        "site_asset": site_asset,
        "housings": housings[: max(5, depth * 5)],
        "connections": connections[: max(10, depth * 10)],
        "cables": cables[: max(10, depth * 10)],
        "linked_leases": linked_leases[: max(10, depth * 10)],
        "route_endpoints": nearby_route_endpoints[: max(10, depth * 10)],
    }


def _resolve_cable_spatial(
    con: sqlite3.Connection, service_id: str, radius_meters: int = 300
) -> dict[str, Any]:
    anchor = _build_site_anchor(con, service_id)
    anchor_geom = anchor.get("anchor_geometry") or {}
    best_seed = anchor.get("best_spatial_seed") or {}
    x_l93 = anchor_geom.get("x_l93")
    y_l93 = anchor_geom.get("y_l93")
    if x_l93 is None or y_l93 is None:
        return {
            "service_id": service_id,
            "radius_meters": radius_meters,
            "anchor_geometry": anchor_geom or None,
            "best_spatial_seed": best_seed or None,
            "cable_candidates": [],
            "summary": "Aucun seed spatial exploitable pour chercher des cables proches.",
        }

    query = (
        "SELECT cable_id, reference, userreference, site_tokens_json, geom_start_x, geom_start_y, geom_end_x, geom_end_y, geom_centroid_x, geom_centroid_y, "
        "MIN("
        "COALESCE(((geom_centroid_x - ?) * (geom_centroid_x - ?) + (geom_centroid_y - ?) * (geom_centroid_y - ?)), 9e18),"
        "COALESCE(((geom_start_x - ?) * (geom_start_x - ?) + (geom_start_y - ?) * (geom_start_y - ?)), 9e18),"
        "COALESCE(((geom_end_x - ?) * (geom_end_x - ?) + (geom_end_y - ?) * (geom_end_y - ?)), 9e18)"
        ") AS distance_sq "
        "FROM ref_optical_cable "
        "WHERE (geom_centroid_x BETWEEN ? AND ? AND geom_centroid_y BETWEEN ? AND ?) "
        "   OR (geom_start_x BETWEEN ? AND ? AND geom_start_y BETWEEN ? AND ?) "
        "   OR (geom_end_x BETWEEN ? AND ? AND geom_end_y BETWEEN ? AND ?) "
        "ORDER BY distance_sq ASC LIMIT 20"
    )
    rows = []
    for row in con.execute(
        query,
        (
            x_l93,
            x_l93,
            y_l93,
            y_l93,
            x_l93,
            x_l93,
            y_l93,
            y_l93,
            x_l93,
            x_l93,
            y_l93,
            y_l93,
            x_l93 - radius_meters,
            x_l93 + radius_meters,
            y_l93 - radius_meters,
            y_l93 + radius_meters,
            x_l93 - radius_meters,
            x_l93 + radius_meters,
            y_l93 - radius_meters,
            y_l93 + radius_meters,
            x_l93 - radius_meters,
            x_l93 + radius_meters,
            y_l93 - radius_meters,
            y_l93 + radius_meters,
        ),
    ).fetchall():
        row_dict = _row_to_dict(row) or {}
        distance_sq = float(row_dict.pop("distance_sq") or 0.0)
        if distance_sq <= float(radius_meters * radius_meters):
            row_dict["distance_meters"] = round(distance_sq**0.5, 1)
            rows.append(row_dict)
    return {
        "service_id": service_id,
        "radius_meters": radius_meters,
        "anchor_geometry": anchor_geom,
        "best_spatial_seed": best_seed,
        "cable_candidates": rows,
        "summary": f"{len(rows)} cable(s) a moins de {radius_meters}m de l'ancre geometrique {anchor_geom.get('source', 'unknown')}.",
    }


def _build_route_topology_bundle(
    con: sqlite3.Connection, route_ref: str, *, limit_steps: int = 60
) -> dict[str, Any]:
    route_ref = (route_ref or "").strip()
    if not route_ref:
        return {"error": "route_ref manquant"}

    route_row = con.execute(
        "SELECT route_id, route_ref, network, client, lessee, status FROM ref_routes WHERE route_ref = ? OR route_id = ? LIMIT 1",
        (route_ref, route_ref),
    ).fetchone()
    if route_row is None:
        route_row = con.execute(
            "SELECT route_id, route_ref, network, client, lessee, status FROM ref_routes WHERE route_ref LIKE ? LIMIT 1",
            (f"%{route_ref}%",),
        ).fetchone()
    route = _row_to_dict(route_row) or {"route_ref": route_ref, "route_id": None}
    effective_route_ref = str(route.get("route_ref") or route_ref)
    effective_route_id = route.get("route_id")

    parcours = _rows_to_dicts(
        con.execute(
            "SELECT route_ref, route_id, step_no, step_type, site, site_detail, address, bpe, cable_in, cable_out, commentaire "
            "FROM ref_route_parcours WHERE route_ref = ? OR route_id = ? ORDER BY step_no LIMIT ?",
            (effective_route_ref, effective_route_id, limit_steps),
        ).fetchall()
    )
    endpoints = []
    if _table_or_view_exists(con, "v_route_endpoint_sites"):
        endpoints = _rows_to_dicts(
            con.execute(
                "SELECT route_ref, route_id, step_no, step_type, site_label, site_detail, bpe, cable_in, cable_out, commentaire "
                "FROM v_route_endpoint_sites WHERE route_ref = ? OR route_id = ? ORDER BY step_no",
                (effective_route_ref, effective_route_id),
            ).fetchall()
        )

    cable_refs: list[str] = []
    bpe_refs: list[str] = []
    site_labels: list[str] = []
    seen_cables: set[str] = set()
    seen_bpes: set[str] = set()
    seen_sites: set[str] = set()
    for row in parcours:
        for key in ("cable_in", "cable_out"):
            ref = str(row.get(key) or "").strip()
            if ref and ref not in seen_cables:
                seen_cables.add(ref)
                cable_refs.append(ref)
        bpe = str(row.get("bpe") or "").strip()
        if bpe and bpe not in seen_bpes:
            seen_bpes.add(bpe)
            bpe_refs.append(bpe)
        site = str(row.get("site") or "").strip()
        if site and site not in seen_sites:
            seen_sites.add(site)
            site_labels.append(site)

    route_leases = _rows_to_dicts(
        con.execute(
            "SELECT optical_lease_id, ref_exploit, reference, lease_kind, client, lessee, comments "
            "FROM ref_optical_lease WHERE ref_exploit = ? OR ref_exploit LIKE ? LIMIT 20",
            (effective_route_ref, f"%{effective_route_ref}%"),
        ).fetchall()
    )

    route_cables: list[dict[str, Any]] = []
    if cable_refs:
        placeholders = ",".join("?" for _ in cable_refs)
        route_cables = _rows_to_dicts(
            con.execute(
                f"SELECT cable_id, reference, userreference, site_tokens_json, geom_start_x, geom_start_y, geom_end_x, geom_end_y, geom_centroid_x, geom_centroid_y "
                f"FROM ref_optical_cable WHERE reference IN ({placeholders}) OR userreference IN ({placeholders}) LIMIT 40",
                tuple(cable_refs) + tuple(cable_refs),
            ).fetchall()
        )

    footprint = None
    xs: list[float] = []
    ys: list[float] = []
    for cable in route_cables:
        for key_x, key_y in (
            ("geom_start_x", "geom_start_y"),
            ("geom_end_x", "geom_end_y"),
            ("geom_centroid_x", "geom_centroid_y"),
        ):
            x = cable.get(key_x)
            y = cable.get(key_y)
            if x is not None and y is not None:
                xs.append(float(x))
                ys.append(float(y))
    if xs and ys:
        footprint = {
            "min_x": min(xs),
            "max_x": max(xs),
            "min_y": min(ys),
            "max_y": max(ys),
            "centroid_x": round(sum(xs) / len(xs), 1),
            "centroid_y": round(sum(ys) / len(ys), 1),
        }

    return {
        "route": route,
        "route_ref": effective_route_ref,
        "route_id": effective_route_id,
        "endpoints": endpoints,
        "parcours": parcours,
        "site_labels": site_labels,
        "bpe_refs": bpe_refs,
        "cable_refs": cable_refs,
        "cables": route_cables,
        "leases": route_leases,
        "footprint": footprint,
    }


def _route_service_coherence(
    con: sqlite3.Connection,
    service_id: str,
    route_ref: str,
    *,
    anchor: dict[str, Any] | None = None,
    route_bundle: dict[str, Any] | None = None,
) -> dict[str, Any]:
    anchor = anchor or _build_site_anchor(con, service_id)
    route_bundle = route_bundle or _build_route_topology_bundle(con, route_ref)
    if route_bundle.get("error"):
        return {
            "route_ref": route_ref,
            "coherence_level": "weak",
            "score": 0,
            "positive_signals": [],
            "negative_signals": [route_bundle["error"]],
        }

    endpoint_rows = anchor.get("endpoints") or []
    site_a_id = next(
        (
            row.get("matched_site_id")
            for row in endpoint_rows
            if row.get("endpoint_label") == "A"
        ),
        None,
    )
    site_z_id = next(
        (
            row.get("matched_site_id")
            for row in endpoint_rows
            if row.get("endpoint_label") == "Z"
        ),
        None,
    )
    site_tokens = _site_label_tokens(
        anchor.get("endpoint_a_raw"),
        anchor.get("endpoint_z_raw"),
        *(row.get("matched_site_name") for row in endpoint_rows),
    )

    score = 0
    positive: list[str] = []
    negative: list[str] = []

    route_labels = " ".join(route_bundle.get("site_labels") or [])
    route_norm = _norm_token_text(route_labels)
    matched_tokens = [token for token in site_tokens if token in route_norm]
    if matched_tokens:
        score += min(20, 8 * len(matched_tokens))
        positive.append(
            f"parcours contient tokens site {', '.join(matched_tokens[:4])}"
        )
    else:
        negative.append("aucun token site du service retrouve dans le parcours")

    lease_sites: set[str] = set()
    if _table_or_view_exists(con, "ref_optical_lease_endpoint"):
        for row in con.execute(
            "SELECT DISTINCT site_id FROM ref_optical_lease_endpoint WHERE optical_lease_id IN (SELECT optical_lease_id FROM ref_optical_lease WHERE ref_exploit = ? OR ref_exploit LIKE ?)",
            (route_bundle["route_ref"], f"%{route_bundle['route_ref']}%"),
        ).fetchall():
            if row["site_id"]:
                lease_sites.add(str(row["site_id"]))
    if site_z_id and site_z_id in lease_sites:
        score += 35
        positive.append("lease endpoint confirme le site Z")
    if site_a_id and site_a_id in lease_sites:
        score += 25
        positive.append("lease endpoint confirme le site A")

    anchor_geom = anchor.get("anchor_geometry") or {}
    x = anchor_geom.get("x_l93")
    y = anchor_geom.get("y_l93")
    best_distance = None
    for cable in route_bundle.get("cables") or []:
        for key_x, key_y in (
            ("geom_start_x", "geom_start_y"),
            ("geom_end_x", "geom_end_y"),
            ("geom_centroid_x", "geom_centroid_y"),
        ):
            if x is None or y is None:
                continue
            cx = cable.get(key_x)
            cy = cable.get(key_y)
            if cx is None or cy is None:
                continue
            dist = _distance_sq(float(x), float(y), float(cx), float(cy)) ** 0.5
            if best_distance is None or dist < best_distance:
                best_distance = dist
    if best_distance is not None:
        if best_distance <= 100:
            score += 25
            positive.append(
                f"cable de la route a {best_distance:.1f}m de l'ancre geometrique"
            )
        elif best_distance <= 300:
            score += 15
            positive.append(
                f"cable de la route a {best_distance:.1f}m de l'ancre geometrique"
            )
        elif best_distance <= 800:
            score += 6
            positive.append(
                f"cable de la route a {best_distance:.1f}m de l'ancre geometrique"
            )
        else:
            negative.append(
                f"cables de la route loin de l'ancre geometrique ({best_distance:.1f}m)"
            )

    if route_bundle.get("endpoints"):
        endpoint_norm = " ".join(
            _norm_token_text(row.get("site_label"))
            for row in route_bundle.get("endpoints") or []
        )
        matched_endpoint_tokens = [
            token for token in site_tokens if token in endpoint_norm
        ]
        if matched_endpoint_tokens:
            score += min(20, 10 * len(matched_endpoint_tokens))
            positive.append(
                f"endpoints de route contiennent {', '.join(matched_endpoint_tokens[:4])}"
            )

    if route_bundle.get("leases"):
        score += min(10, 2 * len(route_bundle.get("leases") or []))
        positive.append("route supportee par au moins une lease en base")

    if score >= 70:
        level = "strong"
    elif score >= 35:
        level = "medium"
    else:
        level = "weak"

    return {
        "route_ref": route_bundle["route_ref"],
        "route_id": route_bundle.get("route_id"),
        "coherence_level": level,
        "score": score,
        "best_distance_meters": round(best_distance, 1)
        if best_distance is not None
        else None,
        "positive_signals": positive,
        "negative_signals": negative,
    }


def _hunt_route_from_site(
    con: sqlite3.Connection, service_id: str, site_z_id: str | None = None
) -> dict[str, Any]:
    anchor = _build_site_anchor(con, service_id)
    endpoint_rows = anchor.get("endpoints") or []
    resolved_site_z = site_z_id or next(
        (
            row.get("matched_site_id")
            for row in endpoint_rows
            if row.get("endpoint_label") == "Z" and row.get("matched_site_id")
        ),
        None,
    )
    resolved_site_a = next(
        (
            row.get("matched_site_id")
            for row in endpoint_rows
            if row.get("endpoint_label") == "A" and row.get("matched_site_id")
        ),
        None,
    )
    if not resolved_site_z:
        return {
            "service_id": service_id,
            "site_z_id": None,
            "direct_evidence": [],
            "context_only": [],
            "passive_chain": None,
            "summary": "Aucun site_z_id resolu pour demarrer la chasse route depuis le site.",
        }

    direct_evidence: list[dict[str, Any]] = []
    context_only: list[dict[str, Any]] = []
    seen_route_refs: set[str] = set()

    if resolved_site_a and _table_or_view_exists(con, "v_lease_site_pair"):
        for row in con.execute(
            "SELECT optical_lease_id, ref_exploit, reference, lease_kind, site_a_name, site_z_name "
            "FROM v_lease_site_pair WHERE (site_a_id = ? AND site_z_id = ?) OR (site_a_id = ? AND site_z_id = ?) "
            "LIMIT 10",
            (resolved_site_a, resolved_site_z, resolved_site_z, resolved_site_a),
        ).fetchall():
            route_ref = (row["ref_exploit"] or "").strip()
            if route_ref and route_ref not in seen_route_refs:
                seen_route_refs.add(route_ref)
                direct_evidence.append(
                    {
                        "candidate_route_ref": route_ref,
                        "candidate_lease_id": row["optical_lease_id"],
                        "proof_level": "strong",
                        "evidence_for": [
                            f"lease {row['optical_lease_id']} relie directement site_a={resolved_site_a} et site_z={resolved_site_z}",
                        ],
                        "evidence_against": [],
                        "source": "lease_site_pair_view",
                    }
                )

    passive_chain = _resolve_passive_chain(con, service_id, resolved_site_z, depth=2)
    for lease in passive_chain.get("linked_leases", []):
        route_ref = str(lease.get("ref_exploit") or "").strip()
        if route_ref and route_ref not in seen_route_refs:
            seen_route_refs.add(route_ref)
            context_only.append(
                {
                    "candidate_route_ref": route_ref,
                    "candidate_lease_id": lease.get("optical_lease_id"),
                    "source": "site_z_lease_presence",
                    "evidence_for": [
                        f"lease {lease.get('optical_lease_id')} touche site_z={resolved_site_z}",
                    ],
                    "evidence_against": [
                        "site Z confirme, mais chainage jusqu'au site A ou au service encore indirect"
                    ],
                    "note": "bonne piste de route depuis le site Z",
                }
            )

    cable_spatial = _resolve_cable_spatial(con, service_id, radius_meters=300)
    if cable_spatial.get("cable_candidates"):
        context_only.append(
            {
                "source": "cable_spatial",
                "service_id": service_id,
                "site_z_id": resolved_site_z,
                "cable_candidates": cable_spatial["cable_candidates"][:5],
                "evidence_for": [cable_spatial["summary"]],
                "evidence_against": ["proximite cable seule - route non prouvee"],
                "note": "utiliser resolve_passive_chain ou query_db pour remonter vers lease/route",
            }
        )

    for bucket in (direct_evidence, context_only):
        for item in bucket:
            route_ref = str(item.get("candidate_route_ref") or "").strip()
            if not route_ref:
                continue
            coherence = _route_service_coherence(
                con,
                service_id,
                route_ref,
                anchor=anchor,
            )
            item["coherence"] = coherence
            item.setdefault("evidence_for", []).extend(
                coherence.get("positive_signals", [])[:3]
            )
            item.setdefault("evidence_against", []).extend(
                coherence.get("negative_signals", [])[:2]
            )

    direct_evidence.sort(
        key=lambda item: (
            -int(item.get("coherence", {}).get("score") or 0),
            item.get("candidate_route_ref") or "",
        )
    )
    context_only.sort(
        key=lambda item: (
            -int(item.get("coherence", {}).get("score") or 0),
            item.get("candidate_route_ref") or "",
        )
    )

    return {
        "service_id": service_id,
        "site_a_id": resolved_site_a,
        "site_z_id": resolved_site_z,
        "anchor": anchor,
        "direct_evidence": direct_evidence,
        "context_only": context_only,
        "passive_chain": passive_chain,
        "spatial_cables": cable_spatial.get("cable_candidates", []),
        "summary": (
            f"{len(direct_evidence)} evidence(s) directe(s), {len(context_only)} contexte(s) depuis site_z={resolved_site_z}."
        ),
    }


def _hunt_route(con: sqlite3.Connection, service_id: str) -> dict[str, Any]:
    """Chasse a la route optique - separe evidence directe vs contexte seul."""
    svc = con.execute(
        """SELECT s.service_id, s.nature_service, s.principal_client, s.client_final,
                  s.principal_external_ref AS ref_external,
                  s.endpoint_a_raw, s.endpoint_z_raw,
                  e_a.matched_site_id AS site_a_id, e_a.matched_site_name AS site_a_name,
                  e_z.matched_site_id AS site_z_id, e_z.matched_site_name AS site_z_name
           FROM service_master_active s
           LEFT JOIN service_endpoint e_a ON e_a.service_id = s.service_id AND e_a.endpoint_label = 'A'
           LEFT JOIN service_endpoint e_z ON e_z.service_id = s.service_id AND e_z.endpoint_label = 'Z'
           WHERE s.service_id = ?""",
        (service_id,),
    ).fetchone()
    if not svc:
        return {"error": f"service_id '{service_id}' not found"}

    direct_evidence: list[dict[str, Any]] = []
    context_only: list[dict[str, Any]] = []
    data_explored: list[str] = []
    seen_routes: set[str] = set()
    anchor = _build_site_anchor(con, service_id)
    client_tokens = _extract_client_tokens(
        svc["client_final"],
        svc["principal_client"],
        svc["site_a_name"],
        svc["site_z_name"],
        svc["endpoint_z_raw"],
    )

    # --- Source 1 : ref_external → ref_optical_lease.ref_exploit [strong] ---
    ref_external = (svc["ref_external"] or "").strip()
    data_explored.append("ref_optical_lease.ref_exploit (match exact sur ref_external)")
    if ref_external:
        for r in con.execute(
            "SELECT optical_lease_id, ref_exploit, lease_kind "
            "FROM ref_optical_lease WHERE ref_exploit = ? LIMIT 5",
            (ref_external,),
        ).fetchall():
            rr = (r["ref_exploit"] or "").strip()
            if rr and rr not in seen_routes:
                seen_routes.add(rr)
                direct_evidence.append(
                    {
                        "candidate_route_ref": rr,
                        "candidate_lease_id": r["optical_lease_id"],
                        "proof_level": "strong",
                        "evidence_for": [
                            f"ref_external '{ref_external}' = ref_exploit dans ref_optical_lease"
                        ],
                        "evidence_against": [],
                        "source": "ref_exploit_exact",
                    }
                )

    # --- Source 2 : route_refs_json (TOIP) -> ref_optical_lease.ref_exploit [strong] ---
    toip_refs: list[str] = []
    for sig in con.execute(
        "SELECT route_refs_json FROM service_lea_signal WHERE service_id = ? AND route_refs_json IS NOT NULL",
        (service_id,),
    ).fetchall():
        try:
            for ref in json.loads(sig["route_refs_json"]):
                if ref:
                    toip_refs.append(ref.strip())
        except (json.JSONDecodeError, TypeError):
            pass

    for raw_ref in toip_refs:
        data_explored.append(f"ref_optical_lease.ref_exploit LIKE '%{raw_ref}%'")
        # Try exact match and LIKE match for flexibility
        for r in con.execute(
            "SELECT optical_lease_id, ref_exploit, lease_kind "
            "FROM ref_optical_lease WHERE ref_exploit LIKE ? LIMIT 5",
            (f"%{raw_ref}%",),
        ).fetchall():
            rr = (r["ref_exploit"] or "").strip()
            if rr and rr not in seen_routes:
                seen_routes.add(rr)
                direct_evidence.append(
                    {
                        "candidate_route_ref": rr,
                        "candidate_lease_id": r["optical_lease_id"],
                        "proof_level": "strong",
                        "evidence_for": [
                            f"LEA route_ref '{raw_ref}' matche ref_exploit '{rr}' dans ref_optical_lease",
                        ],
                        "evidence_against": [],
                        "source": "toip_lease",
                    }
                )

    # --- Source 3 : endpoints A et Z dans ref_optical_lease_endpoint [strong/medium] ---
    has_lease_ep = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ref_optical_lease_endpoint'"
    ).fetchone()
    site_ids = [sid for sid in (svc["site_a_id"], svc["site_z_id"]) if sid]
    if has_lease_ep and site_ids:
        data_explored.append(
            "ref_optical_lease_endpoint WHERE site_id IN (site_a, site_z)"
        )
        lease_ep_rows = con.execute(
            f"""SELECT ole.optical_lease_id, ole.ref_exploit, ole.lease_kind,
                       GROUP_CONCAT(ep.endpoint_label) as endpoints,
                       GROUP_CONCAT(ep.site_id) as site_ids
                FROM ref_optical_lease ole
                JOIN ref_optical_lease_endpoint ep ON ep.optical_lease_id = ole.optical_lease_id
                WHERE ep.site_id IN ({",".join("?" for _ in site_ids)})
                GROUP BY ole.optical_lease_id
                ORDER BY ole.ref_exploit
                LIMIT 10""",
            site_ids,
        ).fetchall()
        for r in lease_ep_rows:
            rr = (r["ref_exploit"] or "").strip()
            if not rr or rr in seen_routes:
                continue
            seen_routes.add(rr)
            found_sites = set((r["site_ids"] or "").split(","))
            both_ends = (
                svc["site_a_id"] in found_sites and svc["site_z_id"] in found_sites
            )
            direct_evidence.append(
                {
                    "candidate_route_ref": rr,
                    "candidate_lease_id": r["optical_lease_id"],
                    "proof_level": "strong" if both_ends else "medium",
                    "evidence_for": [
                        f"lease {r['optical_lease_id']} a des endpoints aux sites {r['site_ids']}",
                        "les deux bouts confirmes"
                        if both_ends
                        else "un seul bout confirme",
                    ],
                    "evidence_against": []
                    if both_ends
                    else ["site Z non confirme par endpoint"],
                    "source": "lease_endpoint",
                }
            )

    # --- Source 3b : ancre site GDB -> chainage passif [strong/medium/context] ---
    site_route_bundle = _hunt_route_from_site(con, service_id, svc["site_z_id"])
    data_explored.append(
        "hunt_route_from_site(site_z_id) via lease pair, passive chain, cable spatial"
    )
    for item in site_route_bundle.get("direct_evidence", []):
        route_ref = str(item.get("candidate_route_ref") or "").strip()
        if route_ref and route_ref not in seen_routes:
            seen_routes.add(route_ref)
            direct_evidence.append(item)
    for item in site_route_bundle.get("context_only", []):
        route_ref = str(item.get("candidate_route_ref") or "").strip()
        if route_ref and route_ref in seen_routes:
            continue
        context_only.append(item)

    # --- Source 4 : leases par tokens de site/client dans comments/reference [medium] ---
    if client_tokens:
        data_explored.append(
            "ref_optical_lease/comments/reference via tokens client/site"
        )
        lease_matches: dict[str, dict[str, Any]] = {}
        for token in client_tokens[:5]:
            for row in con.execute(
                "SELECT optical_lease_id, ref_exploit, reference, client, lessee, comments "
                "FROM ref_optical_lease "
                "WHERE UPPER(COALESCE(ref_exploit,'') || ' ' || COALESCE(reference,'') || ' ' || "
                "COALESCE(client,'') || ' ' || COALESCE(lessee,'') || ' ' || COALESCE(comments,'')) LIKE ? "
                "LIMIT 12",
                (f"%{token}%",),
            ).fetchall():
                route_key = (
                    row["ref_exploit"] or ""
                ).strip() or f"LEASE:{row['optical_lease_id']}"
                match = lease_matches.setdefault(
                    route_key,
                    {
                        "route_ref": (row["ref_exploit"] or "").strip() or None,
                        "lease_id": row["optical_lease_id"],
                        "matched_tokens": [],
                    },
                )
                if token not in match["matched_tokens"]:
                    match["matched_tokens"].append(token)

        for match in lease_matches.values():
            route_ref = match["route_ref"] or f"LEASE:{match['lease_id']}"
            if route_ref in seen_routes:
                continue
            if len(match["matched_tokens"]) >= 2:
                seen_routes.add(route_ref)
                direct_evidence.append(
                    {
                        "candidate_route_ref": match["route_ref"],
                        "candidate_lease_id": match["lease_id"],
                        "proof_level": "medium",
                        "evidence_for": [
                            f"lease {match['lease_id']} matche tokens {', '.join(match['matched_tokens'])}",
                        ],
                        "evidence_against": [
                            "matching textuel sur comments/reference - confirmation directe absente"
                        ],
                        "source": "lease_comment_token_match",
                    }
                )
            else:
                context_only.append(
                    {
                        "candidate_route_ref": match["route_ref"],
                        "candidate_lease_id": match["lease_id"],
                        "source": "lease_comment_token_match",
                        "evidence_for": [
                            f"lease {match['lease_id']} matche token {', '.join(match['matched_tokens'])}",
                        ],
                        "evidence_against": [
                            "un seul token commun - preuve insuffisante"
                        ],
                        "note": "contexte de lease sans preuve suffisante a lui seul",
                    }
                )

    # --- Source 5 : ref_route_parcours via site A/Z tokens [medium] ---
    has_parcours = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ref_route_parcours'"
    ).fetchone()
    site_z_name = (svc["site_z_name"] or svc["endpoint_z_raw"] or "").upper()
    site_tokens: list[str] = []
    site_a_tokens: list[str] = []
    _EXCLUDE_TOKENS = {
        "SITE",
        "POP",
        "NRA",
        "TELOISE",
        "FRANCE",
        "NORD",
        "SUD",
        "EST",
        "OUEST",
        "SAINT",
        "COMMUNE",
        "VILLE",
        "MAIRIE",
        "AGENCE",
        "HOTEL",
    }
    for tok in re.split(r"[^A-Z0-9]+", site_z_name):
        if len(tok) >= 4 and tok not in _EXCLUDE_TOKENS:
            site_tokens.append(tok)
    for tok in re.split(
        r"[^A-Z0-9]+", (svc["site_a_name"] or svc["endpoint_a_raw"] or "").upper()
    ):
        if len(tok) >= 4 and tok not in _EXCLUDE_TOKENS:
            site_a_tokens.append(tok)

    if has_parcours and site_tokens:
        data_explored.append(f"ref_route_parcours WHERE site LIKE '%{site_tokens[0]}%'")
        seen_parcours_routes: set[str] = set()
        routes_for_a: dict[str, list[str]] = {}
        for tok in site_a_tokens[:3]:
            for row in con.execute(
                "SELECT route_ref FROM ref_route_parcours WHERE UPPER(site) LIKE ? ORDER BY route_ref LIMIT 20",
                (f"%{tok}%",),
            ).fetchall():
                route_ref = (row["route_ref"] or "").strip()
                if route_ref:
                    routes_for_a.setdefault(route_ref, []).append(tok)
        for tok in site_tokens[:3]:
            for r in con.execute(
                "SELECT route_ref, step_type, site, bpe, cable_in, cable_out "
                "FROM ref_route_parcours WHERE UPPER(site) LIKE ? "
                "ORDER BY route_ref LIMIT 10",
                (f"%{tok}%",),
            ).fetchall():
                rr = (r["route_ref"] or "").strip()
                if not rr or rr in seen_routes or rr in seen_parcours_routes:
                    continue
                if rr in routes_for_a:
                    distinct_pair = (tok not in site_a_tokens) or any(
                        a_tok not in site_tokens for a_tok in routes_for_a[rr]
                    )
                    seen_parcours_routes.add(rr)
                    if distinct_pair:
                        seen_routes.add(rr)
                        direct_evidence.append(
                            {
                                "candidate_route_ref": rr,
                                "candidate_lease_id": None,
                                "proof_level": "medium",
                                "evidence_for": [
                                    f"route presente dans ref_route_parcours pour tokens A {', '.join(routes_for_a[rr])}",
                                    f"route presente dans ref_route_parcours pour token Z '{tok}'",
                                ],
                                "evidence_against": [
                                    "parcours par tokens - confirmation directe absente"
                                ],
                                "source": "parcours_site_pair",
                            }
                        )
                    else:
                        context_only.append(
                            {
                                "candidate_route_ref": rr,
                                "source": "parcours_site_pair",
                                "evidence_for": [
                                    f"route partage le meme token geographique '{tok}' cote A et Z",
                                ],
                                "evidence_against": [
                                    "meme token geographique des deux cotes - trop ambigu pour conclure"
                                ],
                                "note": "contexte uniquement - paire de sites non assez discriminante",
                            }
                        )
                    continue
                seen_parcours_routes.add(rr)
                context_only.append(
                    {
                        "candidate_route_ref": rr,
                        "source": "parcours_site_z",
                        "evidence_for": [
                            f"route passe par site token '{tok}' (site_z={svc['site_z_name']})"
                        ],
                        "evidence_against": [
                            "parcours seul insuffisant - pas de lien direct au service"
                        ],
                        "note": "contexte uniquement - ne prouve pas le lien au service",
                    }
                )

    # --- Source 6 : housing -> optical_connection -> cable au site Z [contexte] ---
    has_housing = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ref_optical_housing'"
    ).fetchone()
    has_connection = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ref_optical_connection'"
    ).fetchone()
    has_cable = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ref_optical_cable'"
    ).fetchone()
    if has_housing:
        data_explored.append(
            "ref_optical_housing + ref_optical_connection + ref_optical_cable"
        )
        housing_rows = []
        if svc["site_z_id"]:
            housing_rows = con.execute(
                "SELECT housing_id, migration_oid, housing_type, reference, description, site_id, site_name "
                "FROM ref_optical_housing WHERE site_id = ? LIMIT 10",
                (svc["site_z_id"],),
            ).fetchall()
        if not housing_rows and site_tokens:
            for tok in site_tokens[:2]:
                housing_rows.extend(
                    con.execute(
                        "SELECT housing_id, migration_oid, housing_type, reference, description, site_id, site_name "
                        "FROM ref_optical_housing WHERE UPPER(site_name) LIKE ? LIMIT 5",
                        (f"%{tok}%",),
                    ).fetchall()
                )
        seen_housing_ids: set[str] = set()
        for housing in housing_rows:
            housing_id = str(housing["housing_id"])
            if housing_id in seen_housing_ids:
                continue
            seen_housing_ids.add(housing_id)
            cable_refs: list[str] = []
            if has_connection and has_cable and housing["migration_oid"]:
                for conn in con.execute(
                    "SELECT obj1_type, obj1_migration_oid, obj2_type, obj2_migration_oid "
                    "FROM ref_optical_connection WHERE housing_migration_oid = ? OR obj1_migration_oid = ? OR obj2_migration_oid = ? "
                    "LIMIT 20",
                    (
                        housing["migration_oid"],
                        housing["migration_oid"],
                        housing["migration_oid"],
                    ),
                ).fetchall():
                    for obj_type, obj_migoid in (
                        (conn["obj1_type"], conn["obj1_migration_oid"]),
                        (conn["obj2_type"], conn["obj2_migration_oid"]),
                    ):
                        if not obj_migoid or "CABLE" not in str(obj_type or "").upper():
                            continue
                        for cable in con.execute(
                            "SELECT cable_id, reference, userreference FROM ref_optical_cable WHERE migration_oid = ? LIMIT 5",
                            (obj_migoid,),
                        ).fetchall():
                            ref = (
                                cable["reference"]
                                or cable["userreference"]
                                or cable["cable_id"]
                            )
                            if ref and ref not in cable_refs:
                                cable_refs.append(str(ref))
            context_only.append(
                {
                    "type": "housing_chain",
                    "housing_id": housing["housing_id"],
                    "reference": housing["reference"],
                    "site_name": housing["site_name"],
                    "source": "housing_site_z",
                    "cable_refs": cable_refs,
                    "note": "topologie passive au site Z - utile pour guider la recherche de route",
                }
            )

    for bucket in (direct_evidence, context_only):
        for item in bucket:
            route_ref = str(item.get("candidate_route_ref") or "").strip()
            if not route_ref:
                continue
            coherence = _route_service_coherence(
                con,
                service_id,
                route_ref,
                anchor=anchor,
            )
            item["coherence"] = coherence
            item.setdefault("evidence_for", []).extend(
                coherence.get("positive_signals", [])[:3]
            )
            item.setdefault("evidence_against", []).extend(
                coherence.get("negative_signals", [])[:2]
            )

    direct_evidence.sort(
        key=lambda item: (
            _VLAN_LEVEL_ORDER.get(item.get("proof_level", "weak"), 99),
            -int(item.get("coherence", {}).get("score") or 0),
            item.get("candidate_route_ref") or "",
        )
    )
    context_only.sort(
        key=lambda item: (
            -int(item.get("coherence", {}).get("score") or 0),
            item.get("candidate_route_ref") or "",
        )
    )

    return {
        "service_id": service_id,
        "nature_service": svc["nature_service"],
        "site_a": svc["site_a_name"],
        "site_z": svc["site_z_name"],
        "anchor": anchor,
        "ref_external": ref_external or None,
        "toip_refs_in_lea": toip_refs,
        "direct_evidence": direct_evidence,
        "context_only": context_only,
        "site_route_bundle": site_route_bundle,
        "data_explored": data_explored,
        "summary": (
            f"{len(direct_evidence)} evidence(s) directe(s) : "
            f"{sum(1 for e in direct_evidence if e['proof_level'] == 'strong')} strong, "
            f"{sum(1 for e in direct_evidence if e['proof_level'] == 'medium')} medium. "
            f"{len(context_only)} element(s) contexte seul."
            if direct_evidence
            else f"Aucune evidence directe. {len(context_only)} element(s) contexte seul."
        ),
    }


@tool(
    "hunt_site_anchor",
    "Construit l'ancre d'enquete GDB/spatiale d'un service : sites A/Z, qualite des matches, "
    "seeds spatiaux, actifs optiques proches et point d'entree recommande (site, route, reseau). "
    "A utiliser en debut d'enquete quand l'ancre n'est pas evidente.",
    {"service_id": str},
)
async def hunt_site_anchor(args: dict[str, Any]) -> dict[str, Any]:
    service_id = args.get("service_id", "").strip()
    if not service_id:
        return _text("ERROR: No service_id provided.")

    con = _connect(read_only=True)
    try:
        payload = _build_site_anchor(con, service_id)
        return _text(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    except Exception as e:
        return _text(f"ERROR: {e}")
    finally:
        con.close()


@tool(
    "resolve_passive_chain",
    "Explore la chaine passive depuis un site GDB: site -> housing -> connection -> cable -> lease -> route. "
    "Utile pour comprendre par ou passe la fibre et quels objets optiques sont attaches au site.",
    {"service_id": str, "site_id": str, "depth": int},
)
async def resolve_passive_chain(args: dict[str, Any]) -> dict[str, Any]:
    service_id = args.get("service_id", "").strip()
    site_id = args.get("site_id", "").strip()
    depth = int(args.get("depth", 2) or 2)
    if not service_id:
        return _text("ERROR: No service_id provided.")
    if not site_id:
        return _text("ERROR: No site_id provided.")

    con = _connect(read_only=True)
    try:
        payload = _resolve_passive_chain(con, service_id, site_id, depth=depth)
        return _text(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    except Exception as e:
        return _text(f"ERROR: {e}")
    finally:
        con.close()


@tool(
    "resolve_cable_spatial",
    "Recherche les cables optiques proches du meilleur seed spatial/BAN d'un service. "
    "Retourne des candidats par distance en Lambert-93.",
    {"service_id": str, "radius_meters": int},
)
async def resolve_cable_spatial(args: dict[str, Any]) -> dict[str, Any]:
    service_id = args.get("service_id", "").strip()
    radius_meters = int(args.get("radius_meters", 300) or 300)
    if not service_id:
        return _text("ERROR: No service_id provided.")

    con = _connect(read_only=True)
    try:
        payload = _resolve_cable_spatial(con, service_id, radius_meters=radius_meters)
        return _text(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    except Exception as e:
        return _text(f"ERROR: {e}")
    finally:
        con.close()


@tool(
    "resolve_route_topology",
    "Construit l'empreinte topologique d'une route: endpoints, parcours, BPE, cables, leases et footprint spatiale.",
    {"route_ref": str},
)
async def resolve_route_topology(args: dict[str, Any]) -> dict[str, Any]:
    route_ref = (args.get("route_ref") or "").strip()
    if not route_ref:
        return _text("ERROR: No route_ref provided.")

    con = _connect(read_only=True)
    try:
        payload = _build_route_topology_bundle(con, route_ref)
        return _text(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    except Exception as e:
        return _text(f"ERROR: {e}")
    finally:
        con.close()


@tool(
    "route_service_coherence",
    "Evalue la coherence topo/spatiale entre un service et une route candidate. Retourne des sous-signaux explicables.",
    {"service_id": str, "route_ref": str},
)
async def route_service_coherence(args: dict[str, Any]) -> dict[str, Any]:
    service_id = (args.get("service_id") or "").strip()
    route_ref = (args.get("route_ref") or "").strip()
    if not service_id:
        return _text("ERROR: No service_id provided.")
    if not route_ref:
        return _text("ERROR: No route_ref provided.")

    con = _connect(read_only=True)
    try:
        payload = _route_service_coherence(con, service_id, route_ref)
        return _text(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    except Exception as e:
        return _text(f"ERROR: {e}")
    finally:
        con.close()


@tool(
    "hunt_route_from_site",
    "Chasse a la route optique en partant du site GDB/Z: paires de lease, chainage passif et cables proches. "
    "A privilegier quand l'ancre site est plus fiable que les labels.",
    {"service_id": str, "site_z_id": str},
)
async def hunt_route_from_site(args: dict[str, Any]) -> dict[str, Any]:
    service_id = args.get("service_id", "").strip()
    site_z_id = (args.get("site_z_id") or "").strip() or None
    if not service_id:
        return _text("ERROR: No service_id provided.")

    con = _connect(read_only=True)
    try:
        payload = _hunt_route_from_site(con, service_id, site_z_id)
        return _text(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    except Exception as e:
        return _text(f"ERROR: {e}")
    finally:
        con.close()


@tool(
    "hunt_vlan",
    "Chasse au VLAN pour un service L2L. "
    "Retourne des hypotheses de VLAN avec evidence_for, evidence_against et proof_level. "
    "Sources explorees (ordre de fiabilite) : "
    "(1) ancre TOIP via xconnect_circuit_id [strong], "
    "(2) labels client dans ref_network_vlans [medium], "
    "(3) sub-interfaces CO actives du POP A [medium si coherent], "
    "(4) CPE hostname depuis service_support_reseau [strong si CPE+port+vlan]. "
    "Point de depart recommande pour tout service L2L sans network_vlan_id.",
    {"service_id": str},
)
async def hunt_vlan(args: dict[str, Any]) -> dict[str, Any]:
    service_id = args.get("service_id", "").strip()
    if not service_id:
        return _text("ERROR: No service_id provided.")

    con = _connect(read_only=True)
    try:
        payload = _hunt_vlan(con, service_id)
        return _text(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    except Exception as e:
        return _text(f"ERROR: {e}")
    finally:
        con.close()


@tool(
    "hunt_route",
    "Chasse a la route optique pour un service L2L ou FON. "
    "Separe evidence directe (preuve suffisante) et contexte seul (non suffisant). "
    "Sources directes : ref_exploit exact sur ref_external, TOIP dans ref_optical_lease, "
    "endpoints lease aux sites A et Z. "
    "Sources contexte : routes voisines par parcours, housing au site Z. "
    "Point de depart recommande pour tout service sans route_ref.",
    {"service_id": str},
)
async def hunt_route(args: dict[str, Any]) -> dict[str, Any]:
    service_id = args.get("service_id", "").strip()
    if not service_id:
        return _text("ERROR: No service_id provided.")

    con = _connect(read_only=True)
    try:
        payload = _hunt_route(con, service_id)
        return _text(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    except Exception as e:
        return _text(f"ERROR: {e}")
    finally:
        con.close()


@tool(
    "get_co_cluster",
    "Cartographie tous les VLANs et circuits d'un CO (ex: 'COM1', 'CRL1', 'AMI3', 'BEA1'). "
    "Distingue VLANs actifs (xconnect present = circuit reel confirme) "
    "vs VLANs pool (xconnect NULL, souvent VREG_* = ressources non affectees). "
    "Utile pour raisonner par cohorte : identifier les VLANs libres ou parmi les circuits "
    "actifs lesquels correspondent a des services clients connus. "
    "IMPORTANT : xconnect present = signal fort, xconnect absent != preuve que c'est un pool.",
    {"location_prefix": str},
)
async def get_co_cluster(args: dict[str, Any]) -> dict[str, Any]:
    location_prefix = (args.get("location_prefix") or "").strip().upper()
    if not location_prefix:
        return _text(
            "ERROR: No location_prefix provided (ex: 'COM1', 'CRL1', 'AMI3', 'BEA1')."
        )

    con = _connect(read_only=True)
    try:
        # SW prefix: COM1-CO-* → COM1-SW-*
        sw_prefix = location_prefix.replace("-CO-", "-SW-")
        if sw_prefix == location_prefix:
            sw_prefix = location_prefix  # no -CO- in name, use as-is

        all_subifs = con.execute(
            """SELECT cs.subif_id, cs.device_name, cs.interface_name, cs.vlan_id,
                      cs.description, cs.site_code, cs.xconnect_ip, cs.xconnect_circuit_id,
                      nv.network_vlan_id, nv.label AS sw_label, nv.device_name AS sw_device
               FROM ref_co_subinterface cs
               LEFT JOIN ref_network_vlans nv ON nv.vlan_id = cs.vlan_id
                  AND nv.device_name LIKE ?
               WHERE cs.device_name LIKE ?
               ORDER BY cs.device_name, cs.vlan_id""",
            (f"{sw_prefix}%", f"{location_prefix}-CO-%"),
        ).fetchall()

        devices: list[str] = []
        active_circuits: list[dict[str, Any]] = []
        pool_vlans: list[dict[str, Any]] = []
        toip_circuits: list[dict[str, Any]] = []

        seen_devs: set[str] = set()
        for r in all_subifs:
            dev = r["device_name"]
            if dev not in seen_devs:
                devices.append(dev)
                seen_devs.add(dev)

            row_dict = _row_to_dict(r)
            xconn = r["xconnect_circuit_id"]
            if xconn:
                active_circuits.append(row_dict)
                # TOIP circuits: xconnect numerique
                if str(xconn).isdigit():
                    toip_circuits.append(row_dict)
            else:
                pool_vlans.append(row_dict)

        return _text(
            json.dumps(
                {
                    "location_prefix": location_prefix,
                    "sw_prefix_used": sw_prefix,
                    "devices": devices,
                    "total_subifs": len(all_subifs),
                    "active_circuits_count": len(active_circuits),
                    "pool_vlans_count": len(pool_vlans),
                    "toip_circuits_count": len(toip_circuits),
                    "active_circuits": active_circuits[:50],
                    "pool_vlans": pool_vlans[:20],
                    "toip_circuits": toip_circuits[:30],
                    "note": (
                        "active_circuits = xconnect_circuit_id non null (circuit reel confirme). "
                        "pool_vlans = xconnect null (potentiellement libre mais non prouve). "
                        "toip_circuits = xconnect numerique uniquement (candidats TOIP)."
                    ),
                },
                indent=2,
                ensure_ascii=False,
                default=str,
            )
        )
    except Exception as e:
        return _text(f"ERROR: {e}")
    finally:
        con.close()
