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
    stripped = sql.strip()
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
        if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='service_spatial_seed'").fetchone()
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
        if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='service_spatial_evidence'").fetchone()
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
        if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='service_lea_signal'").fetchone()
        else []
    )

    return {
        "service": _row_to_dict(service),
        "review_items": [
            {
                "review_type": row["review_type"],
                "severity": row["severity"],
                "reason": row["reason"],
                "context": json.loads(row["context_json"]) if row["context_json"] else None,
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
                "payload": json.loads(row["payload_json"]) if row["payload_json"] else None,
            }
            for row in evidences
        ],
        "party_rows": _rows_to_dicts(party_rows),
        "endpoint_rows": _rows_to_dicts(endpoint_rows),
        "network_support_rows": _rows_to_dicts(network_support_rows),
        "optical_support_rows": _rows_to_dicts(optical_support_rows),
        "lea_signal_rows": _rows_to_dicts(lea_signal_rows),
        "spatial_seed_rows": _rows_to_dicts(spatial_seed_rows),
        "spatial_evidence_rows": [
            {
                **_row_to_dict(row),
                "context": json.loads(row["context_json"]) if row["context_json"] else None,
            }
            for row in spatial_evidence_rows
        ],
        "gold_row": _row_to_dict(gold_row),
    }


def _resolve_party_candidates(con: sqlite3.Connection, service_id: str) -> dict[str, Any]:
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

    recommended_final_party_id: str | None = None
    recommended_final_party_name: str | None = None
    recommendation_confidence = "none"
    reason = "no deterministic final party candidate found"

    if pipeline_final:
        recommended_final_party_id = pipeline_final[0]["party_id"]
        recommended_final_party_name = pipeline_final[0]["canonical_name"]
        recommendation_confidence = "high"
        reason = "pipeline final_party already exists"
    elif client_final_alias_matches:
        recommended_final_party_id = client_final_alias_matches[0]["party_id"]
        recommended_final_party_name = client_final_alias_matches[0]["canonical_name"]
        recommendation_confidence = "medium"
        reason = "exact alias match on client_final_raw"

    return {
        "service_id": service_id,
        "principal_client_raw": service["principal_client"] or "",
        "client_final_raw": service["client_final"] or "",
        "pipeline_contract_parties": _rows_to_dicts(pipeline_contract),
        "pipeline_final_parties": _rows_to_dicts(pipeline_final),
        "principal_client_alias_matches": principal_alias_matches,
        "client_final_alias_matches": client_final_alias_matches,
        "recommended_final_party_id": recommended_final_party_id,
        "recommended_final_party_name": recommended_final_party_name,
        "recommendation_confidence": recommendation_confidence,
        "reason": reason,
    }


def _resolve_optical_candidates(con: sqlite3.Connection, service_id: str) -> dict[str, Any]:
    gold_cols = {
        row["name"] for row in con.execute('PRAGMA table_info("gold_service_active")').fetchall()
    }
    optical_cols = {
        row["name"] for row in con.execute('PRAGMA table_info("service_support_optique")').fetchall()
    }
    gold_select = [col for col in ("route_ref", "route_id", "lease_id", "fiber_lease_id", "isp_lease_id") if col in gold_cols]
    optical_select = [
        col
        for col in (
            "route_ref", "route_id", "route_match_rule", "route_score",
            "lease_ref", "lease_id", "lease_match_rule", "lease_score",
            "fiber_lease_id", "fiber_lease_match_rule", "fiber_lease_score",
            "isp_lease_id", "isp_lease_match_rule", "isp_lease_score", "support_type", "support_ref",
            "logical_route_id", "cable_id", "cable_match_rule", "cable_score",
            "housing_id", "housing_match_rule", "housing_score",
            "site_a_optical_id", "site_z_optical_id", "optical_context_json",
            "spatial_match_rule", "spatial_distance_meters", "spatial_score",
        )
        if col in optical_cols
    ]

    gold_row = (
        con.execute(
            f'SELECT {", ".join(gold_select)} FROM gold_service_active WHERE service_id = ?',
            (service_id,),
        ).fetchone()
        if gold_select
        else None
    )
    support_rows = (
        con.execute(
            f'SELECT {", ".join(optical_select)} FROM service_support_optique WHERE service_id = ?',
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
            con.execute("SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name='ref_optical_logical_route'").fetchone()
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
            con.execute("SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name='ref_optical_lease_endpoint'").fetchone()
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
            con.execute("SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name='ref_optical_cable'").fetchone()
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
            con.execute("SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name='ref_optical_housing'").fetchone()
        )
        else []
    )
    # Search cables by site tokens matching service endpoints A/Z
    nearby_cables: list[dict[str, Any]] = []
    has_cable_table = _row_to_dict(
        con.execute("SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name='ref_optical_cable'").fetchone()
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
                    "SITE", "POP", "NRA", "TELOISE", "FRANCE", "NORD", "SUD", "EST", "OUEST",
                    "SAINT", "COMMUNE", "VILLE", "MAIRIE", "AGENCE", "HOTEL",
                }:
                    site_tokens.append(token)
        matched_cable_ids = {r["cable_id"] for r in cable_candidates} if cable_candidates else set()
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

    return {
        "service_id": service_id,
        "gold_optical": _row_to_dict(gold_row),
        "optical_candidates": _rows_to_dicts(support_rows),
        "logical_routes": _rows_to_dicts(logical_routes),
        "lease_endpoints": _rows_to_dicts(lease_endpoints),
        "cable_candidates": _rows_to_dicts(cable_candidates),
        "housing_candidates": _rows_to_dicts(housing_candidates),
        "nearby_cables_by_site": nearby_cables,
    }


def _resolve_network_candidates(con: sqlite3.Connection, service_id: str) -> dict[str, Any]:
    gold_cols = {
        row["name"] for row in con.execute('PRAGMA table_info("gold_service_active")').fetchall()
    }
    network_cols = {
        row["name"] for row in con.execute('PRAGMA table_info("service_support_reseau")').fetchall()
    }
    gold_select = [
        col
        for col in ("interface_id", "network_interface_id", "network_vlan_id", "cpe_id", "config_id", "inferred_vlans_json")
        if col in gold_cols
    ]
    network_select = [
        col
        for col in (
            "service_ref", "interface_id", "interface_match_rule", "interface_score",
            "network_interface_id", "network_interface_match_rule", "network_interface_score",
            "network_vlan_id", "network_vlan_match_rule", "network_vlan_score",
            "cpe_id", "cpe_match_rule", "cpe_score",
            "config_id", "config_match_rule", "config_score", "inferred_vlans_json",
            "device_name", "interface_name",
        )
        if col in network_cols
    ]
    gold_row = (
        con.execute(
            f'SELECT {", ".join(gold_select)} FROM gold_service_active WHERE service_id = ?',
            (service_id,),
        ).fetchone()
        if gold_select
        else None
    )
    support_rows = (
        con.execute(
            f'SELECT {", ".join(network_select)} FROM service_support_reseau WHERE service_id = ?',
            (service_id,),
        ).fetchall()
        if network_select
        else []
    )
    payload = {
        "service_id": service_id,
        "gold_network": _row_to_dict(gold_row),
        "network_candidates": _rows_to_dicts(support_rows),
    }
    if gold_row and gold_row["inferred_vlans_json"]:
        payload["gold_network"]["inferred_vlans"] = json.loads(gold_row["inferred_vlans_json"])
    for row in payload["network_candidates"]:
        if row.get("inferred_vlans_json"):
            row["inferred_vlans"] = json.loads(row["inferred_vlans_json"])
    return payload


def _resolve_spatial_candidates(con: sqlite3.Connection, service_id: str) -> dict[str, Any]:
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
        if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='service_spatial_seed'").fetchone()
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
        if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='service_spatial_evidence'").fetchone()
        else []
    )
    site_candidates = [row for row in evidences if row["target_table"] == "ref_sites"]
    housing_candidates = [row for row in evidences if row["target_table"] == "ref_optical_housing"]
    cable_candidates = [row for row in evidences if row["target_table"] == "ref_optical_cable"]
    best = evidences[0] if evidences else None
    return {
        "service_id": service_id,
        "spatial_seeds": _rows_to_dicts(seeds),
        "site_candidates": [
            {**_row_to_dict(row), "context": json.loads(row["context_json"]) if row["context_json"] else None}
            for row in site_candidates
        ],
        "housing_candidates": [
            {**_row_to_dict(row), "context": json.loads(row["context_json"]) if row["context_json"] else None}
            for row in housing_candidates
        ],
        "cable_candidates": [
            {**_row_to_dict(row), "context": json.loads(row["context_json"]) if row["context_json"] else None}
            for row in cable_candidates
        ],
        "best_spatial_evidence": (
            {**_row_to_dict(best), "context": json.loads(best["context_json"]) if best and best["context_json"] else None}
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


def _resolve_lea_signal_candidates(con: sqlite3.Connection, service_id: str) -> dict[str, Any]:
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
        if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='service_lea_signal'").fetchone()
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
        "recommended_address_signals": [row for row in payload_rows if row["signal_kind"] in {"postal_address_precise", "mixed_site_address", "postal_address_partial", "postcode_city", "city_only"}][:5],
        "recommended_site_signals": [row for row in payload_rows if row["signal_kind"] in {"technical_site_anchor", "site_label_business", "mixed_site_address", "postal_address_precise", "postal_address_partial"}][:5],
        "recommended_technical_signals": [row for row in payload_rows if row["signal_kind"] == "technical_site_anchor"][:5],
        "recommended_route_signals": [row for row in payload_rows if row["signal_kind"] == "route_or_service_reference"][:5],
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
            result += f"\n\n... (truncated at {_MAX_ROWS} rows, add LIMIT to your query)"
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
        return _text(f"ERROR: invalid table name '{table_name}' (only alphanumeric and _ allowed).")

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
                sample_str = ", ".join(
                    str(s[0])[:40] for s in samples
                )
            except Exception:
                nulls = "?"
                distinct = "?"
                sample_str = ""

            lines.append(f"{col_name} | {col_type} | {nulls} | {distinct} | {sample_str}")

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
