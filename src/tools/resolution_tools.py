"""MCP tools for submitting and validating service reconciliation resolutions."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from ..sdk_compat import tool

from .validation_lib import validate_site, validate_device_pop, validate_route_endpoints

_db_path: Path | None = None

AGENT_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS agent_resolutions (
    resolution_id TEXT PRIMARY KEY,
    service_id TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now')),
    confidence TEXT CHECK(confidence IN ('high','medium','low')),
    site_a TEXT,
    site_z TEXT,
    network_support_id TEXT,
    optical_support_ref TEXT,
    party_final TEXT,
    party_final_id TEXT,
    resolved_site_a_id TEXT,
    resolved_site_z_id TEXT,
    route_ref TEXT,
    route_id TEXT,
    lease_id TEXT,
    fiber_lease_id TEXT,
    isp_lease_id TEXT,
    cable_id TEXT,
    housing_id TEXT,
    spatial_best_object_type TEXT,
    spatial_best_object_id TEXT,
    spatial_distance_meters REAL,
    spatial_note TEXT,
    network_interface_id TEXT,
    network_vlan_id TEXT,
    cpe_id TEXT,
    config_id TEXT,
    inferred_vlans_json TEXT,
    justification TEXT NOT NULL,
    status TEXT DEFAULT 'proposed',
    evidence_count INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS agent_evidence (
    evidence_id TEXT PRIMARY KEY,
    resolution_id TEXT NOT NULL,
    service_id TEXT NOT NULL,
    evidence_type TEXT NOT NULL,
    source_table TEXT,
    source_key TEXT,
    description TEXT NOT NULL,
    score INTEGER DEFAULT 0,
    payload_json TEXT
);
"""

_AGENT_RESOLUTION_COLUMNS = {
    "resolved_site_a_id": "TEXT",
    "resolved_site_z_id": "TEXT",
    "route_ref": "TEXT",
    "route_id": "TEXT",
    "lease_id": "TEXT",
    "fiber_lease_id": "TEXT",
    "isp_lease_id": "TEXT",
    "cable_id": "TEXT",
    "housing_id": "TEXT",
    "spatial_best_object_type": "TEXT",
    "spatial_best_object_id": "TEXT",
    "spatial_distance_meters": "REAL",
    "spatial_note": "TEXT",
    "network_interface_id": "TEXT",
    "network_vlan_id": "TEXT",
    "cpe_id": "TEXT",
    "config_id": "TEXT",
    "inferred_vlans_json": "TEXT",
}


def configure(db_path: Path) -> None:
    global _db_path
    _db_path = Path(db_path)


def _safe_hash(parts: list[str]) -> str:
    payload = "||".join(str(p).strip().lower() for p in parts)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]


def _connect() -> sqlite3.Connection:
    if _db_path is None or not _db_path.exists():
        raise RuntimeError(f"Database not configured or missing: {_db_path}")
    con = sqlite3.connect(str(_db_path))
    con.execute("PRAGMA journal_mode=WAL")
    con.row_factory = sqlite3.Row
    return con


def ensure_agent_tables(db_path: Path) -> None:
    """Create agent tables if they don't exist. Called from CLI prepare command."""
    con = sqlite3.connect(str(db_path))
    try:
        con.executescript(AGENT_SCHEMA_SQL)
        existing_columns = {
            row[1] for row in con.execute("PRAGMA table_info(agent_resolutions)").fetchall()
        }
        for column_name, column_type in _AGENT_RESOLUTION_COLUMNS.items():
            if column_name not in existing_columns:
                con.execute(
                    f'ALTER TABLE agent_resolutions ADD COLUMN "{column_name}" {column_type}'
                )
        con.commit()
    finally:
        con.close()


def _text(content: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": content}]}


def _distinct_evidence_types(evidences: list[dict[str, Any]]) -> set[str]:
    return {str(ev.get("evidence_type", "unknown")).strip() or "unknown" for ev in evidences}


def _has_party_search_evidence(evidences: list[dict[str, Any]]) -> bool:
    return any(str(ev.get("evidence_type", "")).strip() == "party_search" for ev in evidences)


def _mentions_party_search_failure(justification: str) -> bool:
    lowered = justification.lower()
    required_markers = ("party", "final")
    failure_markers = ("introuv", "not found", "unresolved", "search", "recherche")
    return all(marker in lowered for marker in required_markers) and any(marker in lowered for marker in failure_markers)


@tool(
    "submit_resolution",
    "Soumet une resolution pour un service. Le service_id doit exister dans "
    "service_master_active. La resolution inclut les champs trouves "
    "(site_a, site_z, network_support_id, optical_support_ref, party_final, "
    "party_final_id), une confidence (high/medium/low), une justification, "
    "et une liste d'evidences (au moins 1).",
    {"service_id": str, "resolution_json": str},
)
async def submit_resolution(args: dict[str, Any]) -> dict[str, Any]:
    service_id = args.get("service_id", "").strip()
    resolution_raw = args.get("resolution_json", "").strip()

    if not service_id:
        return _text("ERROR: No service_id provided.")
    if not resolution_raw:
        return _text("ERROR: No resolution_json provided.")

    try:
        resolution = json.loads(resolution_raw)
    except json.JSONDecodeError as e:
        return _text(f"ERROR: Invalid JSON: {e}")

    if _db_path is not None:
        ensure_agent_tables(_db_path)
    con = _connect()
    try:
        # Validate service exists
        svc = con.execute(
            "SELECT service_id FROM service_master_active WHERE service_id = ?",
            (service_id,),
        ).fetchone()
        if not svc:
            return _text(f"ERROR: service_id '{service_id}' not found in service_master_active.")

        confidence = resolution.get("confidence", "low")
        if confidence not in ("high", "medium", "low"):
            return _text(f"ERROR: confidence must be high/medium/low, got '{confidence}'.")

        evidences = resolution.get("evidences", [])
        if not evidences:
            return _text("ERROR: at least 1 evidence is required for traceability.")

        justification = resolution.get("justification", "").strip()
        if not justification:
            return _text("ERROR: justification is required.")

        party_final_id = str(resolution.get("party_final_id") or "").strip()

        resolution_id = _safe_hash([service_id, datetime.now().isoformat()])
        inferred_vlans = resolution.get("inferred_vlans_json")
        if inferred_vlans is None and resolution.get("inferred_vlans") is not None:
            inferred_vlans = resolution.get("inferred_vlans")
        if isinstance(inferred_vlans, (list, dict)):
            inferred_vlans = json.dumps(inferred_vlans, ensure_ascii=True)

        # Insert resolution
        con.execute(
            """INSERT OR REPLACE INTO agent_resolutions
            (resolution_id, service_id, confidence,
             site_a, site_z, network_support_id, optical_support_ref,
             party_final, party_final_id,
             resolved_site_a_id, resolved_site_z_id,
             route_ref, route_id, lease_id, fiber_lease_id, isp_lease_id, cable_id, housing_id,
             spatial_best_object_type, spatial_best_object_id, spatial_distance_meters, spatial_note,
             network_interface_id, network_vlan_id, cpe_id, config_id, inferred_vlans_json,
             justification, evidence_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                resolution_id,
                service_id,
                confidence,
                resolution.get("site_a"),
                resolution.get("site_z"),
                resolution.get("network_support_id"),
                resolution.get("optical_support_ref"),
                resolution.get("party_final"),
                party_final_id or None,
                resolution.get("resolved_site_a_id"),
                resolution.get("resolved_site_z_id"),
                resolution.get("route_ref"),
                resolution.get("route_id"),
                resolution.get("lease_id"),
                resolution.get("fiber_lease_id"),
                resolution.get("isp_lease_id"),
                resolution.get("cable_id"),
                resolution.get("housing_id"),
                resolution.get("spatial_best_object_type"),
                resolution.get("spatial_best_object_id"),
                resolution.get("spatial_distance_meters"),
                resolution.get("spatial_note"),
                resolution.get("network_interface_id"),
                resolution.get("network_vlan_id"),
                resolution.get("cpe_id"),
                resolution.get("config_id"),
                inferred_vlans,
                justification,
                len(evidences),
            ),
        )

        # Insert evidences
        for ev in evidences:
            ev_id = _safe_hash([
                resolution_id,
                ev.get("evidence_type", ""),
                ev.get("description", ""),
            ])
            payload = json.dumps(ev.get("payload", {})) if ev.get("payload") else None
            con.execute(
                """INSERT OR REPLACE INTO agent_evidence
                (evidence_id, resolution_id, service_id, evidence_type,
                 source_table, source_key, description, score, payload_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    ev_id,
                    resolution_id,
                    service_id,
                    ev.get("evidence_type", "unknown"),
                    ev.get("source_table"),
                    ev.get("source_key"),
                    ev.get("description", ""),
                    ev.get("score", 0),
                    payload,
                ),
            )

        con.commit()
        return _text(
            f"Resolution submitted: {resolution_id}\n"
            f"- service: {service_id}\n"
            f"- confidence: {confidence}\n"
            f"- evidences: {len(evidences)}\n"
            f"- status: proposed"
        )
    except Exception as e:
        return _text(f"ERROR: {e}")
    finally:
        con.close()


@tool(
    "validate_resolution",
    "Valide une resolution soumise en effectuant des controles croises : "
    "les sites existent-ils dans ref_sites? Le device est-il dans le bon POP? "
    "La route optique est-elle coherente? "
    "Passe le status de 'proposed' a 'validated' si tout est OK.",
    {"service_id": str},
)
async def validate_resolution(args: dict[str, Any]) -> dict[str, Any]:
    service_id = args.get("service_id", "").strip()
    if not service_id:
        return _text("ERROR: No service_id provided.")

    con = _connect()
    try:
        res = con.execute(
            "SELECT * FROM agent_resolutions WHERE service_id = ? ORDER BY created_at DESC LIMIT 1",
            (service_id,),
        ).fetchone()
        if not res:
            return _text(f"No resolution found for service {service_id}.")

        checks: list[str] = []
        warnings: list[str] = []
        errors: list[str] = []
        evidence_rows = con.execute(
            """
            SELECT evidence_type, score
            FROM agent_evidence
            WHERE resolution_id = ?
            """,
            (res["resolution_id"],),
        ).fetchall()
        evidence_types = {row["evidence_type"] for row in evidence_rows}

        # Anti self-loop: site_a must differ from site_z
        if res["site_a"] and res["site_z"] and res["site_a"].strip().upper() == res["site_z"].strip().upper():
            errors.append(f"site_a and site_z are identical ('{res['site_a']}') — self-loop")

        if not (res["party_final_id"] or "").strip():
            checks.append("party_final_id missing — noted in justification")

        # Validate site_a via validation_lib
        if res["site_a"]:
            vr = validate_site(con, res["site_a"])
            if vr.passed:
                checks.append(f"site_a: {vr.detail}")
            else:
                warnings.append(f"site_a: {vr.detail}")

        # Validate site_z via validation_lib
        if res["site_z"]:
            vr = validate_site(con, res["site_z"])
            if vr.passed:
                checks.append(f"site_z: {vr.detail}")
            else:
                warnings.append(f"site_z: {vr.detail}")

        # Validate network device POP
        if res["network_support_id"]:
            dev = con.execute(
                "SELECT device_name FROM ref_network_devices WHERE device_name = ?",
                (res["network_support_id"],),
            ).fetchone()
            if dev:
                checks.append(f"network device '{res['network_support_id']}' found")
                # Also validate POP association
                vr = validate_device_pop(con, res["network_support_id"], res["site_a"] or "", res["site_z"] or "")
                if vr.passed:
                    checks.append(f"POP check: {vr.detail}")
                else:
                    warnings.append(f"POP check: {vr.detail}")
            else:
                iface = con.execute(
                    "SELECT device_name FROM ref_network_interfaces WHERE device_name = ? LIMIT 1",
                    (res["network_support_id"],),
                ).fetchone()
                if iface:
                    checks.append(f"network device '{res['network_support_id']}' found in interfaces")
                else:
                    warnings.append(f"network device '{res['network_support_id']}' NOT found")

        # Validate optical route endpoints
        if res["optical_support_ref"]:
            vr = validate_route_endpoints(con, res["optical_support_ref"], res["site_a"] or "", res["site_z"] or "")
            if vr.passed:
                checks.append(f"optical route: {vr.detail}")
            else:
                warnings.append(f"optical route: {vr.detail}")

        # Check party
        if res["party_final_id"]:
            party = con.execute(
                "SELECT party_id, canonical_name FROM party_master WHERE party_id = ?",
                (res["party_final_id"],),
            ).fetchone()
            if party:
                checks.append(f"party '{res['party_final_id']}' ({party['canonical_name']}) found")
            else:
                warnings.append(f"party_final_id '{res['party_final_id']}' NOT found in party_master")

        # Note low confidence for reporting
        if res["confidence"] == "low":
            checks.append("low confidence — flagged for review")

        # Decide validation status
        if errors:
            status = "rejected"
        elif warnings:
            status = "needs_review"
        else:
            status = "validated"

        con.execute(
            "UPDATE agent_resolutions SET status = ? WHERE resolution_id = ?",
            (status, res["resolution_id"]),
        )
        con.commit()

        lines = [f"## Validation: {service_id}", f"Status: **{status}**", ""]
        if checks:
            lines.append("**Checks passed:**")
            for c in checks:
                lines.append(f"- {c}")
        if warnings:
            lines.append("\n**Warnings:**")
            for w in warnings:
                lines.append(f"- {w}")
        if errors:
            lines.append("\n**Errors:**")
            for e in errors:
                lines.append(f"- {e}")

        return _text("\n".join(lines))
    except Exception as e:
        return _text(f"ERROR: {e}")
    finally:
        con.close()


@tool(
    "submit_and_validate",
    "Soumet puis valide immediatement une resolution pour un service. "
    "Si la soumission echoue, la validation n'est pas lancee.",
    {"service_id": str, "resolution_json": str},
)
async def submit_and_validate(args: dict[str, Any]) -> dict[str, Any]:
    submit_result = await submit_resolution.handler(args)
    submit_text = submit_result["content"][0]["text"]
    if "ERROR:" in submit_text:
        return submit_result

    validate_result = await validate_resolution.handler({"service_id": args.get("service_id", "")})
    validate_text = validate_result["content"][0]["text"]
    return _text(f"{submit_text}\n\n---\n\n{validate_text}")


@tool(
    "list_resolutions",
    "Liste les resolutions soumises par l'agent. "
    "Filtres optionnels par client, nature de service, ou status.",
    {"filter_client": str, "filter_nature": str, "filter_status": str},
)
async def list_resolutions(args: dict[str, Any]) -> dict[str, Any]:
    filter_client = args.get("filter_client", "").strip()
    filter_nature = args.get("filter_nature", "").strip()
    filter_status = args.get("filter_status", "").strip()

    con = _connect()
    try:
        query = """
            SELECT r.resolution_id, r.service_id, r.confidence, r.status,
                   r.evidence_count, r.justification,
                   s.principal_client, s.nature_service,
                   r.network_vlan_id, r.route_ref
            FROM agent_resolutions r
            JOIN service_master_active s ON s.service_id = r.service_id
            WHERE 1=1
        """
        params: list[str] = []

        if filter_client:
            query += " AND s.principal_client LIKE ?"
            params.append(f"%{filter_client}%")
        if filter_nature:
            query += " AND s.nature_service LIKE ?"
            params.append(f"%{filter_nature}%")
        if filter_status:
            query += " AND r.status = ?"
            params.append(filter_status)

        query += " ORDER BY r.created_at DESC LIMIT 100"

        rows = con.execute(query, params).fetchall()

        if not rows:
            return _text("No resolutions found matching filters.")

        lines = [
            "service_id | client | nature | confidence | status | evidences | vlan | route_ref",
            "--- | --- | --- | --- | --- | --- | --- | ---",
        ]
        for row in rows:
            lines.append(
                f"{row['service_id']} | {row['principal_client']} | "
                f"{row['nature_service']} | {row['confidence']} | "
                f"{row['status']} | {row['evidence_count']} | "
                f"{row['network_vlan_id'] or '-'} | {row['route_ref'] or '-'}"
            )

        return _text(f"**{len(rows)} resolutions**\n\n" + "\n".join(lines))
    finally:
        con.close()
