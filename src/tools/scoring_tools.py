"""MCP tool providing a live reconciliation scorecard."""

from __future__ import annotations

import re
import sqlite3
import unicodedata
from pathlib import Path
from typing import Any

from ..sdk_compat import tool

_db_path: Path | None = None


def configure(db_path: Path) -> None:
    global _db_path
    _db_path = Path(db_path)


def _connect() -> sqlite3.Connection:
    if _db_path is None or not _db_path.exists():
        raise RuntimeError(f"Database not configured or missing: {_db_path}")
    con = sqlite3.connect(str(_db_path))
    con.execute("PRAGMA journal_mode=WAL")
    con.row_factory = sqlite3.Row
    return con


def _text(content: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": content}]}


def _party_role_query() -> str:
    return "role_name IN ('client_final', 'final_party')"


def _normalized_status(status: str) -> str:
    if status == "validated_with_warnings":
        return "needs_review (legacy)"
    return status


def _normalize_alias(value: object) -> str:
    text = "" if value is None else str(value)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.upper()
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _count_alias_direct_matches(con: sqlite3.Connection) -> int:
    alias_rows = con.execute("SELECT normalized_alias FROM party_alias").fetchall()
    normalized_aliases = {row["normalized_alias"] for row in alias_rows if row["normalized_alias"]}
    service_rows = con.execute(
        """
        SELECT DISTINCT sm.service_id, sm.principal_client
        FROM agent_resolutions ar
        JOIN service_master_active sm ON sm.service_id = ar.service_id
        WHERE ar.party_final_id IS NULL OR TRIM(ar.party_final_id) = ''
        """
    ).fetchall()
    return sum(
        1
        for row in service_rows
        if _normalize_alias(row["principal_client"]) in normalized_aliases
    )


def compute_scorecard(db_path: Path, focus: str | None = None) -> str:
    """Compute and format the reconciliation scorecard."""
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row

    try:
        lines: list[str] = []
        lines.append("=" * 64)
        lines.append(" SCORECARD RECONCILIATION SERVICE-REF")
        lines.append("=" * 64)

        # Total services
        total = con.execute("SELECT COUNT(*) c FROM service_master_active").fetchone()["c"]

        # Gold states
        gold_states = con.execute(
            "SELECT match_state, COUNT(*) c FROM gold_service_active GROUP BY match_state"
        ).fetchall()
        state_map = {r["match_state"]: r["c"] for r in gold_states}
        auto_valid = state_map.get("auto_valid", 0)
        review_required = state_map.get("review_required", 0)

        lines.append(f"\n SERVICES ACTIFS: {total}")
        lines.append(f"   Auto-valides (pipeline): {auto_valid}")
        lines.append(f"   En attente de review:    {review_required}")

        # Agent resolutions
        agent_table_exists = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='agent_resolutions'"
        ).fetchone()

        if agent_table_exists:
            res_stats = con.execute(
                "SELECT confidence, status, COUNT(*) c FROM agent_resolutions "
                "GROUP BY confidence, status ORDER BY confidence, status"
            ).fetchall()

            total_resolved = sum(r["c"] for r in res_stats)
            by_confidence = {}
            by_status = {}
            for r in res_stats:
                by_confidence[r["confidence"]] = by_confidence.get(r["confidence"], 0) + r["c"]
                status_key = _normalized_status(r["status"])
                by_status[status_key] = by_status.get(status_key, 0) + r["c"]

            proposed = con.execute(
                "SELECT COUNT(*) c FROM agent_resolutions WHERE status = 'proposed'"
            ).fetchone()["c"]
            missing_party = con.execute(
                """
                SELECT COUNT(*) c
                FROM agent_resolutions
                WHERE party_final_id IS NULL OR TRIM(party_final_id) = ''
                """
            ).fetchone()["c"]
            medium_high_missing_party = con.execute(
                """
                SELECT COUNT(*) c
                FROM agent_resolutions
                WHERE confidence IN ('medium', 'high')
                  AND (party_final_id IS NULL OR TRIM(party_final_id) = '')
                """
            ).fetchone()["c"]
            low_one_evidence = con.execute(
                """
                SELECT COUNT(*) c
                FROM agent_resolutions
                WHERE confidence = 'low' AND evidence_count = 1
                """
            ).fetchone()["c"]
            low_one_party_only = con.execute(
                """
                SELECT COUNT(*) c
                FROM agent_resolutions ar
                WHERE ar.confidence = 'low'
                  AND ar.evidence_count = 1
                  AND EXISTS (
                      SELECT 1
                      FROM agent_evidence ae
                      WHERE ae.resolution_id = ar.resolution_id
                        AND ae.evidence_type = 'party'
                  )
                """
            ).fetchone()["c"]

            lines.append(f"\n AGENT RESOLUTIONS: {total_resolved}")
            for conf in ("high", "medium", "low"):
                if conf in by_confidence:
                    lines.append(f"   {conf}: {by_confidence[conf]}")
            for st, cnt in sorted(by_status.items()):
                lines.append(f"   status={st}: {cnt}")

            remaining = total - total_resolved
            lines.append(f"\n NON TRAITES: {remaining}")
            coverage = total_resolved / total * 100 if total > 0 else 0
            lines.append(f" COUVERTURE AGENT: {coverage:.1f}%")
            lines.append(f" PROPOSED RESTANTS: {proposed}")
            lines.append(f" PARTY_FINAL ABSENT: {missing_party}")
            lines.append(f" MEDIUM/HIGH SANS PARTY_FINAL: {medium_high_missing_party}")
            lines.append(f" LOW A 1 EVIDENCE: {low_one_evidence}")
            lines.append(f" LOW A 1 EVIDENCE PARTY ONLY: {low_one_party_only}")

            # Auto-valid confirmations
            confirmed = con.execute(
                """SELECT COUNT(*) c FROM agent_resolutions a
                   JOIN gold_service_active g ON g.service_id = a.service_id
                   WHERE g.match_state = 'auto_valid'"""
            ).fetchone()["c"]
            lines.append(f"\n AUTO-VALIDES CONFIRMES PAR AGENT: {confirmed}/{auto_valid}")
        else:
            lines.append(f"\n AGENT RESOLUTIONS: 0 (tables non creees)")
            lines.append(f" NON TRAITES: {total}")

        # Coverage by axis
        lines.append("\n COUVERTURE PAR AXE:")
        axes = [
            ("Site matche", "SELECT COUNT(DISTINCT service_id) c FROM service_endpoint WHERE matched_site_id IS NOT NULL"),
            ("Support reseau", "SELECT COUNT(DISTINCT service_id) c FROM service_support_reseau"),
            ("Route optique", "SELECT COUNT(DISTINCT service_id) c FROM service_support_optique WHERE support_type = 'route'"),
            ("Lease optique", "SELECT COUNT(DISTINCT service_id) c FROM service_support_optique WHERE support_type = 'lease'"),
            ("Party final", f"SELECT COUNT(DISTINCT service_id) c FROM service_party WHERE {_party_role_query()}"),
        ]
        for label, sql in axes:
            try:
                cnt = con.execute(sql).fetchone()["c"]
                lines.append(f"   {label}: {cnt}/{total}")
            except Exception:
                lines.append(f"   {label}: N/A")

        # By nature
        lines.append("\n PAR NATURE DE SERVICE:")
        natures = con.execute(
            "SELECT nature_service, COUNT(*) c FROM service_master_active "
            "GROUP BY nature_service ORDER BY c DESC"
        ).fetchall()
        for r in natures:
            lines.append(f"   {r['nature_service']}: {r['c']}")

        # Focus-specific sections
        if focus:
            lines.append(f"\n{'=' * 64}")
            lines.append(f" FOCUS: {focus}")
            lines.append("=" * 64)

            if focus.startswith("client:"):
                client = focus[7:]
                lines.extend(_focus_client(con, client))
            elif focus.startswith("nature:"):
                nature = focus[7:]
                lines.extend(_focus_nature(con, nature))
            elif focus == "unresolved":
                lines.extend(_focus_unresolved(con))
            elif focus == "auto_valid":
                lines.extend(_focus_auto_valid(con))
            elif focus == "party_gaps":
                lines.extend(_focus_party_gaps(con))

        # Top clients remaining
        lines.append(f"\n TOP CLIENTS RESTANTS:")
        if agent_table_exists:
            top_clients = con.execute(
                """SELECT s.principal_client, COUNT(*) c
                   FROM service_master_active s
                   LEFT JOIN agent_resolutions a ON a.service_id = s.service_id
                   WHERE a.service_id IS NULL
                   GROUP BY s.principal_client
                   ORDER BY c DESC LIMIT 10"""
            ).fetchall()
        else:
            top_clients = con.execute(
                """SELECT principal_client, COUNT(*) c FROM service_master_active
                   GROUP BY principal_client ORDER BY c DESC LIMIT 10"""
            ).fetchall()

        for r in top_clients:
            lines.append(f"   {r['principal_client']}: {r['c']}")

        lines.append("")
        lines.append("=" * 64)

        return "\n".join(lines)
    finally:
        con.close()


def _focus_client(con: sqlite3.Connection, client: str) -> list[str]:
    lines: list[str] = []
    services = con.execute(
        "SELECT service_id, nature_service, principal_offer, endpoint_a_raw, endpoint_z_raw "
        "FROM service_master_active WHERE principal_client LIKE ? LIMIT 50",
        (f"%{client}%",),
    ).fetchall()
    lines.append(f" {len(services)} services pour client contenant '{client}':")
    for s in services:
        lines.append(
            f"   {s['service_id']} | {s['nature_service']} | "
            f"{s['principal_offer'] or '-'} | {s['endpoint_a_raw'] or '-'} -> {s['endpoint_z_raw'] or '-'}"
        )
    return lines


def _focus_nature(con: sqlite3.Connection, nature: str) -> list[str]:
    lines: list[str] = []
    services = con.execute(
        "SELECT service_id, principal_client, principal_offer "
        "FROM service_master_active WHERE nature_service LIKE ? LIMIT 50",
        (f"%{nature}%",),
    ).fetchall()
    lines.append(f" {len(services)} services nature contenant '{nature}':")
    for s in services:
        lines.append(f"   {s['service_id']} | {s['principal_client']} | {s['principal_offer'] or '-'}")
    return lines


def _focus_unresolved(con: sqlite3.Connection) -> list[str]:
    lines: list[str] = []
    # Check if agent_resolutions exists
    has_agent = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='agent_resolutions'"
    ).fetchone()

    if has_agent:
        unresolved = con.execute(
            """SELECT s.service_id, s.principal_client, s.nature_service
               FROM service_master_active s
               LEFT JOIN agent_resolutions a ON a.service_id = s.service_id
               WHERE a.service_id IS NULL
               ORDER BY s.principal_client, s.nature_service LIMIT 50"""
        ).fetchall()
    else:
        unresolved = con.execute(
            "SELECT service_id, principal_client, nature_service "
            "FROM service_master_active ORDER BY principal_client LIMIT 50"
        ).fetchall()

    lines.append(f" {len(unresolved)} services non resolus (max 50):")
    for s in unresolved:
        lines.append(f"   {s['service_id']} | {s['principal_client']} | {s['nature_service']}")
    return lines


def _focus_auto_valid(con: sqlite3.Connection) -> list[str]:
    lines: list[str] = []
    auto = con.execute(
        """SELECT g.service_id, s.principal_client, s.nature_service,
                  g.confidence_band, g.match_state
           FROM gold_service_active g
           JOIN service_master_active s ON s.service_id = g.service_id
           WHERE g.match_state = 'auto_valid'
           ORDER BY s.principal_client"""
    ).fetchall()

    lines.append(f" {len(auto)} services auto-valides:")
    for s in auto:
        # Check if agent has also resolved
        has_agent = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='agent_resolutions'"
        ).fetchone()
        agent_status = "-"
        if has_agent:
            ar = con.execute(
                "SELECT confidence, status FROM agent_resolutions WHERE service_id = ?",
                (s["service_id"],),
            ).fetchone()
            if ar:
                agent_status = f"{ar['confidence']}/{ar['status']}"

        lines.append(
            f"   {s['service_id']} | {s['principal_client']} | "
            f"{s['nature_service']} | pipeline={s['confidence_band']} | agent={agent_status}"
        )
    return lines


def _focus_party_gaps(con: sqlite3.Connection) -> list[str]:
    lines: list[str] = []
    rows = con.execute(
        """
        SELECT ar.service_id, ar.confidence, ar.status,
               sm.principal_client, sm.client_final,
               EXISTS(
                   SELECT 1 FROM service_party sp
                   WHERE sp.service_id = ar.service_id AND sp.role_name = 'final_party' AND sp.party_id IS NOT NULL
               ) AS has_pipeline_final_party,
               EXISTS(
                   SELECT 1 FROM party_alias pa
                   WHERE pa.alias_value = sm.principal_client
               ) AS has_principal_alias
        FROM agent_resolutions ar
        JOIN service_master_active sm ON sm.service_id = ar.service_id
        WHERE ar.party_final_id IS NULL OR TRIM(ar.party_final_id) = ''
        ORDER BY has_pipeline_final_party DESC, ar.confidence, ar.status, ar.service_id
        LIMIT 50
        """
    ).fetchall()
    lines.append(f" {len(rows)} services avec gap party (max 50):")
    for row in rows:
        reasons = []
        if row["has_pipeline_final_party"]:
            reasons.append("pipeline_final_party")
        if row["client_final"]:
            reasons.append("client_final_raw")
        if row["has_principal_alias"]:
            reasons.append("principal_alias")
        reason_text = ",".join(reasons) if reasons else "no_direct_candidate"
        lines.append(
            f"   {row['service_id']} | {row['principal_client']} | "
            f"{row['confidence']}/{_normalized_status(row['status'])} | {reason_text}"
        )
    alias_direct_matches = _count_alias_direct_matches(con)
    lines.append(f" alias_matches_principal_client: {alias_direct_matches}")
    return lines


@tool(
    "get_review_queue_summary",
    "Resume de la review queue : review_types groupes par nature_service et "
    "principal_client avec le nombre de services non resolus. "
    "Permet de prioriser les lots a traiter.",
    {},
)
async def get_review_queue_summary(args: dict[str, Any]) -> dict[str, Any]:
    con = _connect()
    try:
        # Check if review queue table exists
        has_queue = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='service_review_queue'"
        ).fetchone()
        if not has_queue:
            return _text("Table service_review_queue introuvable.")

        has_agent = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='agent_resolutions'"
        ).fetchone()

        if has_agent:
            rows = con.execute("""
                SELECT s.nature_service, s.principal_client, q.review_type, COUNT(DISTINCT q.service_id) AS cnt
                FROM service_review_queue q
                JOIN service_master_active s ON s.service_id = q.service_id
                LEFT JOIN agent_resolutions a ON a.service_id = q.service_id
                WHERE a.service_id IS NULL
                GROUP BY s.nature_service, s.principal_client, q.review_type
                ORDER BY cnt DESC, s.nature_service, s.principal_client
            """).fetchall()
        else:
            rows = con.execute("""
                SELECT s.nature_service, s.principal_client, q.review_type, COUNT(DISTINCT q.service_id) AS cnt
                FROM service_review_queue q
                JOIN service_master_active s ON s.service_id = q.service_id
                GROUP BY s.nature_service, s.principal_client, q.review_type
                ORDER BY cnt DESC, s.nature_service, s.principal_client
            """).fetchall()

        if not rows:
            return _text("Aucun service en attente de review.")

        lines = [
            "nature_service | principal_client | review_type | services_non_resolus",
            "--- | --- | --- | ---",
        ]
        total = 0
        for r in rows:
            lines.append(f"{r['nature_service']} | {r['principal_client']} | {r['review_type']} | {r['cnt']}")
            total += r["cnt"]

        return _text(
            f"**{total} items de review** ({len(rows)} groupes)\n\n" + "\n".join(lines)
        )
    except Exception as e:
        return _text(f"ERROR: {e}")
    finally:
        con.close()


@tool(
    "reconciliation_scorecard",
    "Dashboard temps reel sur les 575 services : agent-resolus "
    "(high/medium/low) vs non traites, comparaison avec les auto-valides "
    "du pipeline, couverture par axe (site, reseau, optique, party), "
    "top clients restants. "
    "Focus optionnel: 'client:ADISTA', 'nature:Lan To Lan', "
    "'unresolved', 'auto_valid', 'party_gaps'.",
    {"focus": str},
)
async def reconciliation_scorecard(args: dict[str, Any]) -> dict[str, Any]:
    focus = args.get("focus", "").strip() or None

    if _db_path is None or not _db_path.exists():
        return _text(f"Database not found: {_db_path}")

    try:
        result = compute_scorecard(_db_path, focus=focus)
        return _text(result)
    except Exception as e:
        return _text(f"ERROR computing scorecard: {e}")
