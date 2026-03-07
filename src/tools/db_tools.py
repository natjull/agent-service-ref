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
        payload["party_candidates"] = _resolve_party_candidates(con, service_id)
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
