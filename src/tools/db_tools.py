"""MCP tools for querying the service referential SQLite database."""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any

from claude_agent_sdk import tool

from service_ref.review.batching import fetch_service_context as _fetch_ctx

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
    "Retourne le contexte complet d'un service en un seul appel : "
    "evidences du pipeline, items de review, top sites matches, parties. "
    "Ideal pour comprendre rapidement un service avant de soumettre une resolution.",
    {"service_id": str},
)
async def fetch_service_context(args: dict[str, Any]) -> dict[str, Any]:
    service_id = args.get("service_id", "").strip()
    if not service_id:
        return _text("ERROR: No service_id provided.")

    con = _connect(read_only=True)
    try:
        ctx = _fetch_ctx(con, service_id)
        return _text(json.dumps(ctx, indent=2, ensure_ascii=False, default=str))
    except Exception as e:
        return _text(f"ERROR: {e}")
    finally:
        con.close()
