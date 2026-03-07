"""Benchmark helpers for comparing service-ref agent models on the same lot."""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from .batch import batch_run


@dataclass
class BenchmarkMetrics:
    model: str
    duration_seconds: float
    num_turns: int
    total_cost_usd: float
    services_targeted: int
    services_touched: int
    services_proposed: int
    services_validated: int
    services_needs_review: int
    services_rejected: int
    confidence_high: int
    confidence_medium: int
    confidence_low: int
    party_final_coverage: float
    party_final_gaps: int


def _connect(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    return con


def _table_columns(con: sqlite3.Connection, table_name: str) -> list[str]:
    rows = con.execute(f'PRAGMA table_info("{table_name}")').fetchall()
    return [row["name"] for row in rows]


def _table_exists(con: sqlite3.Connection, table_name: str) -> bool:
    row = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _fetch_service_ids(db_path: Path, lot_sql: str) -> list[str]:
    con = _connect(db_path)
    try:
        cursor = con.execute(lot_sql)
        if cursor.description is None or len(cursor.description) != 1:
            raise ValueError("lot_sql must return exactly one column named service_id.")
        column_name = cursor.description[0][0]
        if column_name != "service_id":
            raise ValueError("lot_sql must return exactly one column named service_id.")

        seen: set[str] = set()
        service_ids: list[str] = []
        for (service_id,) in cursor.fetchall():
            if service_id is None:
                continue
            normalized = str(service_id).strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            service_ids.append(normalized)

        if not service_ids:
            raise ValueError("lot_sql returned no service_id rows.")
        return service_ids
    finally:
        con.close()


def _build_benchmark_prompt(service_ids: list[str]) -> str:
    lines = [
        f"Traite uniquement ce lot de {len(service_ids)} services.",
        "Utilise le workflow nominal du projet et termine sans laisser de resolution en proposed.",
        "Ne traite aucun autre service en dehors de cette liste.",
        "",
        "Services cibles:",
    ]
    lines.extend(f"- {service_id}" for service_id in service_ids)
    return "\n".join(lines)


def _backup_agent_tables(db_path: Path) -> dict[str, list[tuple]]:
    con = _connect(db_path)
    try:
        backup: dict[str, list[tuple]] = {}
        for table_name in ("agent_resolutions", "agent_evidence"):
            backup[table_name] = list(
                con.execute(f'SELECT * FROM "{table_name}"').fetchall()
            ) if _table_exists(con, table_name) else []
        return backup
    finally:
        con.close()


def _clear_agent_tables(db_path: Path) -> None:
    con = _connect(db_path)
    try:
        if _table_exists(con, "agent_evidence"):
            con.execute("DELETE FROM agent_evidence")
        if _table_exists(con, "agent_resolutions"):
            con.execute("DELETE FROM agent_resolutions")
        con.commit()
    finally:
        con.close()


def _restore_agent_tables(db_path: Path, backup: dict[str, list[tuple]]) -> None:
    con = _connect(db_path)
    try:
        if _table_exists(con, "agent_evidence"):
            con.execute("DELETE FROM agent_evidence")
        if _table_exists(con, "agent_resolutions"):
            con.execute("DELETE FROM agent_resolutions")
        for table_name in ("agent_resolutions", "agent_evidence"):
            rows = backup.get(table_name, [])
            if not _table_exists(con, table_name):
                continue
            if not rows:
                continue
            columns = _table_columns(con, table_name)
            placeholders = ", ".join("?" for _ in columns)
            quoted_columns = ", ".join(f'"{column}"' for column in columns)
            con.executemany(
                f'INSERT INTO "{table_name}" ({quoted_columns}) VALUES ({placeholders})',
                rows,
            )
        con.commit()
    finally:
        con.close()


def _latest_resolution_rows(con: sqlite3.Connection, service_ids: list[str]) -> list[sqlite3.Row]:
    if not service_ids:
        return []
    placeholders = ", ".join("?" for _ in service_ids)
    rows = con.execute(
        f"""
        SELECT *
        FROM agent_resolutions
        WHERE service_id IN ({placeholders})
        ORDER BY service_id, created_at DESC, rowid DESC
        """,
        service_ids,
    ).fetchall()
    latest: dict[str, sqlite3.Row] = {}
    for row in rows:
        latest.setdefault(row["service_id"], row)
    return [latest[service_id] for service_id in service_ids if service_id in latest]


def _collect_metrics(
    db_path: Path,
    model: str,
    service_ids: list[str],
    duration_seconds: float,
    total_cost_usd: float,
    num_turns: int,
) -> BenchmarkMetrics:
    con = _connect(db_path)
    try:
        latest_rows = _latest_resolution_rows(con, service_ids)
        targeted = len(service_ids)
        touched = len(latest_rows)

        services_proposed = sum(1 for row in latest_rows if row["status"] == "proposed")
        services_validated = sum(1 for row in latest_rows if row["status"] == "validated")
        services_needs_review = sum(1 for row in latest_rows if row["status"] == "needs_review")
        services_rejected = sum(1 for row in latest_rows if row["status"] == "rejected")
        confidence_high = sum(1 for row in latest_rows if row["confidence"] == "high")
        confidence_medium = sum(1 for row in latest_rows if row["confidence"] == "medium")
        confidence_low = sum(1 for row in latest_rows if row["confidence"] == "low")
        with_party = sum(1 for row in latest_rows if (row["party_final_id"] or "").strip())
        party_final_gaps = targeted - with_party
        coverage = (with_party / targeted * 100.0) if targeted else 0.0

        return BenchmarkMetrics(
            model=model,
            duration_seconds=duration_seconds,
            num_turns=num_turns,
            total_cost_usd=total_cost_usd,
            services_targeted=targeted,
            services_touched=touched,
            services_proposed=services_proposed,
            services_validated=services_validated,
            services_needs_review=services_needs_review,
            services_rejected=services_rejected,
            confidence_high=confidence_high,
            confidence_medium=confidence_medium,
            confidence_low=confidence_low,
            party_final_coverage=coverage,
            party_final_gaps=party_final_gaps,
        )
    finally:
        con.close()


def _print_comparison(metrics: list[BenchmarkMetrics]) -> None:
    headers = [
        ("model", "model"),
        ("services_touched", "touched"),
        ("services_validated", "validated"),
        ("services_needs_review", "needs_review"),
        ("services_rejected", "rejected"),
        ("services_proposed", "proposed"),
        ("party_final_coverage", "party_cov%"),
        ("party_final_gaps", "party_gaps"),
        ("num_turns", "turns"),
        ("total_cost_usd", "cost_usd"),
        ("duration_seconds", "seconds"),
    ]
    widths = {
        label: max(
            len(label),
            *(len(_format_metric(getattr(metric, field), field)) for metric in metrics),
        )
        for field, label in headers
    }

    def render_row(metric: BenchmarkMetrics) -> str:
        parts = []
        for field, label in headers:
            value = _format_metric(getattr(metric, field), field)
            parts.append(value.ljust(widths[label]))
        return " | ".join(parts)

    header_line = " | ".join(label.ljust(widths[label]) for _, label in headers)
    separator = "-+-".join("-" * widths[label] for _, label in headers)
    print(header_line)
    print(separator)
    for metric in metrics:
        print(render_row(metric))


def _format_metric(value: object, field_name: str) -> str:
    if field_name in {"duration_seconds", "total_cost_usd"}:
        return f"{float(value):.3f}"
    if field_name == "party_final_coverage":
        return f"{float(value):.1f}"
    return str(value)


async def run_benchmark(
    lot_sql: str,
    models: list[str],
    workspace: str = ".",
    output_path: str | None = None,
) -> list[BenchmarkMetrics]:
    ws = Path(workspace).resolve()
    db_path = ws / "service_ref" / "output" / "service_referential.sqlite"
    service_ids = _fetch_service_ids(db_path, lot_sql)
    prompt = _build_benchmark_prompt(service_ids)
    backup = _backup_agent_tables(db_path)
    results: list[BenchmarkMetrics] = []

    try:
        for model in models:
            _clear_agent_tables(db_path)
            start = time.perf_counter()
            total_cost_usd, num_turns = await batch_run(
                prompt,
                workspace=str(ws),
                model=model,
            )
            duration_seconds = time.perf_counter() - start
            results.append(
                _collect_metrics(
                    db_path=db_path,
                    model=model,
                    service_ids=service_ids,
                    duration_seconds=duration_seconds,
                    total_cost_usd=total_cost_usd,
                    num_turns=num_turns,
                )
            )
    finally:
        _restore_agent_tables(db_path, backup)

    if output_path:
        output_file = Path(output_path)
        if not output_file.is_absolute():
            output_file = ws / output_file
        output_file.write_text(
            json.dumps([asdict(metric) for metric in results], indent=2, ensure_ascii=True),
            encoding="utf-8",
        )

    _print_comparison(results)
    return results
