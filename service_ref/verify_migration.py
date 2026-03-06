from __future__ import annotations

import argparse
import hashlib
import sqlite3
from pathlib import Path


def list_tables(con: sqlite3.Connection) -> list[str]:
    return [
        row[0]
        for row in con.execute(
            "select name from sqlite_master where type='table' and name not like 'sqlite_%' order by name"
        )
    ]


def table_rows(con: sqlite3.Connection, table: str) -> list[tuple]:
    columns = [row[1] for row in con.execute(f"pragma table_info({table})")]
    order_clause = ", ".join(columns)
    return con.execute(f"select * from {table} order by {order_clause}").fetchall()


def digest_rows(rows: list[tuple]) -> str:
    hasher = hashlib.sha256()
    for row in rows:
        hasher.update(repr(row).encode("utf-8"))
    return hasher.hexdigest()


def compare_databases(baseline_path: Path, candidate_path: Path) -> list[str]:
    baseline = sqlite3.connect(baseline_path)
    candidate = sqlite3.connect(candidate_path)
    errors: list[str] = []

    baseline_tables = list_tables(baseline)
    candidate_tables = list_tables(candidate)
    if baseline_tables != candidate_tables:
        errors.append(f"Table list mismatch: baseline={baseline_tables}, candidate={candidate_tables}")

    for table in baseline_tables:
        baseline_rows = table_rows(baseline, table)
        candidate_rows = table_rows(candidate, table)
        if len(baseline_rows) != len(candidate_rows):
            errors.append(f"Row count mismatch in {table}: baseline={len(baseline_rows)}, candidate={len(candidate_rows)}")
            continue
        baseline_digest = digest_rows(baseline_rows)
        candidate_digest = digest_rows(candidate_rows)
        if baseline_digest != candidate_digest:
            errors.append(f"Content mismatch in {table}: baseline_digest={baseline_digest}, candidate_digest={candidate_digest}")

    gold_query = "select match_state, count(*) from gold_service_active group by match_state order by match_state"
    if baseline.execute(gold_query).fetchall() != candidate.execute(gold_query).fetchall():
        errors.append("Gold match_state distribution mismatch")

    review_query = "select review_type, count(*) from service_review_queue group by review_type order by review_type"
    if baseline.execute(review_query).fetchall() != candidate.execute(review_query).fetchall():
        errors.append("Review queue distribution mismatch")

    baseline.close()
    candidate.close()
    return errors


def file_digest(path: Path) -> str:
    hasher = hashlib.sha256()
    hasher.update(path.read_bytes())
    return hasher.hexdigest()


def compare_output_dirs(baseline_dir: Path, candidate_dir: Path) -> list[str]:
    errors: list[str] = []
    baseline_files = sorted(
        path.name
        for path in baseline_dir.glob("*")
        if path.is_file() and path.suffix.lower() not in {".sqlite", ".db"}
    )
    candidate_files = sorted(
        path.name
        for path in candidate_dir.glob("*")
        if path.is_file() and path.suffix.lower() not in {".sqlite", ".db"}
    )
    for file_name in baseline_files:
        if file_name not in candidate_files:
            errors.append(f"Missing output file in candidate directory: {file_name}")
            continue
        baseline_path = baseline_dir / file_name
        candidate_path = candidate_dir / file_name
        if file_digest(baseline_path) != file_digest(candidate_path):
            errors.append(f"Output file mismatch: {file_name}")
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--baseline-out-dir", type=Path, default=None)
    parser.add_argument("--candidate-out-dir", type=Path, default=None)
    args = parser.parse_args(argv)

    errors = compare_databases(args.baseline, args.candidate)
    if args.baseline_out_dir and args.candidate_out_dir:
        errors.extend(compare_output_dirs(args.baseline_out_dir, args.candidate_out_dir))
    if errors:
        for error in errors:
            print(error)
        return 1
    print("Migration verification succeeded")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
