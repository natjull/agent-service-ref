from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

from service_ref import build_service_referential as legacy


create_schema = legacy.create_schema
export_csv = legacy.export_csv
safe_hash = legacy.safe_hash


def connect(db_path: Path) -> sqlite3.Connection:
    return sqlite3.connect(db_path)


def delete_from_tables(con: sqlite3.Connection, tables: Iterable[str]) -> None:
    for table in tables:
        con.execute(f"delete from {table}")
    con.commit()
