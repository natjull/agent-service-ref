from __future__ import annotations

from service_ref import build_service_referential as legacy
from service_ref.config import BuildConfig, apply_runtime_config
from service_ref.lib.db import connect, delete_from_tables


TABLES = ["party_alias", "party_master"]


def run(cfg: BuildConfig) -> dict[str, int]:
    apply_runtime_config(cfg)
    con = connect(cfg.db_path)
    delete_from_tables(con, TABLES)
    legacy.build_party_master(con)
    stats = {
        "party_master": con.execute("select count(*) from party_master").fetchone()[0],
        "party_alias": con.execute("select count(*) from party_alias").fetchone()[0],
    }
    con.close()
    return stats
