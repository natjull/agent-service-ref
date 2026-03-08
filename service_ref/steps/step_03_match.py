from __future__ import annotations

from service_ref import build_service_referential as legacy
from service_ref.config import BuildConfig, apply_runtime_config
from service_ref.lib.db import connect, delete_from_tables


TABLES = [
    "service_bss_line",
    "service_lea_signal",
    "service_spatial_seed",
    "service_spatial_evidence",
    "service_party",
    "service_endpoint",
    "service_support_optique",
    "service_support_reseau",
    "service_match_evidence",
    "service_master_active",
]


def run(cfg: BuildConfig) -> dict[str, int]:
    apply_runtime_config(cfg)
    con = connect(cfg.db_path)
    delete_from_tables(con, TABLES)
    legacy.build_service_master(con)
    legacy.reconcile_services(con)
    stats = {
        "service_master_active": con.execute("select count(*) from service_master_active").fetchone()[0],
        "service_match_evidence": con.execute("select count(*) from service_match_evidence").fetchone()[0],
        "service_lea_signal": con.execute("select count(*) from service_lea_signal").fetchone()[0],
        "service_spatial_seed": con.execute("select count(*) from service_spatial_seed").fetchone()[0],
        "service_spatial_evidence": con.execute("select count(*) from service_spatial_evidence").fetchone()[0],
        "service_support_optique": con.execute("select count(*) from service_support_optique").fetchone()[0],
        "service_support_reseau": con.execute("select count(*) from service_support_reseau").fetchone()[0],
    }
    con.close()
    return stats
