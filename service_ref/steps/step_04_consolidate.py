from __future__ import annotations

from service_ref import build_service_referential as legacy
from service_ref.config import BuildConfig, apply_runtime_config
from service_ref.lib.db import connect, delete_from_tables


TABLES = ["service_review_queue", "gold_service_active", "service_facturable_final"]


def run(cfg: BuildConfig) -> dict[str, int]:
    apply_runtime_config(cfg)
    con = connect(cfg.db_path)
    delete_from_tables(con, TABLES)
    legacy.build_publication_views(con)
    legacy.build_facturable_publication(con)
    publication_stats = dict(
        con.execute(
            "select publication_status, count(*) from service_facturable_final group by publication_status"
        ).fetchall()
    )
    stats = {
        "gold_service_active": con.execute("select count(*) from gold_service_active").fetchone()[0],
        "service_review_queue": con.execute("select count(*) from service_review_queue").fetchone()[0],
        "service_facturable_final": con.execute("select count(*) from service_facturable_final").fetchone()[0],
        "published_validated": publication_stats.get("published_validated", 0),
        "published_from_gold": publication_stats.get("published_from_gold", 0),
        "needs_review": publication_stats.get("needs_review", 0),
    }
    con.close()
    return stats
