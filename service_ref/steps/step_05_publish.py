from __future__ import annotations

from service_ref import build_service_referential as legacy
from service_ref.config import BuildConfig, apply_runtime_config
from service_ref.lib.db import connect


def run(cfg: BuildConfig) -> dict[str, int]:
    apply_runtime_config(cfg)
    cfg.ensure_output_dir()
    con = connect(cfg.db_path)
    legacy.export_outputs(con)
    legacy.build_report(con)
    stats = {
        "service_master_active_csv": 1 if (cfg.out_dir / "service_master_active.csv").exists() else 0,
        "service_facturable_final_csv": 1 if (cfg.out_dir / "service_facturable_final.csv").exists() else 0,
        "service_match_evidence_csv": 1 if (cfg.out_dir / "service_match_evidence.csv").exists() else 0,
        "service_review_queue_csv": 1 if (cfg.out_dir / "service_review_queue.csv").exists() else 0,
        "service_referential_report_md": 1 if (cfg.out_dir / "service_referential_report.md").exists() else 0,
    }
    con.close()
    return stats
