from __future__ import annotations

from service_ref import build_service_referential as legacy
from service_ref.config import BuildConfig, apply_runtime_config
from service_ref.lib.db import connect


def run(cfg: BuildConfig) -> dict[str, int]:
    apply_runtime_config(cfg)
    cfg.ensure_output_dir()
    if cfg.db_path.exists():
        cfg.db_path.unlink()

    con = connect(cfg.db_path)
    legacy.create_schema(con)
    legacy.load_lea_active(con)
    legacy.load_sites(con)
    legacy.load_routes(con)
    legacy.load_lease_tables(con)
    legacy.load_swag_interfaces(con)
    legacy.load_cpe_inventory(con)
    legacy.load_cpe_configs(con)
    legacy.load_network_text_artifacts(con)
    stats = {
        "lea_active_lines": con.execute("select count(*) from lea_active_lines").fetchone()[0],
        "ref_sites": con.execute("select count(*) from ref_sites").fetchone()[0],
        "ref_routes": con.execute("select count(*) from ref_routes").fetchone()[0],
        "ref_lease_template": con.execute("select count(*) from ref_lease_template").fetchone()[0],
        "ref_swag_interfaces": con.execute("select count(*) from ref_swag_interfaces").fetchone()[0],
        "ref_network_devices": con.execute("select count(*) from ref_network_devices").fetchone()[0],
    }
    con.close()
    return stats
