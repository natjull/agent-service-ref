from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT_DIR = ROOT / "service_ref" / "output"
DEFAULT_DB_PATH = DEFAULT_OUT_DIR / "service_referential.sqlite"
DEFAULT_LEA_PATH = ROOT / "6-3_20260203_Suivi_Contrats_LEA.xlsx"
DEFAULT_GDB_ZIP_PATH = ROOT / "GDB_TeloiseV3 (1).zip"
DEFAULT_SWAG_PATH = ROOT / "unzipped_equip" / "Export inventaire SWAG.xlsx"
DEFAULT_CPE_PATH = ROOT / "unzipped_equip" / "Inventaire CPE Teloise Janv26.xlsx"
DEFAULT_CONFIG_DIR = ROOT / "unzipped_equip"

ROLE_BY_OFFER = {
    "fibre-longue distance-iru": ("IRU FON", "principal"),
    "fibre-longue distance-maintenance": ("IRU FON", "maintenance"),
    "fibre-metro-iru": ("IRU FON", "principal"),
    "fibre-metro-maintenance": ("IRU FON", "maintenance"),
    "fibre-longue distance-location": ("Location FON", "principal"),
    "fibre-metro-location": ("Location FON", "principal"),
    "fibre-metro-location escomptee": ("Location FON", "principal"),
    "lien ethernet - 1-100 mbits": ("Lan To Lan", "principal"),
    "lien sdh - e1 (2 mbits)": ("Lan To Lan", "principal"),
    "netcenter lan - divers": ("Lan To Lan", "annexe"),
    "netcenter lan - extra works": ("Lan To Lan", "frais_acces"),
    "emplacement baie - 48v - 1, 3 ou 5 ans": ("Hebergement", "principal"),
    "netcenter baie - divers": ("Hebergement", "annexe"),
    "fibre - extra works": ("Hebergement", "frais_acces"),
}


@dataclass(slots=True)
class BuildConfig:
    root: Path = ROOT
    out_dir: Path = DEFAULT_OUT_DIR
    db_path: Path = DEFAULT_DB_PATH
    lea_path: Path = DEFAULT_LEA_PATH
    gdb_zip_path: Path = DEFAULT_GDB_ZIP_PATH
    swag_path: Path = DEFAULT_SWAG_PATH
    cpe_path: Path = DEFAULT_CPE_PATH
    config_dir: Path = DEFAULT_CONFIG_DIR
    verbose: bool = False

    @property
    def gdb_uri(self) -> str:
        return f"zip://{self.gdb_zip_path.as_posix()}!TELOISE_TELOISE_20250610-130725.gdb"

    def ensure_output_dir(self) -> None:
        self.out_dir.mkdir(parents=True, exist_ok=True)


@dataclass(slots=True)
class ReviewConfig:
    out_dir: Path
    db_path: Path
    max_services: int | None = None
    dry_run: bool = True
    model: str = "claude-sonnet"


def apply_runtime_config(cfg: BuildConfig) -> None:
    from service_ref import build_service_referential as legacy

    legacy.ROOT = cfg.root
    legacy.OUT_DIR = cfg.out_dir
    legacy.DB_PATH = cfg.db_path
    legacy.LEA_PATH = cfg.lea_path
    legacy.GDB_ZIP_PATH = cfg.gdb_zip_path
    legacy.SWAG_PATH = cfg.swag_path
    legacy.CPE_PATH = cfg.cpe_path
    legacy.CONFIG_DIR = cfg.config_dir
    legacy.GDB_URI = cfg.gdb_uri
