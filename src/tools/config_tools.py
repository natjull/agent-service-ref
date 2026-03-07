"""MCP tools for searching and reading network device configuration files."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

from ..sdk_compat import tool

_config_dir: Path | None = None
_config_available: bool = False
_MAX_RESULTS = 50
_CONTEXT_LINES = 2
_MAX_FILE_LINES = 5000


def configure(config_dir: Path) -> None:
    global _config_dir, _config_available
    _config_dir = Path(config_dir)
    _config_available = _config_dir.exists() and any(_config_dir.rglob("*.txt"))


def _text(content: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": content}]}


def _find_config_files() -> list[Path]:
    """Find all .txt config files in the config directory tree."""
    if _config_dir is None or not _config_dir.exists():
        return []
    return sorted(_config_dir.rglob("*.txt"))


def _detect_vendor(filepath: Path) -> str:
    """Detect vendor from filename or content heuristics."""
    name = filepath.name.lower()
    if "rad" in name:
        return "RAD"
    # Check filename for device naming convention
    if any(x in name for x in ("co-", "sec-", "bas-")):
        return "Cisco"
    if any(x in name for x in ("sw-", "sws-")):
        return "Huawei"
    # For CPE examples
    if any(x in name for x in ("3328", "5328", "5624")):
        return "Huawei"
    return "unknown"


def _extract_device_name(filepath: Path) -> str:
    """Extract device hostname from RANCID-style filename."""
    name = filepath.stem
    # DSP_TELOISE_rancidIP_bea1-co-1.teloise.net_FILTRED -> bea1-co-1
    m = re.search(r"rancidIP_([^.]+)\.teloise", name)
    if m:
        return m.group(1)
    # CPE example files
    return name


@tool(
    "search_configs",
    "Recherche par regex dans les fichiers de configuration reseau "
    "(RANCID Cisco/Huawei, configs CPE). Retourne max 50 resultats avec "
    "2 lignes de contexte. Filtres optionnels par device ou vendor.",
    {"pattern": str, "device_filter": str, "vendor_filter": str},
)
async def search_configs(args: dict[str, Any]) -> dict[str, Any]:
    pattern = args.get("pattern", "").strip()
    if not pattern:
        return _text("ERROR: No search pattern provided.")

    device_filter = args.get("device_filter", "").strip().lower()
    vendor_filter = args.get("vendor_filter", "").strip().lower()

    try:
        regex = re.compile(pattern, re.IGNORECASE)
    except re.error as e:
        return _text(f"Invalid regex: {e}")

    if not _config_available:
        return _text(
            f"ERROR: Aucun fichier de configuration disponible. "
            f"Le repertoire '{_config_dir}' n'existe pas ou ne contient aucun fichier .txt. "
            f"Placer les exports RANCID dans ce repertoire et relancer 'prepare'."
        )

    files = _find_config_files()
    if not files:
        return _text(f"No config files found in {_config_dir}")

    results: list[str] = []
    total_matches = 0

    for filepath in files:
        device = _extract_device_name(filepath)
        vendor = _detect_vendor(filepath)

        # Apply filters
        if device_filter and device_filter not in device.lower():
            continue
        if vendor_filter and vendor_filter not in vendor.lower():
            continue

        try:
            lines = filepath.read_text(encoding="utf-8", errors="replace").splitlines()
        except Exception:
            continue

        for i, line in enumerate(lines):
            if regex.search(line):
                total_matches += 1
                if len(results) < _MAX_RESULTS:
                    # Context window
                    start = max(0, i - _CONTEXT_LINES)
                    end = min(len(lines), i + _CONTEXT_LINES + 1)
                    context = []
                    for j in range(start, end):
                        marker = ">>>" if j == i else "   "
                        context.append(f"{marker} {j+1:4d} | {lines[j]}")

                    results.append(
                        f"**{filepath.name}** (device={device}, vendor={vendor})\n"
                        + "\n".join(context)
                    )

    if not results:
        return _text(f"No matches for pattern `{pattern}` in {len(files)} config files.")

    header = f"**{total_matches} matches** (showing {len(results)}) for `{pattern}`\n"
    return _text(header + "\n\n---\n\n".join(results))


@tool(
    "read_config_file",
    "Lit le contenu complet d'un fichier de configuration reseau. "
    "Le fichier doit etre un .txt situe dans le repertoire de configs.",
    {"file_name": str},
)
async def read_config_file(args: dict[str, Any]) -> dict[str, Any]:
    file_name = args.get("file_name", "").strip()
    if not file_name:
        return _text("ERROR: No file_name provided.")

    if not _config_available:
        return _text(
            f"ERROR: Aucun fichier de configuration disponible. "
            f"Le repertoire '{_config_dir}' n'existe pas ou ne contient aucun fichier .txt."
        )

    if not file_name.endswith(".txt"):
        return _text("ERROR: Only .txt files are allowed.")

    # Find the file in config dir tree
    matches = list(_config_dir.rglob(file_name)) if _config_dir else []
    if not matches:
        # Try partial match
        if _config_dir:
            matches = [f for f in _find_config_files() if file_name.lower() in f.name.lower()]

    if not matches:
        return _text(f"File '{file_name}' not found in config directory.")

    filepath = matches[0]

    # Security: ensure it's within config_dir
    try:
        filepath.resolve().relative_to(_config_dir.resolve())
    except ValueError:
        return _text("ERROR: File is outside the config directory.")

    try:
        lines = filepath.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception as e:
        return _text(f"ERROR reading file: {e}")

    if len(lines) > _MAX_FILE_LINES:
        content = "\n".join(lines[:_MAX_FILE_LINES])
        content += f"\n\n... (truncated at {_MAX_FILE_LINES} lines, file has {len(lines)} lines)"
    else:
        content = "\n".join(lines)

    device = _extract_device_name(filepath)
    vendor = _detect_vendor(filepath)

    return _text(
        f"**{filepath.name}** (device={device}, vendor={vendor}, {len(lines)} lines)\n\n"
        f"```\n{content}\n```"
    )
