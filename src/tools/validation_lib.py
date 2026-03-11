"""Deterministic validation helpers for service resolutions."""

from __future__ import annotations

import re
import sqlite3
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass


@dataclass
class ValidationResult:
    passed: bool
    detail: str
    score: int = 0


# ---------------------------------------------------------------------------
# Inlined scoring helpers (from service_ref.build_service_referential)
# to avoid importing the heavy fiona/geopandas dependency chain.
# ---------------------------------------------------------------------------

_BUSINESS_STOPWORDS = {
    "CLIENT",
    "CLIENTS",
    "LAN2LAN",
    "L2L",
    "TRUNK",
    "VERS",
    "CPE",
    "DSP",
    "VLAN",
    "POP",
    "SITE",
    "PORT",
    "ACCES",
    "ACCESS",
    "TRANSPORT",
    "COLLECTE",
    "SERVICE",
    "SERVICES",
    "SHUT",
    "TEMPORAIRE",
    "NE",
    "PAS",
    "CPEDSP",
    "CLIENTLAN2LAN",
    "CLIENTL2L",
}


def _norm_text(value: object) -> str:
    text = "" if value is None else str(value)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.upper()
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _clean_business_label(value: object) -> str:
    label = _norm_text(value)
    if not label:
        return ""
    label = re.sub(r"\bTOIP\s*\d+\b", " ", label)
    label = re.sub(r"\b(?:OPE|PE)\s*\d+\b", " ", label)
    label = re.sub(r"\bL2L\s*\d+\b", " ", label)
    label = re.sub(r"\b\d{3,5}\b", " ", label)
    tokens = [token for token in label.split() if token not in _BUSINESS_STOPWORDS]
    return " ".join(tokens).strip()


def _business_tokens(value: object) -> set[str]:
    return {token for token in _clean_business_label(value).split() if len(token) >= 4}


def _score_label_match(seeds: Iterable[str], candidate: str) -> int:
    candidate_clean = _clean_business_label(candidate)
    if not candidate_clean:
        return 0
    candidate_tokens = _business_tokens(candidate_clean)
    best_score = 0
    for seed in seeds:
        seed_clean = _clean_business_label(seed)
        if not seed_clean:
            continue
        if seed_clean == candidate_clean:
            best_score = max(best_score, 96)
            continue
        if seed_clean in candidate_clean or candidate_clean in seed_clean:
            overlap = len(_business_tokens(seed_clean) & candidate_tokens)
            if overlap >= 2:
                best_score = max(best_score, 90)
                continue
        overlap = len(_business_tokens(seed_clean) & candidate_tokens)
        if overlap >= 3:
            best_score = max(best_score, min(88, 18 * overlap + 20))
        elif overlap == 2:
            best_score = max(best_score, 72)
    return best_score


# ---------------------------------------------------------------------------
# Validation functions
# ---------------------------------------------------------------------------

_POP_RE = re.compile(r"^([a-z]+\d+)-", re.IGNORECASE)

_SITE_SCORE_THRESHOLD = 72


def validate_site(con: sqlite3.Connection, site_value: str) -> ValidationResult:
    """Check that site_value matches a ref_sites entry with score >= 72."""
    if not site_value:
        return ValidationResult(passed=False, detail="empty site value", score=0)

    # Exact match first
    row = con.execute(
        "SELECT site_id, reference FROM ref_sites "
        "WHERE site_id = ? OR normalized_reference = ?",
        (site_value, site_value),
    ).fetchone()
    if row:
        return ValidationResult(
            passed=True,
            detail=f"exact match: {row['reference']} (site_id={row['site_id']})",
            score=100,
        )

    # Fuzzy match via score_label_match
    candidates = con.execute(
        "SELECT site_id, reference FROM ref_sites WHERE reference IS NOT NULL"
    ).fetchall()

    best_score = 0
    best_ref = ""
    best_id = ""
    for c in candidates:
        s = _score_label_match([site_value], c["reference"])
        if s > best_score:
            best_score = s
            best_ref = c["reference"]
            best_id = c["site_id"]

    if best_score >= _SITE_SCORE_THRESHOLD:
        return ValidationResult(
            passed=True,
            detail=f"fuzzy match: {best_ref} (site_id={best_id}, score={best_score})",
            score=best_score,
        )

    return ValidationResult(
        passed=False,
        detail=f"no match in ref_sites (best score={best_score}, ref={best_ref!r})",
        score=best_score,
    )


def validate_device_pop(
    con: sqlite3.Connection, device_name: str, site_a: str, site_z: str
) -> ValidationResult:
    """Check that the POP extracted from device hostname is geographically linked to a site."""
    if not device_name:
        return ValidationResult(passed=False, detail="empty device_name", score=0)

    m = _POP_RE.match(device_name)
    if not m:
        return ValidationResult(
            passed=False,
            detail=f"cannot extract POP from '{device_name}' (expected pattern like 'abc1-...')",
            score=0,
        )

    pop_code = m.group(1).upper()

    # Check if any site reference contains the POP code
    sites_to_check = [s for s in (site_a, site_z) if s]
    if not sites_to_check:
        return ValidationResult(
            passed=False,
            detail=f"POP={pop_code} but no sites provided to verify against",
            score=0,
        )

    # Look for the POP in ref_sites via reference or site_id
    pop_site = con.execute(
        "SELECT site_id, reference FROM ref_sites "
        "WHERE UPPER(site_id) LIKE ? OR UPPER(reference) LIKE ? LIMIT 1",
        (f"%{pop_code}%", f"%{pop_code}%"),
    ).fetchone()

    if not pop_site:
        return ValidationResult(
            passed=False,
            detail=f"POP '{pop_code}' not found in ref_sites",
            score=0,
        )

    # Verify POP site is one of the resolution sites
    pop_site_id = pop_site["site_id"]
    for site_val in sites_to_check:
        if pop_code.lower() in site_val.lower() or pop_site_id == site_val:
            return ValidationResult(
                passed=True,
                detail=f"POP '{pop_code}' matches site '{site_val}' (ref: {pop_site['reference']})",
                score=85,
            )

    return ValidationResult(
        passed=False,
        detail=f"POP '{pop_code}' (site_id={pop_site_id}) not linked to sites {sites_to_check}",
        score=30,
    )


def validate_route_endpoints(
    con: sqlite3.Connection, route_ref: str, site_a: str, site_z: str
) -> ValidationResult:
    """Check that route matches terminal/passage sites via parcours data."""
    if not route_ref:
        return ValidationResult(passed=False, detail="empty route_ref", score=0)

    # Resolve route_id from route_ref
    route = con.execute(
        "SELECT route_id FROM ref_routes WHERE route_id = ? OR route_ref LIKE ? LIMIT 1",
        (route_ref, f"%{route_ref}%"),
    ).fetchone()

    if not route:
        return ValidationResult(
            passed=False,
            detail=f"route '{route_ref}' not found in ref_routes",
            score=0,
        )

    route_id = route["route_id"]

    # Check parcours view/table exists
    has_view = con.execute(
        "SELECT name FROM sqlite_master WHERE name='v_route_endpoint_sites' AND type='view'"
    ).fetchone()
    has_table = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='ref_route_parcours'"
    ).fetchone()
    if not has_table:
        return ValidationResult(
            passed=True,
            detail=f"route '{route_ref}' exists (ref_route_parcours table not available for endpoint check)",
            score=60,
        )

    terminal_stops = (
        con.execute(
            "SELECT site_label AS site, step_type FROM v_route_endpoint_sites WHERE route_id = ? ORDER BY step_no",
            (route_id,),
        ).fetchall()
        if has_view
        else []
    )
    stops = con.execute(
        "SELECT site, step_type FROM ref_route_parcours WHERE route_id = ? ORDER BY step_no, rowid",
        (route_id,),
    ).fetchall()

    if not stops:
        return ValidationResult(
            passed=True,
            detail=f"route '{route_ref}' exists but no parcours data",
            score=60,
        )

    def _has_match(site_val: str, rows: list[sqlite3.Row]) -> bool:
        candidate = _norm_text(site_val)
        if not candidate:
            return False
        for row in rows:
            current = _norm_text(row["site"])
            if current and (candidate in current or current in candidate):
                return True
        return False

    endpoint_matches = []
    passage_matches = []
    for label, site_val in (("site_a", site_a), ("site_z", site_z)):
        if not site_val:
            continue
        if _has_match(site_val, terminal_stops):
            endpoint_matches.append(label)
        elif _has_match(site_val, stops):
            passage_matches.append(label)

    if len(endpoint_matches) == 2:
        return ValidationResult(
            passed=True,
            detail=f"route '{route_ref}' has terminal endpoints matching site_a and site_z",
            score=97,
        )
    if len(endpoint_matches) == 1 and len(endpoint_matches) + len(passage_matches) == 2:
        return ValidationResult(
            passed=True,
            detail=f"route '{route_ref}' matches one terminal endpoint and one passage site",
            score=78,
        )
    if len(endpoint_matches) == 1:
        return ValidationResult(
            passed=True,
            detail=f"route '{route_ref}' matches terminal endpoint for {endpoint_matches[0]} only",
            score=72,
        )
    if len(passage_matches) == 2:
        return ValidationResult(
            passed=True,
            detail=f"route '{route_ref}' passes through both site_a and site_z but without terminal confirmation",
            score=60,
        )
    if len(passage_matches) == 1:
        return ValidationResult(
            passed=True,
            detail=f"route '{route_ref}' passes through {passage_matches[0]} only",
            score=45,
        )

    stop_sites = [s["site"] for s in stops]
    return ValidationResult(
        passed=False,
        detail=f"route '{route_ref}' stops ({stop_sites}) don't match site_a='{site_a}' / site_z='{site_z}'",
        score=20,
    )
