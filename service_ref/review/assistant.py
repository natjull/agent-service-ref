from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

from service_ref.config import ReviewConfig
from service_ref.review.batching import fetch_service_context, load_review_batches
from service_ref.review.prompts import SYSTEM_PROMPT, render_batch_prompt


def _heuristic_suggestion(service: dict[str, object], context: dict[str, object]) -> dict[str, object]:
    review_types = {item["review_type"] for item in context["review_items"]}
    suggestion = {
        "service_id": service["service_id"],
        "proposed_action": "escalate",
        "target_table": "",
        "target_key": "",
        "target_value": "",
        "confidence": "low",
        "justification": "No deterministic offline suggestion available; analyst review required.",
        "evidence_keys": [evidence["source_key"] for evidence in context["evidences"][:3]],
        "unsafe_if": "Applied without analyst validation.",
        "requires_human_check": True,
    }

    if "missing_final_party" in review_types and service.get("client_final"):
        suggestion.update(
            {
                "proposed_action": "override_party_alias",
                "target_table": "override_party_alias",
                "target_key": str(service["client_final"]),
                "target_value": str(service["client_final"]),
                "confidence": "medium",
                "justification": "Client final is present in LEA but not resolved in party master; propose analyst-reviewed alias override.",
            }
        )
    elif "missing_site_z" in review_types:
        top_site = next((site for site in context["top_sites"] if site["matched_site_id"]), None)
        if top_site:
            suggestion.update(
                {
                    "proposed_action": "override_site_alias",
                    "target_table": "override_site_alias",
                    "target_key": str(top_site["raw_value"]),
                    "target_value": str(top_site["matched_site_id"]),
                    "confidence": "medium" if (top_site["score"] or 0) >= 60 else "low",
                    "justification": "A site candidate already exists in the endpoint matching output; analyst may promote it to a stable alias override.",
                    "unsafe_if": "Another Hubsite shares the same alias or city token.",
                }
            )
    return suggestion


def run(cfg: ReviewConfig) -> dict[str, int]:
    cfg.out_dir.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(cfg.db_path)
    batches = load_review_batches(con, max_services=cfg.max_services)

    prompt_packs = []
    suggestions = []
    for batch in batches:
        services_payload = []
        for service in batch["services"]:
            context = fetch_service_context(con, str(service["service_id"]))
            services_payload.append({**service, **context})
            suggestions.append(_heuristic_suggestion(service, context))
        prompt_batch = {**batch, "services_payload": json.dumps(services_payload, ensure_ascii=True, sort_keys=True, indent=2)}
        prompt_packs.append(
            {
                "batch_id": batch["batch_id"],
                "system_prompt": SYSTEM_PROMPT,
                "user_prompt": render_batch_prompt(prompt_batch),
                "dry_run": cfg.dry_run,
                "model": cfg.model,
            }
        )

    json_path = cfg.out_dir / "review_suggestions.json"
    csv_path = cfg.out_dir / "review_suggestions.csv"
    prompt_path = cfg.out_dir / "review_prompt_packs.json"

    json_path.write_text(json.dumps(suggestions, ensure_ascii=True, sort_keys=True, indent=2), encoding="utf-8")
    prompt_path.write_text(json.dumps(prompt_packs, ensure_ascii=True, sort_keys=True, indent=2), encoding="utf-8")

    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "service_id",
                "proposed_action",
                "target_table",
                "target_key",
                "target_value",
                "confidence",
                "justification",
                "evidence_keys",
                "unsafe_if",
                "requires_human_check",
            ],
        )
        writer.writeheader()
        writer.writerows(suggestions)

    con.close()
    return {
        "batches": len(batches),
        "suggestions": len(suggestions),
        "prompt_packs": len(prompt_packs),
    }
