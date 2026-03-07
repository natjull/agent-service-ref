"""DEPRECATED: This module is superseded by src/prompts/system_prompt.py. Kept for reference only."""

from __future__ import annotations

SYSTEM_PROMPT = """You are assisting a human telecom data analyst.
You do not decide production truth. You analyze unresolved active services and
propose candidate overrides or manual review actions. You must remain cautious,
prefer escalation over unsafe assumptions, and explain every suggestion with the
available evidence.
Output must follow the requested JSON schema exactly.
"""


def render_batch_prompt(batch: dict[str, object]) -> str:
    return (
        "Analyze this telecom service review batch and propose safe candidate overrides.\n\n"
        f"Batch id: {batch['batch_id']}\n"
        f"Nature: {batch['nature_service']}\n"
        f"Principal client: {batch['principal_client']}\n"
        f"Review signature: {batch['review_signature']}\n\n"
        f"Services payload:\n{batch['services_payload']}\n"
    )


SUGGESTION_SCHEMA = {
    "type": "object",
    "properties": {
        "service_id": {"type": "string"},
        "proposed_action": {
            "type": "string",
            "enum": [
                "override_site_alias",
                "override_party_alias",
                "override_service_match",
                "accept_as_is",
                "escalate",
            ],
        },
        "target_table": {"type": "string"},
        "target_key": {"type": "string"},
        "target_value": {"type": "string"},
        "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
        "justification": {"type": "string"},
        "evidence_keys": {"type": "array", "items": {"type": "string"}},
        "unsafe_if": {"type": "string"},
        "requires_human_check": {"type": "boolean"},
    },
    "required": [
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
}
