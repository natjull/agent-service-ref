from __future__ import annotations

import json
import sqlite3
from collections import defaultdict


def load_review_batches(con: sqlite3.Connection, max_services: int | None = None) -> list[dict[str, object]]:
    query = """
        select g.service_id, s.nature_service, s.principal_client, q.review_type,
               s.principal_offer, s.principal_external_ref, s.endpoint_a_raw,
               s.endpoint_z_raw, s.client_final, g.match_state, g.confidence_band
        from gold_service_active g
        join service_master_active s on s.service_id = g.service_id
        join service_review_queue q on q.service_id = g.service_id
        where g.match_state = 'review_required'
        order by s.nature_service, s.principal_client, g.service_id, q.review_type
    """
    rows = con.execute(query).fetchall()

    service_map: dict[str, dict[str, object]] = {}
    for row in rows:
        service_id = row[0]
        record = service_map.setdefault(
            service_id,
            {
                "service_id": service_id,
                "nature_service": row[1],
                "principal_client": row[2],
                "review_types": [],
                "principal_offer": row[4],
                "principal_external_ref": row[5],
                "endpoint_a_raw": row[6],
                "endpoint_z_raw": row[7],
                "client_final": row[8],
                "match_state": row[9],
                "confidence_band": row[10],
            },
        )
        record["review_types"].append(row[3])

    if max_services is not None:
        limited_ids = list(service_map)[:max_services]
        service_map = {service_id: service_map[service_id] for service_id in limited_ids}

    batches_by_signature: dict[tuple[str, str, str], list[dict[str, object]]] = defaultdict(list)
    for record in service_map.values():
        signature = (
            record["nature_service"],
            record["principal_client"],
            "|".join(sorted(set(record["review_types"]))),
        )
        batches_by_signature[signature].append(record)

    batches: list[dict[str, object]] = []
    for signature, services in batches_by_signature.items():
        nature_service, principal_client, review_signature = signature
        for offset in range(0, len(services), 10):
            batch_services = services[offset : offset + 10]
            batches.append(
                {
                    "batch_id": f"{nature_service}|{principal_client}|{review_signature}|{offset // 10 + 1}",
                    "nature_service": nature_service,
                    "principal_client": principal_client,
                    "review_signature": review_signature,
                    "services": batch_services,
                }
            )
    return batches


def fetch_service_context(con: sqlite3.Connection, service_id: str) -> dict[str, object]:
    evidences = [
        {
            "evidence_type": row[0],
            "rule_name": row[1],
            "score": row[2],
            "source_table": row[3],
            "source_key": row[4],
            "payload": json.loads(row[5]),
        }
        for row in con.execute(
            """
            select evidence_type, rule_name, score, source_table, source_key, payload_json
            from service_match_evidence
            where service_id = ?
            order by score desc, evidence_type, rule_name
            limit 10
            """,
            (service_id,),
        )
    ]
    review_items = [
        {
            "review_type": row[0],
            "severity": row[1],
            "reason": row[2],
            "context": json.loads(row[3]),
        }
        for row in con.execute(
            "select review_type, severity, reason, context_json from service_review_queue where service_id = ? order by severity desc, review_type",
            (service_id,),
        )
    ]
    top_sites = [
        {
            "endpoint_label": row[0],
            "raw_value": row[1],
            "matched_site_id": row[2],
            "matched_site_name": row[3],
            "score": row[4],
            "rule_name": row[5],
        }
        for row in con.execute(
            "select endpoint_label, raw_value, matched_site_id, matched_site_name, score, rule_name from service_endpoint where service_id = ? order by score desc, endpoint_label",
            (service_id,),
        )
    ]
    parties = [
        {
            "role_name": row[0],
            "party_id": row[1],
            "rule_name": row[2],
            "score": row[3],
        }
        for row in con.execute(
            "select role_name, party_id, rule_name, score from service_party where service_id = ? order by score desc, role_name",
            (service_id,),
        )
    ]
    return {
        "review_items": review_items,
        "evidences": evidences,
        "top_sites": top_sites,
        "parties": parties,
    }
