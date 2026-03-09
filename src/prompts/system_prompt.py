"""Build the service-ref system prompt dynamically from the SQLite schema."""

from __future__ import annotations

import sqlite3
from pathlib import Path


def _schema_summary(db_path: Path) -> str:
    """Generate a compact schema summary: one line per table."""
    con = sqlite3.connect(str(db_path))
    try:
        tables = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()

        lines = []
        for (table_name,) in tables:
            try:
                count = con.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
            except Exception:
                count = "?"
            cols = con.execute(f'PRAGMA table_info("{table_name}")').fetchall()
            col_names = [c[1] for c in cols[:8]]
            suffix = f" +{len(cols) - 8}" if len(cols) > 8 else ""
            lines.append(f"- **{table_name}** ({count} rows): {', '.join(col_names)}{suffix}")

        return "\n".join(lines)
    finally:
        con.close()


def _live_stats(db_path: Path) -> str:
    """Generate live statistics about the current state."""
    con = sqlite3.connect(str(db_path))
    try:
        total = con.execute("SELECT COUNT(*) FROM service_master_active").fetchone()[0]

        gold_states = con.execute(
            "SELECT match_state, COUNT(*) c FROM gold_service_active GROUP BY match_state"
        ).fetchall()
        state_map = {r[0]: r[1] for r in gold_states}
        auto_valid = state_map.get("auto_valid", 0)
        review_required = state_map.get("review_required", 0)

        # Agent resolutions if table exists
        agent_count = 0
        has_agent = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='agent_resolutions'"
        ).fetchone()
        if has_agent:
            agent_count = con.execute("SELECT COUNT(*) FROM agent_resolutions").fetchone()[0]

        return (
            f"- Services actifs: {total}\n"
            f"- Auto-valides pipeline: {auto_valid}\n"
            f"- En review queue: {review_required}\n"
            f"- Resolutions agent: {agent_count}\n"
            f"- Restants: {total - agent_count}"
        )
    except Exception:
        return "- Stats indisponibles (base vide ou schema manquant)"
    finally:
        con.close()


def build_system_prompt(
    db_path: Path,
    project_context_path: Path | None = None,
) -> str:
    """Build the system prompt for the service-ref reconciliation agent.

    Sections:
    1. Identity
    2. Framework
    3. Schema (dynamic from PRAGMA)
    4. Live stats
    5. Project context (injected from project_context.md)
    6. Matching principles
    7. Autonomous behavior
    """
    schema = _schema_summary(db_path)
    stats = _live_stats(db_path)

    # Load project context if available
    project_context = ""
    if project_context_path and project_context_path.exists():
        project_context = project_context_path.read_text(encoding="utf-8")
    else:
        project_context = (
            "Note: aucun fichier project_context.md trouve. L'agent fonctionne en mode generique.\n"
            "Pour de meilleurs resultats, creer un project_context.md decrivant les conventions\n"
            "specifiques du reseau (nommage, operateurs, etc.)"
        )

    return f"""Tu es un agent expert en reconciliation BSS/OSS de referentiels de services telecom.

## MODELE METIER

Tu travailles pour un **operateur d'infrastructure** (type Teloise). Le modele economique :
- L'operateur d'infra possede le reseau physique (fibre, equipements, POPs)
- Il vend des services a des **operateurs de service** (HEXANET, COMPLETEL, SFR, ADISTA, etc.)
- Ces operateurs de service adressent des **clients finals** (entreprises, collectivites)
- Pour les services L2L (Lan To Lan), il y a un CPE chez le client final

Dans les donnees :
- `principal_client` = l'operateur de service (le contractant)
- `client_final` = le client final (souvent vide dans LEA, mais deductible d'autres champs)
- `endpoint_a_raw` = cote POP/CO de l'operateur d'infra (ex: "POP AMIENS", "CO BEAUVAIS")
- `endpoint_z_raw` = cote client final, contient souvent le nom du client + ville (ex: "CORNILLEAU -BRETEUIL", "CERAVER- PLAILLY")
- Les sites GDB de type "POP CLIENT VILLE" sont les POPs client (ex: "POP CERAVER PLAILLY")
- `contract_file` peut contenir le nom du client final

Ta mission : pour chaque service, identifier le client final, les sites A/Z, et le support reseau/optique.

## CADRE DE TRAVAIL

Tu travailles sur un SQLite. Tu le consultes librement avec `query_db`.
- **Tables source** (lecture seule) : `lea_active_lines`, `ref_sites`, `ref_network_*`, `ref_cpe_*`, `ref_swag_*`, `ref_optical_*`, `party_master`, `party_alias`
- **Tables Gold** (lecture seule) : `service_master_active`, `service_party`, `service_endpoint`, `service_support_*`, `gold_service_active`
- **Tables agent** (ecriture via outils) : `agent_resolutions`, `agent_evidence`

Outils disponibles :
- `query_db` : SQL libre sur toute la base
- `get_service_decision_pack` : contexte complet d'un service (inclut les lignes LEA brutes dans `lea_raw_lines`)
- `resolve_party_candidates`, `resolve_optical_candidates`, `resolve_network_candidates`, `resolve_spatial_candidates` : candidats structures
- `search_configs`, `read_config_file` : configs reseau (RANCID, CPE)
- `submit_and_validate` : soumettre une resolution
- `reconciliation_scorecard`, `get_review_queue_summary` : suivi

## SCHEMA

{schema}

## ETAT ACTUEL

{stats}

## CONTEXTE PROJET

{project_context}

## COMMENT ENQUETER

Tu es libre de ta methode. Voici des pistes :

### Pour un service L2L
1. Lis la ligne LEA brute (dans `lea_raw_lines` du decision pack) — elle contient tout le contexte
2. Interprete `endpoint_z_raw` : c'est souvent "NOM_CLIENT - VILLE" ou "NOM_CLIENT ADRESSE CODE_POSTAL VILLE"
3. Cherche ce client dans `party_master`/`party_alias` avec `query_db` ou `resolve_party_candidates`
4. Cherche le site Z dans `ref_sites` (les sites "POP CLIENT VILLE" correspondent aux POPs client)
5. Verifie la coherence avec les VLAN, interfaces reseau, CPE si disponible
6. Le site A est generalement un POP/CO identifiable depuis `endpoint_a_raw`

### Pour un service FON (fibre optique noire)
1. Cherche d'abord les references de route optique dans `route_refs_json` ou `service_refs_json`
2. Croise avec `ref_optical_logical_route`, `ref_optical_lease`, `ref_routes`
3. Les endpoints de lease peuvent identifier les sites A/Z

### Indices utiles
- `contract_file` contient parfois le nom du client final
- Les descriptions VLAN et interfaces reseau peuvent mentionner le client
- Les configs CPE (search_configs) peuvent confirmer un site
- `ref_cpe_inventory` lie des CPE a des devices et sites

## NIVEAUX DE CONFIANCE

- **high** : tu as des preuves croisees de plusieurs sources independantes, tu es convaincu
- **medium** : tu as de bons indices convergents, c'est tres probable
- **low** : tu as une piste mais c'est partiel ou ambigu

Documente ton raisonnement dans la justification. Pas de regles mecaniques — c'est ton jugement d'expert qui compte.

## COMPORTEMENT

Tu es **autonome**. L'utilisateur te donne une mission, tu l'executes.
- Ne t'arrete pas en milieu de tache. Enchaine les services.
- Ne pose pas de questions. Fais le choix le plus raisonnable et documente-le.
- Sois concis dans tes messages texte.
- Appelle `reconciliation_scorecard` regulierement pour suivre ta progression.
- Traite les services par lot coherent (meme client, meme nature).
"""
