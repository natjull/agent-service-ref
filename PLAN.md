# Plan : Agent de réconciliation BSS/OSS TELOISE

## Contexte

Le pipeline modulaire est **déjà en place** (`python -m service_ref run-all`).
Il charge les sources BSS/OSS, normalise, matche par regex/token overlap, et produit
un Gold layer : **575 services, 23 auto-validés, 552 en review queue** (96% non résolus).

Le matching déterministe plafonne. Le problème est fondamentalement du raisonnement
contextuel : noms de clients avec variantes, conventions de nommage implicites,
labels VLAN cryptiques, adresses approximatives.

**Objectif** : construire un agent autonome sur le pattern de `agent-gracethd` — même
architecture (Claude Agent SDK + MCP tools + Claude Max OAuth), pour résoudre les 552
services que le pipeline déterministe ne sait pas traiter.

**Modèle de référence** : https://github.com/natjull/agent-gracethd

**Pré-requis déjà livré** : pipeline modulaire dans `service_ref/` (steps, lib, cli, tests)

---

## Architecture

### Pattern repris de agent-gracethd

| Composant | agent-gracethd | agent-service-ref |
|-----------|---------------|-------------------|
| **MCP tools** | lookup_table_schema, validate_gracethd, conversion_scorecard | query_db, search_configs, reconciliation_scorecard |
| **System prompt** | MCD GraceTHD, topologie, patrons ogr2ogr | Schema SQLite, règles de matching, patterns BSS/OSS |
| **Agent** | Opus, autonome, convertit des données télécom | Opus, autonome, réconcilie BSS/OSS |
| **Scorecard** | Conformité GPKG vs MCD après chaque écriture | Couverture matching + confiance après chaque batch |
| **Cadre de travail** | pipeline/ + output/ + rapport.md | Le SQLite est le pivot, résolutions dans des tables dédiées |
| **Auth** | Claude Max OAuth (pas de clé API) | Idem |

### Structure du projet

Projet séparé qui consomme le SQLite produit par `python -m service_ref run-all`.

```
agent-service-ref/
  pyproject.toml
  src/
    __init__.py
    agent.py                    # Assemblage MCP server + ClaudeSDKClient (calqué sur agent-gracethd)
    cli.py                      # CLI : interactive, batch, prepare
    batch.py                    # Mode batch non-interactif
    tools/
      __init__.py
      db_tools.py               # query_db, list_tables, describe_table
      config_tools.py           # search_configs, read_config_file
      scoring_tools.py          # reconciliation_scorecard
      resolution_tools.py       # submit_resolution, list_resolutions, validate_resolution
    prompts/
      __init__.py
      system_prompt.py          # Prompt système dynamique (schema, stats, règles métier)
  tests/
```

La commande `prepare` du CLI exécute `python -m service_ref run-all` dans le
workspace pour s'assurer que le SQLite est à jour, puis ajoute les tables agent
(agent_resolutions, agent_evidence).

---

## Fichiers de référence

### Pipeline existant (déjà livré)

Le pipeline modulaire dans `service_ref/` produit le SQLite que l'agent consomme.
L'agent ne remplace PAS le pipeline — il travaille sur les 552 services non résolus
après que le pipeline déterministe a fait son travail.

- `service_ref/steps/step_01_load.py` → `step_05_publish.py` — pipeline complet
- `service_ref/lib/` — utilitaires partagés (parsers, scoring, normalize)
- `service_ref/output/service_referential.sqlite` — base produite par le pipeline

### Depuis `agent-gracethd` (pattern réutilisé)

| Fichier source | Adaptation |
|---------------|------------|
| `src/agent.py` | Copié, adapté : server "gracethd" → "service-ref", tools différents |
| `src/cli.py` | Copié, adapté : ajout commande `prepare` |
| `src/batch.py` | Copié quasi tel quel |
| `src/tools/scoring_tools.py` | Pattern repris pour `reconciliation_scorecard` |
| `src/tools/validation_tools.py` | Pattern repris pour `validate_resolution` |
| `src/prompts/system_prompt.py` | Réécrit pour le domaine BSS/OSS |

---

## MCP Tools (détail)

### `query_db` — Requête SQL libre sur le référentiel

L'agent écrit du SQL pour investiguer. Lecture seule sur les tables Bronze/Silver,
lecture/écriture sur les tables de résolution.

```python
@tool("query_db", "Execute une requete SQL sur la base du referentiel de services. "
      "Tables disponibles : lea_active_lines, ref_sites, ref_routes, ref_fiber_lease, "
      "ref_swag_interfaces, ref_cpe_inventory, ref_network_vlans, ref_network_interfaces, "
      "party_master, service_master_active, agent_resolutions, agent_evidence. "
      "Lecture seule sur les tables ref_* et lea_*. Ecriture via submit_resolution.",
      {"sql": str})
async def query_db(args):
    # Exécute la requête, retourne les résultats formatés en table markdown
    # Garde-fou : pas de DROP, DELETE, UPDATE, ALTER sur tables ref_*
```

### `search_configs` — Grep dans les configs RANCID/CPE

```python
@tool("search_configs", "Recherche un pattern dans les fichiers de configuration "
      "reseau (RANCID, CPE Huawei, CPE RAD). Retourne les lignes matchées avec "
      "le nom du fichier et le device associe.",
      {"pattern": str, "device_filter": str, "vendor_filter": str})
async def search_configs(args):
    # Grep dans unzipped_equip/ et unzipped_equip/TELOISE/TELOISE/
    # Filtre optionnel par device (bea1*, crl1*) ou vendor (huawei, cisco, rad)
```

### `reconciliation_scorecard` — Tableau de bord temps réel

Calqué sur `conversion_scorecard` de agent-gracethd. L'agent l'appelle après
chaque batch de résolutions pour voir sa progression.

```python
@tool("reconciliation_scorecard", "Tableau de bord de la reconciliation BSS/OSS. "
      "Affiche : services resolus / en cours / non resolus, distribution de confiance, "
      "couverture par type (site, reseau, optique, party), top clients non resolus. "
      "Appeler regulierement pour suivre la progression.",
      {"focus": str})
async def reconciliation_scorecard(args):
    # Scanne agent_resolutions vs service_master_active
    # Retourne stats formatées comme le scorecard GraceTHD
```

### `submit_resolution` — Soumettre une résolution

```python
@tool("submit_resolution", "Soumet la resolution d'un service. Le service est identifie "
      "par service_id. La resolution contient les matchs proposes (site_a, site_z, "
      "network_support, optical_support, party_final) avec justification et confiance.",
      {"service_id": str, "resolution_json": str})
async def submit_resolution(args):
    # Parse le JSON, valide les IDs référencés existent, insère dans agent_resolutions
    # Gardes déterministes : IDs existent? Confiance cohérente avec nb preuves?
```

### `validate_resolution` — Vérifier une résolution

```python
@tool("validate_resolution", "Verifie la coherence d'une resolution proposee. "
      "Controle : les IDs site/party existent, le device reseau est dans le bon POP, "
      "la route optique relie les bons endpoints.",
      {"service_id": str})
async def validate_resolution(args):
    # Contrôles croisés déterministes sur la résolution
```

---

## System Prompt (structure)

### Approche : générique + contexte projet

L'agent est un **expert réconciliation BSS/OSS générique** — pas spécifique Teloise.
Les spécificités réseau (conventions de nommage, patterns VLAN, anti-FP) sont injectées
dynamiquement depuis un fichier de contexte projet (`project_context.md` dans le workspace),
exactement comme agent-gracethd injecte le mode FTTH/FTTO.

```
Tu es un agent expert en réconciliation BSS/OSS pour les réseaux de télécommunications.
Tu maîtrises les données contrats (BSS), le réseau optique (GDB/GraceTHD) et
l'inventaire réseau (SWAG/RANCID/CPE). Tu sais interpréter des données sales,
incomplètes et hétérogènes pour rapprocher des services contractuels à leur
support technique réel.

## CADRE DE TRAVAIL
- La base SQLite est ton workspace. Tu la consultes avec query_db.
- Les configs réseau sont en lecture seule. Tu les cherches avec search_configs.
- Tu soumets tes résolutions avec submit_resolution.
- Tu vérifies ta progression avec reconciliation_scorecard.
- Tu ne modifies JAMAIS les tables source (ref_*, lea_*).

## SCHEMA DE LA BASE
[Généré dynamiquement depuis le SQLite — tables, colonnes, stats]

## ÉTAT ACTUEL
[Généré dynamiquement — nb services, nb résolus, nb en attente, top clients]

## CONTEXTE PROJET
[Injecté depuis project_context.md dans le workspace — contient :]
[- Conventions de nommage du réseau]
[- Patterns de matching connus]
[- Règles anti-faux-positifs]
[- Mapping sites/devices spécifiques]

## PRINCIPES DE MATCHING (GENERIQUES)

### Niveau de confiance
- high : 3+ sources concordantes, IDs vérifiés dans les tables de référence
- medium : 2 sources concordantes, ou inférence forte depuis une source primaire
- low : 1 seule source, ou inférence faible → escalade
- JAMAIS de résolution sans au moins 1 preuve vérifiable

### Stratégie d'investigation
1. Partir du service BSS (client, offre, endpoints, refs)
2. Chercher les indices réseau (VLAN labels, descriptions d'interfaces, CPE)
3. Croiser avec les sites (adresses, noms, alias)
4. Vérifier la cohérence optique si applicable (routes, leases, fibres)
5. Documenter chaque étape de raisonnement

### Anti-faux-positifs (génériques)
- Ignorer les VLAN techniques/infra (voir contexte projet pour la liste)
- Ne jamais matcher sur un seul token court (< 4 caractères)
- Toujours vérifier la cohérence géographique (site ↔ POP/device)
- Distinguer client contractant vs client final

## COMPORTEMENT AUTONOME
- Traite les services par batch de clients/types similaires
- Appelle reconciliation_scorecard après chaque batch
- Documente chaque résolution avec justification
- Si bloqué sur un service → confidence=low + justification + passe au suivant
- Ne pose PAS de questions — fais le choix le plus raisonnable et documente-le
```

### Fichier de contexte projet

Pour Teloise, le `project_context.md` contiendrait les sections 5-12 de ARCHITECTURE.md :
- Conventions de nommage (bea1=Beauvais, crl1=Creil, etc.)
- Patterns spécifiques (A-N pour L2L, règles FON)
- VLAN techniques à ignorer
- Anti-FP spécifiques (FP1-FP9)
- Mapping offres (ROLE_BY_OFFER)

Pour un autre réseau, l'utilisateur fournirait un `project_context.md` différent.

---

## CLI

```bash
# 1. Préparer la base (vérifie que le SQLite existe, ajoute les tables agent)
agent-service-ref prepare --workspace /path/to/data

# 2. Lancer l'agent en mode interactif (comme agent-gracethd)
agent-service-ref interactive --workspace /path/to/data

# 3. Ou en mode batch avec un prompt
agent-service-ref batch --workspace /path/to/data \
  --prompt "Résous tous les services Lan To Lan du client ADISTA"
```

### Commande `prepare`

1. Vérifie que `service_ref/output/service_referential.sqlite` existe dans le workspace
2. Ajoute les tables agent si absentes (agent_resolutions, agent_evidence)
3. Charge le `project_context.md` du workspace (conventions réseau spécifiques)
4. Log les stats : "575 services, 23 déjà résolus, 552 en attente"

### Commande `interactive`

Lance `ClaudeSDKClient` avec le system prompt et les MCP tools, exactement comme
`agent-gracethd`. L'utilisateur dialogue avec l'agent qui investigue et résout.

### Commande `batch`

Comme `agent-gracethd/src/batch.py`. Un prompt, une exécution autonome.

---

## Ordre d'implémentation

| Étape | Contenu | Dépendances |
|-------|---------|-------------|
| **1** | Squelette projet (pyproject.toml, structure, `__init__.py`) | Aucune |
| **2** | `tools/db_tools.py` — query_db, list_tables, describe_table | Étape 1 |
| **3** | `tools/config_tools.py` — search_configs, read_config_file | Étape 1 |
| **4** | `tools/resolution_tools.py` — submit_resolution, validate_resolution | Étape 2 |
| **5** | `tools/scoring_tools.py` — reconciliation_scorecard | Étape 2, 4 |
| **6** | `prompts/system_prompt.py` — prompt système dynamique | Étape 2 (lit le schema) |
| **7** | `agent.py` + `cli.py` + `batch.py` — assemblage (copié de agent-gracethd) | Étape 2-6 |
| **8** | Test : `interactive` sur les données réelles | Tout |
| **9** | Itération sur le prompt et les tools selon résultats | Étape 8 |

Étapes 2-3 parallélisables. Étapes 4-5 parallélisables.

---

## Vérification

### Test de `prepare`

```bash
agent-service-ref prepare --workspace /path/to/data
# Vérifier : 817 LEA lines, 575 services, stats identiques au monolithe
sqlite3 output/service_referential.sqlite "SELECT COUNT(*) FROM service_master_active"
# → 575
```

### Test de l'agent

```bash
# Mode interactif
agent-service-ref interactive --workspace /path/to/data
> Résous les 5 services du client "ARC Agglomeration" et montre le scorecard

# Vérifier : l'agent query la DB, cherche dans les configs, propose des résolutions
# avec justification, et le scorecard montre 5 services résolus
```

### Comparaison avec le monolithe

Les 23 services auto-validés par le monolithe doivent être retrouvés par l'agent
avec confidence=high. Requête de vérification :

```sql
SELECT a.service_id, a.confidence, g.match_state
FROM agent_resolutions a
JOIN gold_service_active g ON g.service_id = a.service_id
WHERE g.match_state = 'auto_valid'
```

---

## Fichiers critiques

- `service_ref/build_service_referential.py` — monolithe source (2257 lignes), code à extraire
- `service_ref/output/service_referential.sqlite` — base de référence pour comparaison
- `service_ref/ARCHITECTURE.md` — documentation des règles de matching (sections 7-12)
- `/tmp/agent-gracethd/src/agent.py` — pattern de référence pour l'assemblage agent
- `/tmp/agent-gracethd/src/tools/scoring_tools.py` — pattern de référence pour le scorecard
- `/tmp/agent-gracethd/src/prompts/system_prompt.py` — pattern de référence pour le prompt
