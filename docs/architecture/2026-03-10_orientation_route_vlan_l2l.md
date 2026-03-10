# Orientation Architecture — Résolution Service (2026-03-10)

## Contexte
Le référentiel opère sur des données télécom hétérogènes (GDB, inventaires réseau, routes optiques, données spatiales, historiques opérateur). Les sources sont incomplètes et parfois contradictoires.

L’agent doit se comporter comme un ingénieur télécom enquêteur, pas comme un simple remplisseur de champs.

## North star de résolution
Pour **chaque service**, le succès cible repose d’abord sur deux champs structurants :
1. `route_ref` (référence route optique)
2. `network_vlan_id` (VLAN exploitable)

Ces champs doivent être fournis avec un niveau de preuve explicite (cf. doctrine ci-dessous).

## Doctrine de preuve
Trois niveaux de sortie autorisés :
- **strong** : preuves convergentes multi-sources (topologie + réseau + cohérence métier)
- **medium** : candidat dominant cohérent mais une partie du faisceau reste indirecte
- **declared_gap** : impossible de conclure proprement avec les données disponibles (pas d’hallucination)

Règle : mieux vaut un `declared_gap` explicite qu’une attribution faussement précise.

## Spécificité L2L — enrichissement obligatoire
En L2L, la résolution doit viser un triplet minimum :
- `network_vlan_id`
- `route_ref`
- **ancrage d’extrémité réseau** : `cpe_id` + équipement/site cœur de rattachement (si disponible)

Objectif : garantir la cohérence bout-en-bout service ↔ accès ↔ cœur.

## Stratégie d’inférence recommandée
### 1) Ancre principale
- Utiliser le **site GDB** comme point d’entrée prioritaire.

### 2) Faisceau topologique/spatial
- Explorer `route_parcours_by_site` et chaînes housing/câble/lease.
- En cas de route optique sale/incomplète : utiliser les proxys spatiaux (BPE/câble les plus proches) comme hypothèses.

### 3) Validation réseau
- Croiser avec indices VLAN/interface/CPE/core.
- Écarter les hypothèses non cohérentes (naming, parcours, voisinage, sens opérationnel).

### 4) Décision
- Si preuves convergent : soumettre avec niveau `strong` ou `medium`.
- Sinon : `submit_declared_gap` avec `data_gap_reason` explicite.

## Garde-fous qualité
- Interdire la validation L2L si `network_vlan_id` ou `route_ref` manquant.
- Journaliser systématiquement les contre-preuves (evidence_for / evidence_against).
- Limiter les décisions « single clue » sans corroboration.

## Indicateurs de pilotage (KPI)
- Taux de services avec `route_ref` renseigné
- Taux de services avec `network_vlan_id` renseigné
- Taux L2L avec triplet complet (`route_ref` + `network_vlan_id` + `cpe/core`)
- Répartition `strong / medium / declared_gap`
- Taux de révision humaine post-validation (proxy de faux positifs)

## Décision d’orientation
Cette orientation est la cible architecture à maintenir dans le prompt, les outils et les règles de soumission.
Le pipeline reste un fournisseur d’indices ; la décision finale reste preuve-driven côté agent.
