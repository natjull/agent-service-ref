Service referential build

This folder contains an executable local exploitation build to rebuild the
active technical service referential for the Teloise network from the BSS and
OSS sources available in this workspace.

Inputs expected at the workspace root:

- `6-3_20260203_Suivi_Contrats_LEA.xlsx`
- `Notice de lecture base LEA (1).docx`
- `GDB_TeloiseV3 (1).zip`
- `hello_gracethd.gpkg`
- `routes_optiques_pur_gdb_TELOISE (2).xlsx`
- `unzipped_equip/Export inventaire SWAG.xlsx`
- `unzipped_equip/Inventaire CPE Teloise Janv26.xlsx`
- `unzipped_equip/*.txt`
- `unzipped_equip/TELOISE/TELOISE/*.txt`

Run legacy entrypoint:

```bash
python service_ref/build_service_referential.py
```

Run modular pipeline:

```bash
python -m service_ref run-all
```

Run step by step:

```bash
python -m service_ref load
python -m service_ref normalize
python -m service_ref match
python -m service_ref consolidate
python -m service_ref publish
```

Generate analyst assistance artifacts for unresolved services:

```bash
python -m service_ref review-assist --dry-run --max-services 50
```

Outputs are written to `service_ref/output/`:

- `service_referential.sqlite`: local exploitation database with Bronze/Silver/Gold-style tables
- `service_master_active.csv`: compatibility export of the Gold active service catalog
- `service_facturable_final.csv`: final billing-oriented service referential, one row per `service_id`
- `service_match_evidence.csv`: detailed matching evidence by rule
- `service_review_queue.csv`: analyst review queue for unresolved services
- `party_master.csv`: normalized party referential
- `network_vlan_catalog.csv`: parsed VLAN and labels from configs/RANCID
- `service_referential_report.md`: coverage and implementation summary
- `review_suggestions.json`: structured analyst suggestions from `review-assist`
- `review_suggestions.csv`: analyst-friendly summary of the same suggestions
- `review_prompt_packs.json`: prompt payloads generated in dry-run mode

The pipeline is intentionally deterministic and explainable:

- active LEA scope only (`CMD - Statut Commande = 40`)
- service taxonomy based on the agreed business mapping
- exact matching first (`TOIP`, `OPE/L2L`, site ids)
- parsed network evidence from SWAG, CPE configs and RANCID files
- scored fallback matching for sites, VLAN labels, interface labels and CPEs
- all enrichments stored as evidence with a rule name and score

Publication outputs now distinguish:

- `gold_service_active`: technical Gold used by the reconciliation engine
- `service_facturable_final`: published billing referential using priority
  `agent_validated > gold > review`

The final billing referential keeps every active service visible, including
incomplete ones, but materializes explicit publication statuses and gap flags.

Verification:

```bash
python service_ref/verify_migration.py service_ref/output/service_referential.sqlite service_ref/output_modular/service_referential.sqlite --baseline-out-dir service_ref/output --candidate-out-dir service_ref/output_modular
```
