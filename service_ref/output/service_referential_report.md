# Active service referential build

## Scope
- Active LEA lines loaded: 817
- Active service masters built: 575

## Coverage
- Services with matched site: 434
- Services with matched route: 50
- Services with matched optical lease: 50
- Services with matched optical cable: 167
- Services with matched optical housing: 100
- Services with network support evidence: 232
- Services with at least one strong evidence (score >= 95): 198
- Services auto-validated in Gold: 106
- Services requiring review in Gold: 469
- Open review queue items: 1242

## Service mix
- Lan To Lan: 392
- IRU FON: 114
- Location FON: 63
- Hebergement: 5
- A qualifier: 1

## Referential assets
- Parties in party master: 2807
- Parsed network devices: 40
- Parsed network vlan labels: 6589

## Final facturable publication
- Published rows: 575
- Rows with final party: 435
- Rows with site A: 263
- Rows with site Z: 383
- Rows with network support: 232
- Rows with optical support: 266
- Rows with usable spatial evidence: 12

## Notes
- Optical matching is built directly from the GDB: logical refs (`TOIP`, `00FT`, `FREE`) plus physical cables and housings.
- Site matching uses exact aliases, addresses, BAN geocoding and spatial proximity to GDB objects.
- Network support uses exact SWAG/config refs first, then parsed RANCID VLAN/interface labels and CPE hints.
- Gold and review queue are materialized in SQLite for immediate exploitation.
- `service_facturable_final` is the published billing referential, built with priority `agent_validated > gold > review`.

## Final publication statuses
- needs_review: 463
- published_from_gold: 112

## Final truth sources
- gold_auto_valid: 106
- gold_review_required: 469