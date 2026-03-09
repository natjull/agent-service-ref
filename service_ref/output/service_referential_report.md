# Active service referential build

## Scope
- Active LEA lines loaded: 817
- Active service masters built: 561

## Coverage
- Services with matched site: 429
- Services with matched route: 56
- Services with matched optical lease: 56
- Services with matched optical cable: 550
- Services with matched optical housing: 314
- Services with network support evidence: 236
- Services with at least one strong evidence (score >= 95): 382
- Services auto-validated in Gold: 106
- Services requiring review in Gold: 455
- Open review queue items: 1211

## Service mix
- Lan To Lan: 392
- IRU FON: 100
- Location FON: 63
- Hebergement: 5
- A qualifier: 1

## Referential assets
- Parties in party master: 2807
- Parsed network devices: 40
- Parsed network vlan labels: 6589

## Final facturable publication
- Published rows: 561
- Rows with final party: 424
- Rows with site A: 260
- Rows with site Z: 379
- Rows with network support: 236
- Rows with optical support: 550
- Rows with usable spatial evidence: 36

## Notes
- Optical matching is built directly from the GDB: logical refs (`TOIP`, `00FT`, `FREE`) plus physical cables and housings.
- Site matching uses exact aliases, addresses, BAN geocoding and spatial proximity to GDB objects.
- Network support uses exact SWAG/config refs first, then parsed RANCID VLAN/interface labels and CPE hints.
- Gold and review queue are materialized in SQLite for immediate exploitation.
- `service_facturable_final` is the published billing referential, built with priority `agent_validated > gold > review`.

## Final publication statuses
- needs_review: 450
- published_from_gold: 111

## Final truth sources
- gold_auto_valid: 106
- gold_review_required: 455