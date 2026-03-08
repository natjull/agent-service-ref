# Active service referential build

## Scope
- Active LEA lines loaded: 817
- Active service masters built: 575

## Coverage
- Services with matched site: 433
- Services with matched route: 43
- Services with matched optical lease: 44
- Services with matched optical cable: 166
- Services with matched optical housing: 104
- Services with network support evidence: 224
- Services with at least one strong evidence (score >= 95): 163
- Services auto-validated in Gold: 69
- Services requiring review in Gold: 506
- Open review queue items: 1570

## Service mix
- Lan To Lan: 392
- IRU FON: 114
- Location FON: 63
- Hebergement: 5
- A qualifier: 1

## Referential assets
- Parties in party master: 1743
- Parsed network devices: 40
- Parsed network vlan labels: 6589

## Final facturable publication
- Published rows: 575
- Rows with final party: 179
- Rows with site A: 185
- Rows with site Z: 364
- Rows with network support: 224
- Rows with optical support: 265

## Notes
- Optical matching is built directly from the GDB: logical refs (`TOIP`, `00FT`, `FREE`) plus physical cables and housings.
- Site matching uses exact aliases, addresses and token overlap on Hubsite names.
- Network support uses exact SWAG/config refs first, then parsed RANCID VLAN/interface labels and CPE hints.
- Gold and review queue are materialized in SQLite for immediate exploitation.
- `service_facturable_final` is the published billing referential, built with priority `agent_validated > gold > review`.

## Final publication statuses
- needs_review: 553
- published_from_gold: 22

## Final truth sources
- gold_auto_valid: 69
- gold_review_required: 506