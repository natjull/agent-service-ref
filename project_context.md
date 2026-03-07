# Contexte projet Teloise — Agent Service-Ref

## Conventions de nommage POP

Les noms d'equipements suivent le format `<pop><N>-<role>-<instance>` :

| Code POP | Ville |
|----------|-------|
| ami3 | Amiens |
| avr1 | Avrigny |
| bea1 | Beauvais |
| chv1 | Chevrieres |
| com1 | Compiegne |
| crl1 | Creil |
| ftc1 | Fontaine-Chaalis |
| jou1 | Jaux |
| moy1 | Mouy |
| mru1 | Montataire |
| nan1 | Nanteuil |
| net1 | Neuilly-en-Thelle |
| tho1 | Thourotte |
| vtt1 | Villers-Saint-Paul |

## Roles des equipements

| Suffixe | Role | OS | Informations exploitables |
|---------|------|----|---------------------------|
| co | Routeur coeur (CO) | Cisco IOS | TOIP, bundles infra, VREG generiques |
| sec | Routeur de bordure (SEC) | Cisco IOS | Trunks, VLAN clients, descriptions TOIP |
| sw | Switch d'agregation (SW) | Huawei VRP | Descriptions clients, VLAN, ports CPE |
| sws | Switch d'acces secondaire | Huawei VRP | Idem sw, un port par service |
| bas | Broadband Access Server | Cisco IOS | Peu exploitable pour L2L |

## ROLE_BY_OFFER (mapping offre LEA → nature service)

```
fibre-longue distance-iru         → IRU FON (principal)
fibre-longue distance-maintenance → IRU FON (maintenance)
fibre-metro-iru                   → IRU FON (principal)
fibre-metro-maintenance           → IRU FON (maintenance)
fibre-longue distance-location    → Location FON (principal)
fibre-metro-location              → Location FON (principal)
fibre-metro-location escomptee    → Location FON (principal)
lien ethernet - 1-100 mbits       → Lan To Lan (principal)
lien sdh - e1 (2 mbits)           → Lan To Lan (principal)
netcenter lan - divers            → Lan To Lan (annexe)
netcenter lan - extra works       → Lan To Lan (frais_acces)
emplacement baie - 48v            → Hebergement (principal)
netcenter baie - divers           → Hebergement (annexe)
fibre - extra works               → Hebergement (frais_acces)
```

## Patterns de matching reseau (A-N)

### Pattern A — VLAN name/description (canonique, le plus riche)
Format: `CLIENT-LAN2LAN/<CLIENT>/<VLAN>` ou `L2L/<CLIENT>/<VLAN>`
Fichiers: SW, SWS, SEC. **Pas fiable sur CO** (VREG generiques).

### Pattern B — TRUNK vers CPE
Format: `description TRUNK vers CPE DSP CLIENT-LAN2LAN/<CLIENT>`

### Pattern C — Description interface courte
Format: `description <CLIENT>/<VLAN>` (sans prefixe explicite)
Moins fiable, necessite confirmation.

### Pattern E — Vers TOIP (CO/SEC)
Format: `description Vers <EQUIP>/<PORT>/[FON/]TOIP<NNNN>`
Lie un service a un support optique TOIP.

### Pattern F — L; structure (CO)
Format: `description L;<EQUIP>;<PORT>;;<SERVICE>;`
Extraire equipement distant, port, service TOIP.

### Pattern H — header CPE Huawei
Format: `header shell information "CPE DSP HUAWEI <MODEL> Client:<NOM> Site:<ADRESSE>DSP: TELOISE"`
Tres fiable pour client + site.

### Pattern J — announcement RAD
Format: `announcement '-----CPE L2 RAD  SFR    <NOM>--Site <ADRESSE>--Swag/Port <OPE/L2L>--Model <MODEL>--'`
Tres fiable pour site + OPE/L2L + modele.

### Pattern K — Nom port Ethernet RAD
Contient souvent `OPE<NNNN>/L2L<NNN>` — lien direct vers ref service.

### Pattern L — OPE_L2L dans description VLAN
Format: `description OPE<NNNN>_L2L<NNN>/<VLAN>`

### Pattern M — QinQ sur CPE Huawei
Le `pvid vlan` sur un port client donne le VLAN outer.

### Pattern N — sysname CPE Huawei
Format: `HW<MODEL>_<CLIENT>_<SITE>` — enrichissement, jamais preuve unique.

## VLAN techniques a ignorer

Ne jamais matcher ces VLAN comme services clients:
- `1, 5, 9, 10, 19, 20, 29, 30, 39, 40, 46, 50, 85, 213`
- `900, 960-970, 1000, 4009, 4011, 4012, 4022-4086`
- Noms: `VLAN_NATIF`, `9Ethernet_DSP_nominal`, `Management_des_CPE_DSP`, `QinQ-30`, `VLAN Transport DSP`, `L2L DSP`
- Tout VLAN nomme `VREG_*` sur un CO

## Anti-faux-positifs (FP1-FP9)

- **FP1**: `VREG_*` sur CO = generique, non exploitable pour identifier un client
- **FP2**: VLAN techniques/infrastructure = blackliste ci-dessus
- **FP3**: Descriptions `B;` entre CO = bundles LAG infra, pas des services
- **FP4**: `Vers ...` sans `TOIP` = liaison infra
- **FP5**: Double description (`description description TRUNK...`) = dedoublonner
- **FP6**: Client sans suffixe VLAN explicite = croiser avec d'autres indices
- **FP7**: `announcement` RAD avec champs vides = tolerer les champs vides
- **FP8**: `L2L DSP` = transport infra, pas un service client
- **FP9**: `<Nom>/<VLAN>` sans prefixe = utile mais moins fiable, confirmer

## Mapping devices → POP

Tous les equipements suivent le format `<pop_code><N>-<role>-<instance>.teloise.net`.
Le `pop_code` correspond a la ville (voir tableau ci-dessus).
Les fichiers configs sont dans `unzipped_equip/TELOISE/DSP_TELOISE_rancidIP_<hostname>_FILTRED.txt`.

## Cles de jointure FON

- `REF_EXPLOIT` (format TOIP/FREE/OPSC + 4 chiffres) = pivot GDB ↔ GraceTHD ↔ LEA
- `Fiber_Lease.CableId` ↔ `t_cable.cb_code` (haute fiabilite)
- `Fiber_Lease.FiberId` ↔ `t_fibre.fo_code` (haute fiabilite)
- `Comments` contenant `CODE:TOFO<NN>` → anneau GraceTHD `ro_anneau`

## Mix de services attendu

- Lan To Lan: ~392 services (68%)
- IRU FON: ~114 services (20%)
- Location FON: ~63 services (11%)
- Hebergement: ~5 services (1%)
- A qualifier: ~1 service

## Regles de scoring

- 3+ sources concordantes → confidence `high`
- 2 sources concordantes → confidence `medium`
- 1 seule source → confidence `low` (review only)
