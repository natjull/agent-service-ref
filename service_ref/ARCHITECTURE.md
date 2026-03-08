# Architecture de reconstitution du referentiel de services TELOISE

## 1. Objet du document

Ce document consolide dans un seul support toute la matiere remontee par les
sous-agents, sans occulter les details utiles a la construction d'une base
d'exploitation immediate BSS/OSS pour la reprise du reseau TELOISE.

Le but n'est pas de decrire un MVP, mais de formaliser une architecture de
reconciliation exploitable en production locale, avec:

- les sources disponibles,
- les patterns de donnees detectes,
- les cles de jointure robustes,
- les risques de faux positifs,
- les limites observees du Gold actuel,
- la hierarchie des regles de decision,
- la structure cible du referentiel et de la review queue.

Le perimetre est limite aux services actifs facturables.

## 2. Contexte et objectif metier

Le referentiel cible doit permettre, pour chaque service actif:

- d'identifier formellement le client contractant,
- d'identifier formellement le client final ou le beneficiaire technique,
- de localiser les extremites A/Z,
- de rattacher le service a son support OSS optique et/ou reseau,
- de stocker toutes les preuves de rapprochement,
- de distinguer les cas auto-validables des cas a revoir.

Le pivot n'est ni la ligne LEA, ni la route optique, ni le port reseau. Le
pivot est un objet `service_master_active` relie a:

- un ou plusieurs objets BSS,
- un ou plusieurs objets OSS optiques,
- un ou plusieurs objets OSS reseau,
- un client contractant,
- un client final,
- des sites A/Z,
- un ensemble de preuves.

## 3. Etat de reference observe a date

Etat actuel du build local apres les travaux deja realises:

- lignes LEA actives chargees: `817`
- services actifs reconstruits: `575`
- Gold auto-valides: `23`
- Gold en `review_required`: `552`
- review items ouverts: `1780`

Couverture actuelle:

- services avec site matche: `433`
- services avec support reseau: `224`
- services avec route optique: `42`
- services avec lease optique: `44`
- services avec au moins une preuve forte non-party: `73`

Mix de services:

- `Lan To Lan`: `392`
- `IRU FON`: `114`
- `Location FON`: `63`
- `Hebergement`: `5`
- `A qualifier`: `1`

Causes racines principales de la review queue:

- `missing_final_party`: `397`
- `missing_site_z`: `369`
- `missing_network_support`: `288`
- `missing_optical_support`: `174`
- `manual_review`: `552`

Clients les plus exposes:

- `SFR`
- `ADISTA`
- `COMPLETEL SAS`
- `OPTION SERVICE TELECOM`
- `ARC Agglomeration de la Region de Compiegne`

## 4. Corpus de sources et role architectural

### 4.1 Sources BSS

- `6-3_20260203_Suivi_Contrats_LEA.xlsx`
- `Notice de lecture base LEA (1).docx`

Role:

- lignes de commande,
- offres,
- statuts actifs,
- client contractant,
- client final,
- refs internes/externe,
- dates,
- montants,
- duree de service.

### 4.2 Sources OSS optiques

- `GDB_TeloiseV3 (1).zip`
- `ban-60.csv`

Role:

- source de verite physique et logique du reseau optique,
- source de geocodage des adresses Oise pour les seeds spatiaux,
- routes optiques,
- leases,
- fibres,
- cables,
- sites,
- boitiers,
- connexions.

### 4.3 Sources OSS reseau et services

- `unzipped_equip/Export inventaire SWAG.xlsx`
- `unzipped_equip/Inventaire CPE Teloise Janv26.xlsx`
- `unzipped_equip/*.txt`
- `unzipped_equip/TELOISE/TELOISE/*.txt`

Role:

- inventaire interfaces,
- inventaire CPE,
- configs Huawei/RAD,
- configurations RANCID Cisco/Huawei,
- descriptions d'interfaces,
- labels de VLAN,
- hints client/site/port,
- indices de support reseau.

## 5. Corpus reseau exact vu par les sous-agents

### 5.1 Fichiers RANCID inventories

Fichiers detectes dans `unzipped_equip/TELOISE/TELOISE/`:

- `DSP_TELOISE_rancidIP_bea1-bas-1.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_bea1-sws-1.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_bea1-co-2.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_bea1-co-1.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_avr1-sw-1.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_avr1-sec-1.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_ami3-co-2.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_ami3-co-1.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_ftc1-sw-1.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_ftc1-sec-1.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_crl1-sws-1.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_crl1-sw-1.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_crl1-co-4.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_crl1-co-3.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_crl1-co-2.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_crl1-co-1.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_com1-co-2.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_com1-co-1.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_chv1-sw-1.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_chv1-sec-1.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_tho1-sec-1.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_net1-sw-1.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_net1-sec-1.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_nan1-sw-1.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_nan1-sec-1.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_mru1-sw-1.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_mru1-sec-1.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_moy1-sw-1.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_moy1-sec-1.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_jou1-sw-1.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_jou1-sec-1.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_vtt1-sw-1.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_vtt1-sec-1.teloise.net_FILTRED.txt`
- `DSP_TELOISE_rancidIP_tho1-sw-1.teloise.net_FILTRED.txt`

### 5.2 Fichiers d'exemple CPE/configs

- `unzipped_equip/Exemple 5624F.txt`
- `unzipped_equip/Exemple rad 205.txt`
- `unzipped_equip/Exemple rad 203.txt`
- `unzipped_equip/Exemple 5328.txt`
- `unzipped_equip/Exemple rad 2I-10G.txt`
- `unzipped_equip/Exemple 3328.txt`

## 6. Architecture logique cible

### 6.1 Niveaux de stockage

#### Bronze

Stockage brut source par source:

- LEA actif,
- GDB,
- GraceTHD,
- routes optiques exportees,
- SWAG,
- inventaire CPE,
- configs CPE,
- RANCID Cisco/Huawei.

#### Silver

Referentiels normalises:

- `party_master`
- `party_alias`
- `ref_sites`
- `ref_optical_logical_route`
- `ref_optical_lease`
- `ref_optical_lease_endpoint`
- `ref_optical_cable`
- `ref_optical_housing`
- `ref_optical_connection`
- `ref_optical_site_link`
- `ref_optical_cable_site_hint`
- `ref_ban_address`
- `service_spatial_seed`
- `service_spatial_evidence`
- `ref_routes`
- `ref_route_parcours`
- `ref_lease_template`
- `ref_fiber_lease`
- `ref_isp_lease`
- `ref_swag_interfaces`
- `ref_cpe_inventory`
- `ref_cpe_configs`
- `ref_network_devices`
- `ref_network_interfaces`
- `ref_network_vlans`

#### Gold

Referentiel exploitable:

- `service_master_active`
- `service_party`
- `service_endpoint`
- `service_support_optique`
- `service_support_reseau`
- `service_match_evidence`
- `service_review_queue`
- `gold_service_active`
- `override_party_alias`
- `override_site_alias`
- `override_service_match`

### 6.2 Pivot fonctionnel

Le pivot du referentiel est `service_master_active`.

Autour de ce pivot:

- le monde BSS est rattache par `service_bss_line`,
- le monde client par `service_party`,
- le monde site par `service_endpoint`,
- le monde optique par `service_support_optique`, alimente depuis la GDB logique et physique,
- le monde reseau par `service_support_reseau`,
- la traçabilite par `service_match_evidence`,
- la gouvernance par `service_review_queue` et les tables `override_*`.

## 7. Analyse detaillee du moteur `Lan To Lan`

### 7.1 Familles de fichiers et roles

| Famille | Role | Fichiers | OS / syntaxe |
| --- | --- | --- | --- |
| CO | Routeur coeur, agrege tous les VLAN clients | `ami3-co-1/2`, `bea1-co-1/2`, `com1-co-1/2`, `crl1-co-1/2/3/4` | Cisco IOS |
| SEC | Routeur de bordure par site, porte les trunks vers SW | `avr1-sec-1`, `chv1-sec-1`, `ftc1-sec-1`, `jou1-sec-1`, `moy1-sec-1`, `mru1-sec-1`, `nan1-sec-1`, `net1-sec-1`, `tho1-sec-1`, `vtt1-sec-1` | Cisco IOS |
| SW | Switch d'agregation, termine les CPE L2L, porte les descriptions clients | `avr1-sw-1`, `chv1-sw-1`, `crl1-sw-1`, `ftc1-sw-1`, `jou1-sw-1`, `moy1-sw-1`, `mru1-sw-1`, `nan1-sw-1`, `net1-sw-1`, `tho1-sw-1`, `vtt1-sw-1` | Huawei VRP |
| SWS | Switch d'acces secondaire, un port par service | `bea1-sws-1`, `crl1-sws-1` | Huawei VRP |
| CPE Huawei | Equipement client local, QinQ vers le SW | `Exemple 3328.txt`, `Exemple 5328.txt`, `Exemple 5624F.txt` | Huawei VRP |
| CPE RAD | Equipement client ETX | `Exemple rad 203.txt`, `Exemple rad 205.txt`, `Exemple rad 2I-10G.txt` | RAD ETX CLI |

### 7.2 Patterns de donnees reseau detectes

#### Pattern A - Description de VLAN sur SW/SWS/SEC (format canonique le plus riche)

Syntaxe Cisco IOS:

```text
vlan <ID>
 name CLIENT-LAN2LAN/<NOM_CLIENT>/<ID_VLAN>
```

Exemples:

```text
vlan 695
 name CLIENT-LAN2LAN/CG60/695
vlan 702
 name CLIENT-LAN2LAN/CLIENT/MDB/702
vlan 740
 name CLIENT-LAN2LAN/Opt-Service/740
```

Fichiers: `avr1-sec-1`, `com1-co-1/2`, `chv1-sec-1`, `bea1-co-1/2`

Syntaxe Huawei VRP:

```text
vlan <ID>
 description CLIENT-LAN2LAN/<NOM_CLIENT>/<ID_VLAN>
```

Exemples:

```text
vlan 596
 description CLIENT-LAN2LAN/AFPB Oise/596
vlan 604
 description CLIENT-LAN2LAN/OPAC Clemont/604
vlan 808
 description CLIENT-L2L/SDIS Tille/808
vlan 725
 description L2L/College JulesMichelet/725
vlan 802
 description L2L/HOTEL du Dpt/802
```

Regex proposee:

```regex
^(?:name|description)\s+(CLIENT[-_]?L(?:AN)?2L(?:AN)?|L2L|LAN2LAN)\s*/\s*([^/\n]+?)\s*/\s*([0-9]+)\s*$
```

Informations extraites:

- type de service,
- nom client,
- identifiant VLAN.

#### Pattern B - Description d'interface sur SW/SWS (port physique vers un CPE)

Syntaxe:

```text
description TRUNK vers CPE DSP CLIENT-LAN2LAN/<NOM_CLIENT>
description CPE DSP CLIENT-L2L/<NOM_CLIENT>
description CPE DSP CLIENT-LAN2LAN/<NOM_CLIENT>
```

Exemples:

```text
description TRUNK vers CPE DSP CLIENT-LAN2LAN/KORIAN France
description TRUNK vers CPE DSP CLIENT-LAN2LAN/CA AGGLOMERATION CREIL
description TRUNK vers CPE DSP CLIENT-LAN2LAN/VERALLIA PACKAGING
description TRUNK vers CPE DSP CLIENT-LAN2LAN/CRAMA PARIS VAL DE LOIRE
description TRUNK vers CPE DSP CLIENT-LAN2LAN/LIDL
description TRUNK vers CPE DSP CLIENT-LAN2LAN/Departement de l'Oise
description CPE DSP CLIENT-L2L/CHIMIREC VALRECOISE ST JUST EN CHAUSSEE
description TRUNK vers CPE DSP CLIENT-LAN2LAN/CETIM RECOTEL U118126 15/12/2025
```

Regex proposee:

```regex
^description\s+(?:TRUNK vers )?CPE DSP CLIENT[-_]?L(?:AN)?2L(?:AN)?/(.+)$
```

Information extraite: client.

#### Pattern C - Description d'interface sans prefixe CPE DSP (format court SW)

Formes observees:

```text
description CLIENT-LAN2LAN/<CLIENT>/<VLAN>
description L2L/<CLIENT>/<VLAN>
description LAN2LAN/<CLIENT>/<VLAN>
description CLIENT-L2L/<CLIENT>/<VLAN>
description CLIENT_LAN2LAN/<CLIENT>_<VLAN>
description <CLIENT>/<VLAN>
```

Exemples:

```text
description CLIENT-LAN2LAN/AFPB Oise/596
description CLIENT-LAN2LAN/Completel/778
description LAN2LAN/OPTION SERVICE/741
description CLIENT-LAN2LAN/OPE1214-L2L240 - Norbert Dentressangle/511
description LAN2LAN/Lycee-Cassini/1100
description L2L/College HenryBeaumont/730
description ECCF RANTIGNY-LINKT/628
description Fernel_Clermont/695
description Societe Chimirec/1193
description College Aramont/622
```

Fichiers: `avr1-sw-1`, `bea1-sws-1`, `chv1-sw-1`, `crl1-sw-1`, `ftc1-sw-1`, `jou1-sw-1`, `moy1-sw-1`

#### Pattern D - Nom de VLAN sur CO Cisco IOS (`VREG_<ID>`)

Le nommage CO est generalement generique et peu exploitable.

Exception constatee:

```text
vlan 702
 name CLIENT-LAN2LAN/CLIENT/MDB/702
```

En regle generale, les VLAN `511-850`, `1100-1200`, `3701-3760` sont nommes
`VREG_<ID>` et ne doivent pas servir a identifier un client.

#### Pattern E - Description d'interface sur CO (format `Vers ... TOIP`)

Forme:

```text
description Vers <EQUIPEMENT>/<SLOT.PORT>/[FON/]TOIP<NNNN>[-<NNNN>]
```

Exemples:

```text
description Vers 60BO81-CAA-1/TOIP 0603
description Vers 60BRS1-CAA-1/0.1/TOIP 0257
description Vers MOY1-SEC-1/0.1/FON/TOIP0059-0060
description Vers JOU1-SEC-1/0.1/FON/TOIP0521-0522
description Vers CRL1-CO-1/1.22/FON/TOIP0103-0104
description Vers CLIENT INFOSAT NIVILLIER A10CBV/0.12/TOIP 0113
```

Regex proposee:

```regex
^description Vers ([^/]+)/([^/]+)(?:/FON)?/TOIP\s*([0-9]{4})(?:-(?:TOIP\s*)?([0-9]{4}))?
```

Informations extraites:

- equipement distant,
- port,
- un ou deux TOIP.

#### Pattern F - Description d'interface sur CO (format `L;` structure)

Forme:

```text
description L;<EQUIPEMENT>;<PORT>;;<SERVICE>
```

Exemples:

```text
description L;AMI3-SW-1;1/0/0;;TOIP 2703-2704;
description L;BEA1-CO-1;4/4;;TOIP0345;
description L;60STL1-CAA-1;0/2;;TOIP 0105-0106;
description L;JOU1-SW-2;2.0.0;;TOIP 2535-TOIP 2536;MES_NOK
```

Regex proposee:

```regex
^description L;([^;]+);([^;]+);;([^;]+);
```

Informations extraites:

- equipement distant,
- port distant,
- service TOIP.

#### Pattern G - Description d'interface sur CO (format `B;`)

Forme:

```text
description B;<EQUIPEMENT>;<BUNDLE_ID>
```

Exemples:

```text
description B;AMI3-SW-1;1
description B;AMI3-CO-2;PO5(Te3/1 Te3/2);
description B;COM1-CO-1;PO5
```

Interpretation: bundle/LAG infra entre CO. A exclure des services clients.

#### Pattern H - `header shell information` sur CPE Huawei

Forme:

```text
header shell information "CPE DSP HUAWEI <MODELE> Client:<NOM_CLIENT> Site:<ADRESSE>DSP: TELOISE"
```

Exemples:

```text
header shell information "CPE DSP HUAWEI 3328 Client:Lyct Vincent Site:OBIANE 30 rue de Meaux 60300 SENLISDSP: TELOISE "
header shell information "CPE DSP HUAWEI 5328 Client:LIDL SNC Site: LIDL SNC  60810DSP: TELOISE"
```

Regex proposee:

```regex
header shell information "CPE DSP HUAWEI ([0-9A-Z]+) Client:(.+?) Site:(.+?)DSP:\s*TELOISE"
```

Informations extraites:

- modele CPE,
- client,
- adresse/site.

#### Pattern I - `snmp-agent sys-info location` sur CPE Huawei

Forme:

```text
snmp-agent sys-info location <ADRESSE_SITE>
```

Exemples:

```text
snmp-agent sys-info location OBIANE 30 rue de Meaux 60300 SENLIS
snmp-agent sys-info location LIDL SNC  60810
```

Regex proposee:

```regex
^snmp-agent sys-info location (.+)$
```

#### Pattern J - `announcement` sur CPE RAD

Forme:

```text
announcement '-----CPE L2 RAD  SFR    <NOM_CPE>--Site <ADRESSE>--Swag/Port <OPE_NUM>/<L2L_NUM>--Model <MODELE>-- '
```

Exemples:

```text
announcement '-----CPE L2 RAD  SFR    SFR Collectivite--Site 43 AVENUE D ITALIE 80000 AMIENS--Swag/Port OPE3015/L2L018--Model ETX205-- '
announcement '-----CPE L2 RAD  SFR    RDETX-2I-PORTE-COLLECTE-ADISTA-AMIENS2-001--Site  --Swag/Port AMI3-SW-1-2/0/43--Model ETX-2i-10g 4SFPP 24SFP-- '
announcement '-----CPE L2 RAD  SFR    SFR Collectivite--Site --Swag/Port  --Model ETX203-- '
```

Regex proposee:

```regex
announcement '-----CPE L2 RAD\s+SFR\s+(.+?)--Site\s+(.*?)--Swag/Port\s+(.*?)--Model\s+(.*?)--\s*'
```

Informations extraites:

- nom CPE,
- site,
- port SWAG ou OPE/L2L,
- modele.

#### Pattern K - Nom de port Ethernet RAD (NNI/UNI)

Exemples:

```text
name "AMI3-SW-1_2/0/44_OPE3015/L2L018"
name "OPE3015/L2L018-POP-AMIENS-HEXANET"
name "NET1-SW-2_3/0/30_LAAI8C87"
name "LAAI8FX2-S3G600012-CHANTILLY"
name "NNI-MID_AMI3-SW-1-2/0/43"
```

Regex de base:

```regex
OPE([0-9]+)/L2L([0-9]+)
```

Informations extraites:

- numero OPE,
- numero L2L,
- port SW voisin,
- hint de site/client.

#### Pattern L - Description de VLAN avec `OPE_L2L`

Exemple:

```text
description OPE3686_L2L083/3719
```

Regex proposee:

```regex
^(?:name|description)\s+OPE([0-9]+)[/_-]L2L([0-9]+)/([0-9]+)
```

Informations extraites:

- OPE,
- L2L,
- VLAN.

#### Pattern M - QinQ sur CPE Huawei

Forme:

```text
port hybrid pvid vlan <VLAN_OUTER>
port vlan-stacking vlan 1 to 4094 stack-vlan <VLAN_OUTER> remark-8021p 1
description port acces vers le client <NOM_CLIENT>
```

Exemples:

```text
description port acces vers le client Lycee St Vincent   -> pvid vlan 599/643/676
description port acces vers le client LIDL SNC           -> pvid vlan 2508
```

Regex proposee:

```regex
description port acces vers le client (.+)\s*\n.*port hybrid pvid vlan ([0-9]+)
```

#### Pattern N - `sysname` sur CPE Huawei

Exemples:

```text
sysname HW3328_Lycee_St_Vincent_0
sysname HW5328_LIDL_SNC_BARBERY_0
sysname DSPL2-deVinci-OS_HW
```

Regex proposee:

```regex
^(?:<)?(?:HW|DSP)?([A-Z0-9]+)_(.+?)(?:_[0-9]+)?(?:_HW|_OS_HW)?(?:>)?(?:display current-configuration)?
```

### 7.3 Informations extraites par pattern

| Pattern | Client | Site | Port | VLAN | OPE/L2L | TOIP | POP |
| --- | --- | --- | --- | --- | --- | --- | --- |
| A - VLAN name/description | oui | non | non | oui | partiel | non | non |
| B - TRUNK vers CPE | oui | non | oui | non | non | non | non |
| C - description interface courte | oui | non | oui | partiel | non | non | non |
| D - VREG CO | non | non | non | oui | non | non | oui |
| E - Vers TOIP | non | non | oui | non | non | oui | oui |
| F - L; structure | non | non | oui | non | non | oui | oui |
| H - header CPE Huawei | oui | oui | non | non | non | non | non |
| I - snmp location CPE | partiel | oui | non | non | non | non | oui |
| J - announcement RAD | partiel | oui | oui | non | oui | non | oui |
| K - eth name RAD | non | partiel | oui | non | oui | non | oui |
| L - OPE_L2L dans vlan desc | non | non | non | oui | oui | non | non |
| M - QinQ port | oui | non | oui | oui | non | non | non |
| N - sysname CPE | oui | partiel | non | non | non | non | non |

### 7.4 Familles de fichiers ou chaque pattern apparait

| Pattern | CO | SW | SWS | SEC | CPE HW | CPE RAD |
| --- | --- | --- | --- | --- | --- | --- |
| A - vlan name/desc | partiel (702) | oui | oui | oui | non | non |
| B - TRUNK vers CPE | non | oui | oui | non | non | non |
| C - desc interface courte | non | oui | oui | non | non | non |
| D - VREG | oui | non | non | non | non | non |
| E - Vers TOIP | oui | non | non | SEC | non | non |
| F - L; structure | oui | non | non | non | non | non |
| H - header shell CPE | non | non | non | non | oui | non |
| I - snmp location | non | non | non | non | oui | non |
| J - announcement | non | non | non | non | non | oui |
| K - eth name RAD | non | non | non | non | non | oui |
| L - OPE_L2L vlan | CO (702) | SW (nan1) | SWS (crl1) | non | non | non |
| M - QinQ port | non | non | non | non | oui | non |
| N - sysname CPE | non | non | non | non | oui | non |

### 7.5 Risques de faux positifs `Lan To Lan`

#### FP1 - VLAN `VREG_*` sur CO

Les CO portent des centaines de VLAN nommes `VREG_<ID>` sans information client.

Regle anti-FP:

- ne jamais resoudre un client depuis `VREG_*`
- ne parser les descriptions client que sur SW/SWS/SEC, pas sur CO pour les VLAN `511-1200`

#### FP2 - VLANs purement techniques/infrastructure

Exemples:

- `VLAN_NATIF`
- `9Ethernet_DSP_nominal`
- `Management_des_CPE_DSP`
- `QinQ-30`
- `VLAN Transport DSP`

Regle anti-FP:

- blacklister a minima les VLAN `1, 5, 9, 10, 19, 20, 29, 30, 39, 40, 46, 50, 85, 213, 900, 960-970, 1000, 4009, 4011, 4012, 4022-4086`

#### FP3 - Descriptions `B;` entre CO

Interpretation: port-channel/LAG infra. A exclure du service client.

#### FP4 - Descriptions `Vers ...` sans `TOIP`

Une description `Vers` sans `TOIP` designe une liaison infra. A exclure des
services `Lan To Lan`.

#### FP5 - Double description sur un meme port

Exemple observe dans `jou1-sw-1`:

```text
description description TRUNK vers CPE DSP CLIENT-LAN2LAN/DECATHLON SA FRS1126
```

Le parser doit dedoublonner le prefixe `description`.

#### FP6 - LIDL sans suffixe explicite VLAN

Exemple observe dans `crl1-sws-1`:

```text
description TRUNK vers CPE DSP CLIENT-LAN2LAN/LIDL
```

Le label seul ne donne pas le VLAN. Il faut croiser avec d'autres indices.

#### FP7 - `announcement` RAD incomplet

Exemple:

```text
announcement '-----CPE L2 RAD  SFR    SFR Collectivite--Site --Swag/Port  --Model ETX203-- '
```

Le parser doit tolerer des champs vides.

#### FP8 - `L2L DSP` purement technique

Exemple:

```text
vlan 751 name L2L DSP
description L2L DSP
```

Interpretation: transport infra. A exclure.

#### FP9 - Forme `<Nom>/<VLAN>` sans prefixe

Exemples:

```text
description College Aramont/622
description Fernel_Clermont/695
```

Ce pattern est utile mais moins fiable. Il doit etre confirme par une autre
source.

### 7.6 Hierarchie des regles pour le moteur `Lan To Lan`

#### Priorite 1 - Sources les plus fiables

##### Regle P1-A - `vlan name/description` canonique

Condition:

- ligne au format `CLIENT-LAN2LAN/.../<VLAN>` ou `L2L/.../<VLAN>`

Action:

- extraire `client`, `vlan`, `service_type`
- creer une evidence L2L forte

##### Regle P1-B - `header shell information` Huawei

Condition:

- ligne `header shell information "CPE DSP HUAWEI ... Client:... Site:..."`

Action:

- extraire client et site,
- creer une evidence forte CPE/client/site

##### Regle P1-C - `announcement` RAD

Condition:

- ligne `announcement '-----CPE L2 RAD ... Swag/Port OPE.../L2L... --'`

Action:

- extraire `site`, `OPE/L2L`, `Swag/Port`, `modele`
- creer une evidence forte support reseau

#### Priorite 2 - Sources fiables avec port connu

##### Regle P2-A - Interface `TRUNK vers CPE DSP CLIENT-LAN2LAN/...`

Action:

- extraire le client,
- marquer le port comme point d'acces client,
- croiser si possible avec le VLAN porte sur le trunk

##### Regle P2-B - Nom de port RAD avec `OPE/L2L`

Action:

- relier CPE, port voisin, OPE/L2L et site

##### Regle P2-C - `TOIP` sur interface CO/SEC avec correlation equipement

Action:

- relier le chemin du service vers un service OSS identifie par `TOIP`

#### Priorite 3 - Sources secondaires / enrichissement

##### Regle P3-A - Description `<Client>/<VLAN>` sans prefixe explicite

Action:

- ne creer qu'un candidat,
- necessite confirmation par une autre regle

##### Regle P3-B - `snmp location`

Action:

- enrichissement geographique uniquement

##### Regle P3-C - `sysname` CPE Huawei

Action:

- enrichissement client/site, jamais preuve unique

##### Regle P3-D - `OPE_L2L` dans la description de VLAN

Action:

- utiliser comme pont entre service commercial, VLAN et support reseau

#### Priorite 4 - Consolidation croisee par VLAN

Pour chaque VLAN detecte:

1. chercher sa description dans le meme fichier,
2. chercher l'interface qui l'utilise,
3. chercher le trunk qui le propage,
4. chercher le meme VLAN dans le SEC ou le CO,
5. chercher un `TOIP` voisin.

Regle de score:

- 3 sources concordantes ou plus: confiance elevee,
- 2 sources concordantes: confiance moyenne,
- 1 seule source: review only.

## 8. Analyse detaillee du moteur FON (`IRU FON` / `Location FON`)

### 8.1 GDB ESRI - tables et colonnes structurantes

La GDB est le referentiel OSS source.

#### `LEASE_TEMPLATE`

Colonnes juges structurantes:

- `IdCode`
- `Description`
- `CableId`
- `FiberId`
- `Status`
- `LeaseType`
- `StartDate`
- `EndDate`
- `Comments`
- `MIGRATION_OID`
- `reference_l1`
- `reference_l2`

Usages:

- gabarit de location/IRU,
- type de contrat,
- point de depart/arrivee logique,
- ancrage dans le reseau.

#### `Fiber_Lease`

Colonnes structurantes:

- `IdCode`
- `CableId`
- `FiberId`
- `LeaseId`
- `LeaseType`
- `Status`
- `StartDate`
- `EndDate`
- `Comments`
- `REF_EXPLOIT`

Usages:

- grain fibre individuel,
- rattachement a un bail,
- pivot direct vers `REF_EXPLOIT`.

#### `ISPLease`

Colonnes structurantes:

- `IdCode`
- `Description`
- `LeaseId`
- `ISPId`
- `LeaseType`
- `StartDate`
- `EndDate`
- `Capacity`
- `Comments`

Usages:

- composant ISP associe au bail,
- ancrage equipement/patch panel/tiroir.

#### `CONNEXION_TEMPLATE`

Colonnes structurantes:

- `IdCode`
- `CableId`
- `FiberId1`
- `FiberId2`
- `SpliceType`
- `Comments`
- `MIGRATION_OID`

Usages:

- epissures et connexions physiques,
- preuve de continuite optique,
- support de validation physique.

#### `Hubsite`

Colonnes structurantes:

- `IdCode`
- `Description`
- `SiteId`
- `Status`
- `Prop`
- `Gest`

Usages:

- noeud reseau,
- pivot site GDB vers GraceTHD.

### 8.2 Domaines GDB juges structurants

Domaines cites explicitement:

- `C_LOC_TYPE`: `IRU | LOC | PRET | TIERS | XXXX`
- `dFiberStatus`: `Lit=1 | Dark=2 | Reserved=3 | Broken=4 | Spare=7 | Coupee=8`
- `dStatus`: `Existing=1 | New=2 | Proposed=6 | AsBuilt=7 | Abandoned=4`
- `dISPType`: `OptPatchPanel | ISPEnclosure | ISPContainer | Rack | Shelf ...`
- `dSplice_Type`: `Fusion=2 | Implicit=1 | EnPassage=9 | Connexion non validee=12`

### 8.3 GraceTHD - tables et colonnes structurantes

#### `t_ropt`

Colonnes structurantes:

- `ro_code`
- `ro_ref_exploit`
- `ro_anneau`
- `ro_reseau`
- `ro_client`
- `ro_lessee`
- `ro_statut`

Role:

- pivot central de route optique commerciale.

#### `t_ropt_troncon`

Colonnes structurantes:

- `rt_code`
- `rt_ro_code`
- `rt_ordre`
- `rt_cb_code`
- `rt_fo_code`
- `rt_bp_code`
- `rt_ti_code`
- `rt_st_code`
- `rt_type`

Role:

- decomposition physique de la route.

#### Autres tables utiles

- `t_cable`
- `t_fibre`
- `t_ebp`
- `t_site`
- `t_ptech`
- `t_tiroir`
- `t_baie`
- `t_organisme`

Valeurs observees notables:

- `ro_lessee`: `Reseau SFR` dominant, puis `DSP`, puis `Entreprise`
- `ro_reseau`: quasi exclusivement `BACKBONE`
- `ro_statut`: `ACT`
- `cb_statut`: `REC`
- `st_typelog`: `CLIENT` et `RESEAU`

Exemples reels:

```text
RO_TOIP_0181  ref_exploit=TOIP 0181  anneau=TOFO10  lessee=Reseau SFR  statut=ACT
RO_TOIP_0723  ref_exploit=TOIP 0723  anneau=TOFO30  lessee=Reseau SFR
RO_FREE_4469  ref_exploit=FREE 4469  anneau=''      lessee=Reseau SFR
```

### 8.4 LEA - colonnes structurantes pour le FON

Colonnes citees comme cruciales:

- `CRM.FOU - Nom Usuel`
- `CMD - Nom client contractant`
- `nom fichier`
- `CMD.OFF - Libelle detaille du code offre`
- `Nb installation LigneDeCmd`
- `CMD - Numero commande interne`
- `CMD - Numero commande externe`
- `CMD - Secteur geographique1`
- `CMD - Secteur geographique2`
- `Client Final (ADV)`
- `Lineaire installation`
- `CMD - Date de signature`
- `Duree Service LigneDeCmd`
- `CMD - Statut Commande`
- `CMD - Date de debut facturation`
- `CMD - Date de resiliation`
- `CMD - FMS`
- `RM - Initiale`
- `RM - Derniere`
- `CMD - IRU`
- `Montant LigneDeCmd`

Repartition observee des offres FON:

- `Fibre-Metro-IRU`: `294`
- `Fibre-Longue Distance-IRU`: `64`
- `Fibre-Metro-location`: `74`
- `Fibre-Longue Distance-location`: `13`
- `Location Liaison Fibre Optique`: `4`

Statuts observees:

- `40`: actif
- `50`: actif
- `90`: resilie
- `60`: a qualifier
- `99`: en cours

### 8.5 Export `routes_optiques_pur_gdb_TELOISE (2).xlsx`

Colonnes structurantes citees:

- `ROUTE_ID`
- `REF_EXPLOIT`
- `RESEAU`
- `CLIENT`
- `ETAPE`
- `TYPE`
- `SITE`
- `BPE`
- `CABLE_ENTRANT`
- `CABLE_SORTANT`
- `SOUDURE`
- `COMMENTAIRE`

Valeurs observees:

- `REF_EXPLOIT`: `TOIP`, `FREE`, `OPSC`, etc.
- `TYPE`: `DEPART`, `ARRIVEE`, `PASSAGE`, `IMPASSE`, `BPE`
- `BPE`: format `SPE...`

### 8.6 Cles de jointure robustes

#### GDB vers LEA

| Champ GDB | Champ LEA | Fiabilite | Exemple |
| --- | --- | --- | --- |
| `Fiber_Lease.REF_EXPLOIT` | sous-chaine de `CMD - Numero commande externe` | haute | `TOIP 0181` |
| `LEASE_TEMPLATE.IdCode` | presence de `LOCFON` dans `CMD - Numero commande externe` | moyenne | `OPE2377/LOCFON412` |
| `LEASE_TEMPLATE.Comments` | `CMD - Numero commande externe` | moyenne | `CODE:TOFO10` |
| `LEASE_TEMPLATE.LeaseType` | `CMD.OFF - Libelle offre` | haute par regle | `IRU` vs `location` |
| `LEASE_TEMPLATE.StartDate` | date de debut facturation | moyenne | tolerance `+-90j` |
| `Fiber_Lease.FiberId` | `Nb installation` + `Lineaire` | faible directe | coherence seulement |

Cle composite recommandee:

```text
REF_EXPLOIT like CMD_externe extrait
AND LeaseType = type derive de l'offre
AND abs(StartDate - Date_facturation) < 90 jours
```

#### GDB vers GraceTHD

| Champ GDB | Champ GraceTHD | Fiabilite |
| --- | --- | --- |
| `Fiber_Lease.CableId` | `t_cable.cb_code` | haute |
| `Fiber_Lease.FiberId` | `t_fibre.fo_code` | haute |
| `ISPLease.ISPId` (`NW_ENCLOSURE_*`) | `t_ebp.bp_code` | haute |
| `ISPLease.ISPId` (`NW_SHELF_*`) | `t_tiroir.ti_code` | haute |
| `ISPLease.ISPId` (`NW_OPTPATCHPANEL_*`) | `t_tiroir.ti_code` ou `t_baie.ba_code` | haute |
| `Hubsite.SiteId` (`NW_HUBSITE_*`) | `t_site.st_code` | haute |
| `LEASE_TEMPLATE.Comments` `CODE:` | `t_ropt.ro_anneau` | moyenne |
| `Fiber_Lease.REF_EXPLOIT` | `t_ropt.ro_ref_exploit` | haute |

Jointure principale recommandee:

```text
Fiber_Lease.REF_EXPLOIT = t_ropt.ro_ref_exploit
AND Fiber_Lease.CableId = t_ropt_troncon.rt_cb_code
AND Fiber_Lease.FiberId = t_ropt_troncon.rt_fo_code
```

#### LEA vers GraceTHD

| Champ LEA | Champ GraceTHD | Fiabilite |
| --- | --- | --- |
| `CMD - Numero commande externe` (partie TOIP/FREE) | `t_ropt.ro_ref_exploit` | haute |
| `CMD - Secteur geographique1/2` | `t_site.st_etiquet` | faible |
| `Lineaire installation` | cumul de longueurs `t_ropt_troncon` | moyenne |
| `CMD - Nom client contractant` | `t_ropt.ro_client` | faible |

Extraction recommandee depuis LEA:

```python
pattern = r'(TOIP|FREE|OPSC|SFIP|00FT)\s*\d{4}'
```

### 8.7 Champs GDB juges tres precieux

#### `REF_EXPLOIT`

Presences:

- `Fiber_Lease`
- `t_ropt.ro_ref_exploit`
- `routes_optiques.REF_EXPLOIT`

Formats observes:

- `TOIP 0181`
- `TOIP 1661`
- `FREE 4469`
- `00FT 0237`
- `OPSC 0130`
- `SFIP71596`

Usage:

- cle primaire virtuelle du FON,
- pivot de jointure GDB / GraceTHD / LEA.

Piège:

- dans LEA, la reference est enchassee dans `CMD - Numero commande externe`

#### `Comments`

Usages observes:

- `CODE:TOFO10` -> anneau GraceTHD
- references travaux,
- references historiques,
- commentaires de migration.

#### `MIGRATION_OID`

Usage:

- tracer la migration depuis la base source,
- relier ancien et nouveau modele quand l'information existe.

#### `reference_l1/reference_l2`

Interpretation retenue par les sous-agents:

- references des extremites du bail,
- potentiellement reliees a `Hubsite` / `t_site` / codes physiques,
- tres utiles pour la resolution site-site des `IRU FON` et `Location FON`.

#### `BPE` dans l'export parcours

Format observe:

- `SPExxxxxx-n`

Usage:

- jointure robuste vers `t_ebp.bp_codeext` et `t_ptech.pt_codeext`.

### 8.8 Risques de collision et d'incoherence temporelle

#### Risque 1 - dedoublonnage `REF_EXPLOIT`

Un meme anneau peut porter deux sens logiques distincts, par exemple:

- `TOIP 0181`
- `TOIP 0182`

Ils ne doivent pas etre fusionnes aveuglement.

#### Risque 2 - avenant vs contrat initial LEA

Plusieurs lignes peuvent decrire le meme lien physique avec des dates et des
statuts differents. Il faut raisonner en service actif courant.

#### Risque 3 - IRU techniquement echu mais non resilie formellement

Les IRU de 15 a 20 ans peuvent etre techniquement echus alors que LEA les garde
actifs. Il faut signaler l'alerte, sans casser la reconciliation.

#### Risque 4 - incoherence `LOC` / `IRU`

Un meme support peut porter des fibres en location et d'autres en IRU. Il faut
verifier la coherence entre type GDB et type LEA.

#### Risque 5 - `CMD - IRU` vide pour une offre IRU

Le discriminant de type ne doit jamais etre le montant `IRU`, mais bien le
libelle d'offre.

#### Risque 6 - decallage temporel GDB vs LEA

La photo GDB est plus ancienne que le LEA. Certains contrats recents peuvent
etre absents de la GDB.

#### Risque 7 - multi-lignes maintenance / travaux

Les lignes `Maintenance`, `Extra Works`, `Divers`, `Emplacement Baie` ne doivent
pas etre comptees comme support FON principal.

#### Risque 8 - `codeext` / `etiquet` non normalises

Les `codeext` ne sont pas tous des identifiants stables. Prudence sur les
jointures textuelles brutes.

### 8.9 Hierarchie de regles pour le moteur FON

#### Niveau 1 - Qualification du type

Regles:

- offre contenant `IRU` -> type `IRU`
- offre contenant `location` -> type `LOC`
- offres Ethernet / SDH / NETCENTER / maintenance / extra works -> hors FON principal

Validation croisee:

- `LeaseType(GDB)` doit etre coherent avec le type derive de LEA,
- sinon lever une alerte `CONFLIT_TYPE`.

#### Niveau 2 - Jointure principale

Regles:

- `REF_EXPLOIT` exact entre LEA, GDB et GraceTHD,
- fallback sur `SPE/BPE`, `CableId`, `FiberId`.

#### Niveau 3 - Filtre actif courant

LEA actif recommande:

- statut `40` ou `50`,
- pas de resiliation passee,
- date de facturation presente,
- offre de type FON.

GraceTHD actif recommande:

- `ro_statut = ACT`,
- cable recolte,
- fibre non HS.

#### Niveau 4 - Verifications de coherence

- coherence lineaire,
- coherence nombre de fibres,
- detection des orphelins BSS et OSS,
- gestion des IRU echus,
- gestion des services probabilistes.

#### Niveau 5 - Resolution des cas ambigus

- aller/retour optique distincts,
- coexistence `LOC` puis `IRU`,
- `LOC` sans `REF_EXPLOIT`,
- `IRU` sans montant,
- cas post-photo GDB.

## 9. Diagnostic detaille du Gold actuel

### 9.1 Chiffres de cadrage remontes par les sous-agents

| Indicateur | Valeur |
| --- | --- |
| Services actifs construits | `575` |
| Auto-valides | `23` |
| Review required | `552` |
| Review items ouverts | `1780` |

Conclusion des sous-agents: le ratio est mauvais et releve de blocages
structurels, pas d'un bug unique.

### 9.2 Causes racines par type d'alerte

#### `missing_final_party`

Causes dominantes:

- `client_final` vide dans LEA,
- fallback sur `endpoint_z_raw` qui est souvent geographique et non client,
- resolution party trop dependante de labels deja connus,
- abreviations et codes internes non normalises.

Impact constate:

- tres fort chez `SFR`, `ARC`, `COMPLETEL SAS`, `ADISTA`.

#### `missing_site_z`

Causes dominantes:

- `endpoint_z_raw` souvent abrege ou fonctionnel,
- token overlap site trop strict,
- cercle vicieux avec le `lease_site_pair_match` qui suppose deux sites deja resolus.

#### `missing_network_support`

Causes dominantes:

- absence de `service_refs` dans LEA pour beaucoup de `Lan To Lan`,
- labels reseau pas encore suffisamment exploites,
- couverture reseau incomplete ou non consolidee,
- heuristiques VLAN/interface trop prudentes pour auto-valider.

#### `missing_optical_support`

Causes dominantes:

- `route_refs` absents ou incomplets dans LEA,
- `ROUTE_PATTERN` trop restrictif sur les formats SFR/ARC,
- fallback par paire de sites bloque par `missing_site_z`.

### 9.3 Clients et familles les plus affectes

| Rang | Client | Nature principale | Pathologie dominante |
| --- | --- | --- | --- |
| 1 | `ADISTA` | `Lan To Lan` | `missing_final_party`, `missing_network_support` |
| 2 | `SFR` | `IRU FON` et `Lan To Lan` | `missing_optical_support`, `missing_site_z`, `missing_final_party` |
| 3 | `ARC Agglomeration de la Region de Compiegne` | `IRU FON` | `missing_final_party`, `missing_site_z`, `missing_optical_support` |
| 4 | `COMPLETEL SAS` | `Lan To Lan` | `missing_final_party`, `missing_site_z` |
| 5 | `FREE` | `IRU FON` | plusieurs trous simultanes |
| 6 | `BOUYGUES TELECOM` | `Location FON` / `IRU FON` | `missing_site_z`, `missing_optical_support` |
| 7 | collectivites locales | `IRU FON` | sites resolus trop faiblement |

### 9.4 Leviers les plus efficaces sans augmenter le risque

#### Levier 1 - `override_site_alias`

Effet attendu:

- traiter les abreviations recurrentes (`PTRO`, `HDV`, `BDP`, `GAR`, etc.),
- gain important sur `missing_site_z`,
- risque de faux positifs nul si gouverne manuellement.

#### Levier 2 - `override_party_alias`

Effet attendu:

- traiter les abreviations et sigles clients,
- gain important sur `missing_final_party`,
- risque nul si gouverne manuellement.

#### Levier 3 - extension `ROUTE_PATTERN`

Etendre la capture a:

- `PRJ\d+`
- `WF\d+`
- references `LOCFON`
- autres formats SFR/ARC frequents

Effet attendu:

- baisse de `missing_optical_support`, surtout chez `SFR`.

#### Levier 4 - recalibrage du seuil de signal `missing_site_z`

Observation:

- des matches site a `40` sont souvent plausibles,
- ils ne doivent pas auto-valider seuls,
- mais ils peuvent eviter du bruit inutile en review.

#### Levier 5 - propagation de `final_party` depuis un VLAN fort

Observation:

- les VLANs nommes par etablissement sont un tres gros gisement chez `ADISTA`,
- si le score VLAN est fort et corrobore par site ou support, on peut relever la partie finale.

#### Levier 6 - activer ou muscler le `lease_site_pair_match`

Observation:

- utile pour `IRU FON` / `Location FON`,
- a condition de raisonner avec des sites resolves meme a confiance moyenne,
- mais jamais en auto-validation seul.

### 9.5 Regles a interdire en auto-validation

Les sous-agents ont explicitement demande d'interdire les cas suivants en
`auto_valid` quand ils sont seuls:

- `network_vlan_label_match` score `72-88` sans corroboration,
- `site_token_overlap` score `40` seul,
- `cpe_token_overlap` seul,
- `final_party` resolue uniquement depuis `endpoint_z_raw`,
- `lease_site_pair_match` sans route/lease confirme,
- `network_interface_label_match` sans seconde preuve forte,
- toute auto-validation sans `contract_party_id`.

Regle de garde absolue recommandee:

- `auto_valid` exige `strong_evidence_count >= 2`

## 10. Architecture cible de reconciliation

### 10.1 Referentiel client (`party_master`)

Role:

- consolider les identites client contractant et client final,
- stocker les alias,
- absorber les sigles, abreviations et labels reseau,
- servir de source officielle de partie.

### 10.2 Referentiel site (`site_master` / `ref_sites`)

Role:

- normaliser `Hubsite`,
- exposer `MIGRATION_ID`, `MIGRATION_OID`, `REFERENCE`, `USERREFERENCE`, `ADRESSE1`,
- absorber les overrides d'abreviations,
- servir de source officielle d'extremites A/Z.

### 10.3 Referentiel support reseau

Composants:

- devices,
- interfaces,
- VLANs,
- CPE,
- SWAG,
- configs.

Role:

- reconstruire le support `Lan To Lan`,
- rattacher ports, trunks, CPE, VLANs et hints client/site.

### 10.4 Referentiel support optique

Composants:

- routes GraceTHD,
- parcours optiques,
- leases GDB,
- fibres,
- connexions,
- sites GDB.

Role:

- reconstruire le support `IRU FON` / `Location FON`,
- exposer le lien entre contrat, route, bail, fibre et sites.

### 10.5 Service master

Role:

- une unite unique de facturation technique active,
- reliee a une ou plusieurs lignes BSS,
- reliee a un support OSS,
- reliee a un client et un site,
- publiee avec preuves et niveau de confiance.

### 10.6 Evidence engine

Chaque regle doit produire:

- `rule_name`
- `source_table`
- `source_key`
- `score`
- `payload_json`

L'evidence n'est jamais implicite.

### 10.7 Review et overrides

La gouvernance impose:

- pas de correction directe dans le Gold,
- corrections manuelles via `override_*`,
- rerun complet pour republier le Gold,
- review queue actionnable par client, service, POP et type de manque.

## 11. Decision engine recommande

### 11.1 Etats cibles

- `auto_valid`
- `review_required`
- `rejected`

### 11.2 Regles `Lan To Lan`

Conditions minimales recommandees pour `auto_valid`:

- au moins un support reseau fort,
- au moins une corroboration site ou party,
- `strong_evidence_count >= 2`.

### 11.3 Regles `IRU FON` / `Location FON`

Conditions minimales recommandees pour `auto_valid`:

- route/lease/fiber/isp fort,
- au moins un site coherent,
- `strong_evidence_count >= 2`.

### 11.4 Regles `Hebergement`

Conditions minimales recommandees pour `auto_valid`:

- site fort,
- pas d'ambiguite sur l'objet heberge.

## 12. Plan de travail derive des constats sous-agents

### 12.1 Priorite 1 - Quick wins sans risque

- peupler `override_site_alias`
- peupler `override_party_alias`
- etendre `ROUTE_PATTERN`
- recalibrer le signal `missing_site_z`

### 12.2 Priorite 2 - Moteur `Lan To Lan` v2

- parser tous les patterns VLAN,
- parser tous les patterns interface,
- parser `announcement` RAD,
- parser `header shell` Huawei,
- classifier les VLANs `customer` vs `infra`,
- consolider VLAN -> interface -> device -> CPE -> site -> party.

### 12.3 Priorite 3 - Moteur `Party`

- renforcer la creation de `party_master`,
- utiliser les labels reseau comme candidats encadres,
- distinguer clairement `contract_party` et `final_party`.

### 12.4 Priorite 4 - Moteur `Site`

- alimenter les alias forts,
- renforcer l'adressage,
- accepter certains matches moyens en review, pas en auto-validation.

### 12.5 Priorite 5 - Moteur `FON` v2

- exploiter `REF_EXPLOIT`,
- exploiter `LEASE_TEMPLATE`, `Fiber_Lease`, `ISPLease`,
- exploiter `reference_l1/reference_l2`,
- confirmer par `TOIP` en CO,
- utiliser le parcours comme support, pas comme preuve unique.

### 12.6 Priorite 6 - Review et recette

- prioriser `SFR`, `ADISTA`, `COMPLETEL SAS`, `OPTION SERVICE TELECOM`, `ARC...`,
- tester un panier de verite terrain,
- surveiller les faux positifs avant toute hausse du volume `auto_valid`.

## 13. Checklist de mise en oeuvre a ne pas perdre

### A faire absolument

- conserver toutes les preuves,
- maintenir un pipeline deterministe,
- distinguer strictement `support reseau`, `support optique`, `party`, `site`,
- ne jamais auto-valider une partie finale sur adresse seule,
- ne jamais auto-valider un support reseau sur VLAN seul,
- ne jamais auto-valider un support optique sur paire de sites seule,
- governancer les exceptions avec `override_*`.

### A ne pas faire

- ne pas masquer l'incertain dans le Gold,
- ne pas fusionner des sens optiques distincts,
- ne pas compter maintenance/travaux comme supports principaux,
- ne pas utiliser `VREG_*` ou des VLANs techniques comme preuve client,
- ne pas ecraser les alias manuellement hors pipeline.

## 14. Conclusion d'architecture

Les sous-agents convergent tous vers la meme conclusion:

- le moteur prod ne doit pas etre un simple matching LEA -> GraceTHD,
- il faut un systeme de reconciliation gouverne, multi-source, centre sur le
  `service_master_active`,
- le succes se joue sur trois couches:
  - la resolution client,
  - la resolution site,
  - la reconstruction du support reseau ou optique,
- la review queue et les overrides sont des composants obligatoires, pas des
  gadgets.

Le Gold final doit etre capable d'expliquer pour chaque service:

- qui paye,
- qui consomme,
- ou,
- sur quel support,
- avec quelles preuves,
- et avec quel degre de certitude.

Ce document est volontairement exhaustif afin de servir de reference unique au
chantier de reconstitution du referentiel de services TELOISE.
