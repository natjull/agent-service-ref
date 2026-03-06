from __future__ import annotations

from service_ref import build_service_referential as legacy


extract_route_refs = legacy.extract_route_refs
extract_service_refs = legacy.extract_service_refs
parse_vlan_list = legacy.parse_vlan_list
extract_vlans_from_line = legacy.extract_vlans_from_line
detect_vendor = legacy.detect_vendor
detect_source_family = legacy.detect_source_family
extract_client_site_from_header = legacy.extract_client_site_from_header
