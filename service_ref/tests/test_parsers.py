from __future__ import annotations

import unittest

from service_ref.lib.parsers import extract_route_refs, extract_service_refs, parse_vlan_list


class ParserTests(unittest.TestCase):
    def test_extract_route_refs(self) -> None:
        self.assertEqual(extract_route_refs("OPE2228/L2L490/TOIP 2505"), ["TOIP 2505"])

    def test_extract_service_refs(self) -> None:
        self.assertEqual(extract_service_refs("OPE3015/L2L018-POP-AMIENS-HEXANET"), ["OPE3015/L2L018"])

    def test_parse_vlan_list_supports_ranges(self) -> None:
        self.assertEqual(parse_vlan_list("switchport trunk allowed vlan add 578-584,598,615"), [578, 579, 580, 581, 582, 583, 584, 598, 615])


if __name__ == "__main__":
    unittest.main()
