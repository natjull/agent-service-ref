from __future__ import annotations

import unittest

from service_ref.lib.grouping import build_grouping_key


class GroupingTests(unittest.TestCase):
    def test_grouping_key_prefers_service_ref(self) -> None:
        key = build_grouping_key(
            client_contractant="SFR",
            nature_service="Lan To Lan",
            role_ligne="principal",
            route_refs=[],
            service_refs=["OPE3015/L2L018"],
            endpoint_a_raw="POP Amiens",
            endpoint_z_raw="43 avenue d'Italie 80000 Amiens",
            client_final="HEXANET",
            contract_file="",
            command_internal="CMD123",
        )
        self.assertEqual(key, "SVCREF|OPE3015/L2L018|SFR|Lan To Lan")


if __name__ == "__main__":
    unittest.main()
