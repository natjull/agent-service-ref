from __future__ import annotations

import unittest

from service_ref.lib.normalize import classify_offer, clean_business_label, norm_text


class NormalizeTests(unittest.TestCase):
    def test_norm_text_strips_accents_and_punctuation(self) -> None:
        self.assertEqual(norm_text("Collège Jules-Michelet"), "COLLEGE JULES MICHELET")

    def test_clean_business_label_removes_transport_noise(self) -> None:
        self.assertEqual(clean_business_label("CLIENT-LAN2LAN/OPE1214-L2L240 - Norbert Dentressangle/511"), "NORBERT DENTRESSANGLE")

    def test_classify_offer_maps_lan_to_lan(self) -> None:
        self.assertEqual(classify_offer("Lien Ethernet - 1-100 Mbits"), ("Lan To Lan", "principal"))


if __name__ == "__main__":
    unittest.main()
