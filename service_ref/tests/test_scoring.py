from __future__ import annotations

import unittest

from service_ref.lib.scoring import score_label_match


class ScoringTests(unittest.TestCase):
    def test_score_label_match_prefers_exact_clean_business_match(self) -> None:
        score = score_label_match(["LIDL BARBERY"], "CLIENT-LAN2LAN/LIDL BARBERY/2508")
        self.assertGreaterEqual(score, 90)


if __name__ == "__main__":
    unittest.main()
