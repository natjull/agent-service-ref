from __future__ import annotations

import unittest
from pathlib import Path

from service_ref.verify_migration import compare_databases


class RegressionSmokeTests(unittest.TestCase):
    def test_baseline_database_matches_itself(self) -> None:
        baseline = Path("service_ref/output/service_referential.sqlite")
        self.assertEqual(compare_databases(baseline, baseline), [])


if __name__ == "__main__":
    unittest.main()
