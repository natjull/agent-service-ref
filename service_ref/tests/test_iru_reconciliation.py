from __future__ import annotations

import sqlite3
import unittest

from service_ref.build_service_referential import (
    _contract_file_base,
    _extract_command_base,
    reconcile_iru_maintenance,
)


def _create_test_db() -> sqlite3.Connection:
    """Create an in-memory DB with just the tables needed for reconciliation tests."""
    con = sqlite3.connect(":memory:")
    con.executescript(
        """
        CREATE TABLE lea_active_lines (
            lea_line_id TEXT PRIMARY KEY,
            client_contractant TEXT,
            offer_label TEXT,
            nature_service TEXT,
            role_ligne TEXT,
            command_internal TEXT,
            command_external TEXT,
            contract_file TEXT,
            endpoint_a_raw TEXT,
            endpoint_z_raw TEXT,
            client_final TEXT,
            lineaire TEXT,
            date_signature TEXT,
            date_creation TEXT,
            duree_service TEXT,
            status_code TEXT,
            date_livraison TEXT,
            date_resiliation TEXT,
            fms TEXT,
            rm_initiale TEXT,
            rm_derniere TEXT,
            iru TEXT,
            montant TEXT,
            route_refs_json TEXT,
            service_refs_json TEXT,
            is_old INTEGER,
            grouping_key TEXT,
            source_row INTEGER,
            spatial_hints_json TEXT
        );
        CREATE TABLE service_master_active (
            service_id TEXT PRIMARY KEY,
            service_key TEXT,
            nature_service TEXT,
            principal_client TEXT,
            principal_offer TEXT,
            principal_external_ref TEXT,
            principal_internal_ref TEXT,
            route_refs_json TEXT,
            service_refs_json TEXT,
            endpoint_a_raw TEXT,
            endpoint_z_raw TEXT,
            client_final TEXT,
            line_count INTEGER,
            active_line_count INTEGER
        );
        CREATE TABLE service_bss_line (
            service_id TEXT,
            lea_line_id TEXT,
            role_ligne TEXT,
            is_principal INTEGER
        );
        CREATE TABLE iru_maintenance_reconciliation (
            maintenance_service_id TEXT,
            principal_service_id TEXT,
            strategy TEXT,
            match_detail TEXT,
            lea_lines_moved INTEGER
        );
        """
    )
    return con


def _insert_lea(con: sqlite3.Connection, lea_id: str, client: str, nature: str, role: str,
                cmd: str = "", contract: str = "", ep_a: str = "", ep_z: str = "") -> None:
    con.execute(
        "INSERT INTO lea_active_lines VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (lea_id, client, "IRU", nature, role, cmd, "", contract, ep_a, ep_z,
         "", "", "", "", "", "", "", "", "", "", "", "", "", "[]", "[]", 0, "", 0, ""),
    )


def _insert_service(con: sqlite3.Connection, svc_id: str, nature: str, client: str,
                     line_count: int = 1) -> None:
    con.execute(
        "INSERT INTO service_master_active VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (svc_id, f"KEY-{svc_id}", nature, client, "IRU", "", "", "[]", "[]", "", "", "", line_count, line_count),
    )


def _insert_bss(con: sqlite3.Connection, svc_id: str, lea_id: str, role: str, is_principal: int = 0) -> None:
    con.execute("INSERT INTO service_bss_line VALUES (?,?,?,?)", (svc_id, lea_id, role, is_principal))


class TestExtractCommandBase(unittest.TestCase):
    def test_dot_suffix(self) -> None:
        self.assertEqual(_extract_command_base("52.1"), "52")

    def test_dash_suffix(self) -> None:
        self.assertEqual(_extract_command_base("01-2"), "01")

    def test_no_suffix(self) -> None:
        self.assertEqual(_extract_command_base("52"), "52")

    def test_empty(self) -> None:
        self.assertEqual(_extract_command_base(""), "")

    def test_multi_digit_suffix(self) -> None:
        self.assertEqual(_extract_command_base("68.12"), "68")


class TestContractFileBase(unittest.TestCase):
    def test_pdf_with_dash_suffix(self) -> None:
        self.assertEqual(_contract_file_base("CDE 52-1.pdf"), "CDE 52")

    def test_pdf_with_dot_suffix(self) -> None:
        self.assertEqual(_contract_file_base("CDE 52.1.pdf"), "CDE 52")

    def test_no_suffix(self) -> None:
        self.assertEqual(_contract_file_base("CDE 2"), "CDE 2")

    def test_empty(self) -> None:
        self.assertEqual(_contract_file_base(""), "")

    def test_pdf_no_suffix(self) -> None:
        self.assertEqual(_contract_file_base("CDE 2.pdf"), "CDE 2")


class TestCommandBaseMatch(unittest.TestCase):
    def test_merge_by_command_base(self) -> None:
        con = _create_test_db()
        # Principal: SFR, cmd "52"
        _insert_lea(con, "LEA-P1", "SFR", "IRU FON", "principal", cmd="52")
        _insert_service(con, "SVC-001", "IRU FON", "SFR")
        _insert_bss(con, "SVC-001", "LEA-P1", "principal", 1)
        # Maintenance: SFR, cmd "52.1"
        _insert_lea(con, "LEA-M1", "SFR", "IRU FON", "maintenance", cmd="52.1")
        _insert_service(con, "SVC-002", "IRU FON", "SFR")
        _insert_bss(con, "SVC-002", "LEA-M1", "maintenance", 0)
        con.commit()

        reconcile_iru_maintenance(con)

        # Maintenance service should be deleted
        remaining = con.execute("SELECT service_id FROM service_master_active").fetchall()
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0][0], "SVC-001")
        # Line count updated
        lc = con.execute("SELECT line_count FROM service_master_active WHERE service_id='SVC-001'").fetchone()[0]
        self.assertEqual(lc, 2)
        # BSS line moved
        bss = con.execute("SELECT service_id FROM service_bss_line WHERE lea_line_id='LEA-M1'").fetchone()
        self.assertEqual(bss[0], "SVC-001")
        # Reconciliation logged
        recon = con.execute("SELECT strategy FROM iru_maintenance_reconciliation").fetchone()
        self.assertEqual(recon[0], "command_base")


class TestContractFileMatch(unittest.TestCase):
    def test_merge_by_contract_file(self) -> None:
        con = _create_test_db()
        # Principal: FREE, contract "CDE 2.pdf", cmd "01.01"
        _insert_lea(con, "LEA-P1", "FREE", "IRU FON", "principal", cmd="01.01", contract="CDE 2.pdf")
        _insert_service(con, "SVC-001", "IRU FON", "FREE")
        _insert_bss(con, "SVC-001", "LEA-P1", "principal", 1)
        # Maintenance: FREE, contract "CDE 2", cmd "02.01" (different cmd base)
        _insert_lea(con, "LEA-M1", "FREE", "IRU FON", "maintenance", cmd="02.01", contract="CDE 2")
        _insert_service(con, "SVC-002", "IRU FON", "FREE")
        _insert_bss(con, "SVC-002", "LEA-M1", "maintenance", 0)
        con.commit()

        reconcile_iru_maintenance(con)

        remaining = con.execute("SELECT service_id FROM service_master_active").fetchall()
        self.assertEqual(len(remaining), 1)
        recon = con.execute("SELECT strategy FROM iru_maintenance_reconciliation").fetchone()
        self.assertEqual(recon[0], "contract_file")


class TestEndpointOverlapMatch(unittest.TestCase):
    def test_merge_by_endpoint_overlap(self) -> None:
        con = _create_test_db()
        # Principal: SFR, no cmd/contract, endpoint "POP Paris"
        _insert_lea(con, "LEA-P1", "SFR", "IRU FON", "principal", ep_a="POP Paris", ep_z="POP Lyon")
        _insert_service(con, "SVC-001", "IRU FON", "SFR")
        _insert_bss(con, "SVC-001", "LEA-P1", "principal", 1)
        # Maintenance: SFR, no cmd/contract, shares endpoint "POP Paris"
        _insert_lea(con, "LEA-M1", "SFR", "IRU FON", "maintenance", ep_a="POP Paris", ep_z="POP Marseille")
        _insert_service(con, "SVC-002", "IRU FON", "SFR")
        _insert_bss(con, "SVC-002", "LEA-M1", "maintenance", 0)
        con.commit()

        reconcile_iru_maintenance(con)

        remaining = con.execute("SELECT service_id FROM service_master_active").fetchall()
        self.assertEqual(len(remaining), 1)
        recon = con.execute("SELECT strategy FROM iru_maintenance_reconciliation").fetchone()
        self.assertEqual(recon[0], "endpoint_overlap")


class TestNoCrossClientMerge(unittest.TestCase):
    def test_different_clients_not_merged(self) -> None:
        con = _create_test_db()
        # Principal: SFR, cmd "52"
        _insert_lea(con, "LEA-P1", "SFR", "IRU FON", "principal", cmd="52")
        _insert_service(con, "SVC-001", "IRU FON", "SFR")
        _insert_bss(con, "SVC-001", "LEA-P1", "principal", 1)
        # Maintenance: FREE, cmd "52.1" (same cmd base but different client)
        _insert_lea(con, "LEA-M1", "FREE", "IRU FON", "maintenance", cmd="52.1")
        _insert_service(con, "SVC-002", "IRU FON", "FREE")
        _insert_bss(con, "SVC-002", "LEA-M1", "maintenance", 0)
        con.commit()

        reconcile_iru_maintenance(con)

        # Both should remain
        remaining = con.execute("SELECT service_id FROM service_master_active ORDER BY service_id").fetchall()
        self.assertEqual(len(remaining), 2)
        recon = con.execute("SELECT COUNT(*) FROM iru_maintenance_reconciliation").fetchone()[0]
        self.assertEqual(recon, 0)


class TestMixedServiceUntouched(unittest.TestCase):
    def test_mixed_service_not_modified(self) -> None:
        con = _create_test_db()
        # Service with both principal and maintenance lines
        _insert_lea(con, "LEA-P1", "SFR", "IRU FON", "principal", cmd="52")
        _insert_lea(con, "LEA-M1", "SFR", "IRU FON", "maintenance", cmd="52.1")
        _insert_service(con, "SVC-001", "IRU FON", "SFR", line_count=2)
        _insert_bss(con, "SVC-001", "LEA-P1", "principal", 1)
        _insert_bss(con, "SVC-001", "LEA-M1", "maintenance", 0)
        con.commit()

        reconcile_iru_maintenance(con)

        remaining = con.execute("SELECT service_id FROM service_master_active").fetchall()
        self.assertEqual(len(remaining), 1)
        lc = con.execute("SELECT line_count FROM service_master_active WHERE service_id='SVC-001'").fetchone()[0]
        self.assertEqual(lc, 2)


class TestNonIruUntouched(unittest.TestCase):
    def test_l2l_services_not_touched(self) -> None:
        con = _create_test_db()
        # L2L service with only principal
        _insert_lea(con, "LEA-P1", "SFR", "Lan To Lan", "principal", cmd="52")
        _insert_service(con, "SVC-001", "Lan To Lan", "SFR")
        _insert_bss(con, "SVC-001", "LEA-P1", "principal", 1)
        # Location FON service with only maintenance
        _insert_lea(con, "LEA-M1", "SFR", "Location FON", "maintenance", cmd="52.1")
        _insert_service(con, "SVC-002", "Location FON", "SFR")
        _insert_bss(con, "SVC-002", "LEA-M1", "maintenance", 0)
        con.commit()

        reconcile_iru_maintenance(con)

        remaining = con.execute("SELECT service_id FROM service_master_active ORDER BY service_id").fetchall()
        self.assertEqual(len(remaining), 2)


class TestUnmatchedStaysSeparate(unittest.TestCase):
    def test_maintenance_without_match_stays(self) -> None:
        con = _create_test_db()
        # Maintenance-only with no matching principal
        _insert_lea(con, "LEA-M1", "SFR", "IRU FON", "maintenance", cmd="99.1")
        _insert_service(con, "SVC-001", "IRU FON", "SFR")
        _insert_bss(con, "SVC-001", "LEA-M1", "maintenance", 0)
        con.commit()

        reconcile_iru_maintenance(con)

        remaining = con.execute("SELECT service_id FROM service_master_active").fetchall()
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0][0], "SVC-001")


class TestLineCountsUpdated(unittest.TestCase):
    def test_line_count_is_sum_after_merge(self) -> None:
        con = _create_test_db()
        # Principal with 2 lines
        _insert_lea(con, "LEA-P1", "SFR", "IRU FON", "principal", cmd="52")
        _insert_lea(con, "LEA-P2", "SFR", "IRU FON", "principal", cmd="52")
        _insert_service(con, "SVC-001", "IRU FON", "SFR", line_count=2)
        _insert_bss(con, "SVC-001", "LEA-P1", "principal", 1)
        _insert_bss(con, "SVC-001", "LEA-P2", "principal", 0)
        # Maintenance with 3 lines
        _insert_lea(con, "LEA-M1", "SFR", "IRU FON", "maintenance", cmd="52.1")
        _insert_lea(con, "LEA-M2", "SFR", "IRU FON", "maintenance", cmd="52.2")
        _insert_lea(con, "LEA-M3", "SFR", "IRU FON", "maintenance", cmd="52.3")
        _insert_service(con, "SVC-002", "IRU FON", "SFR", line_count=3)
        _insert_bss(con, "SVC-002", "LEA-M1", "maintenance", 0)
        _insert_bss(con, "SVC-002", "LEA-M2", "maintenance", 0)
        _insert_bss(con, "SVC-002", "LEA-M3", "maintenance", 0)
        con.commit()

        reconcile_iru_maintenance(con)

        lc = con.execute("SELECT line_count FROM service_master_active WHERE service_id='SVC-001'").fetchone()[0]
        self.assertEqual(lc, 5)
        alc = con.execute("SELECT active_line_count FROM service_master_active WHERE service_id='SVC-001'").fetchone()[0]
        self.assertEqual(alc, 5)
        # All BSS lines point to SVC-001
        bss = con.execute("SELECT COUNT(*) FROM service_bss_line WHERE service_id='SVC-001'").fetchone()[0]
        self.assertEqual(bss, 5)


if __name__ == "__main__":
    unittest.main()
