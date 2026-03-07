"""Integration tests for CLI prepare command."""

from __future__ import annotations

import shutil
import sqlite3

import pytest

from src.cli import _prepare


class TestPrepare:
    def test_creates_agent_tables(self, realistic_db, tmp_path):
        """_prepare() creates agent tables when DB exists."""
        # Set up workspace structure matching what _prepare expects
        ws = tmp_path / "workspace"
        ws.mkdir()
        (ws / "service_ref" / "output").mkdir(parents=True)
        dest = ws / "service_ref" / "output" / "service_referential.sqlite"
        shutil.copy(str(realistic_db), str(dest))

        _prepare(str(ws))

        # Verify agent tables exist
        con = sqlite3.connect(str(dest))
        tables = [
            r[0]
            for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'agent_%'"
            ).fetchall()
        ]
        con.close()
        assert "agent_resolutions" in tables
        assert "agent_evidence" in tables

    def test_error_when_db_missing(self, tmp_path, capsys):
        """_prepare() shows error if DB missing and pipeline unavailable."""
        ws = tmp_path / "empty_workspace"
        ws.mkdir()

        # Should not crash, just print error
        _prepare(str(ws))
        captured = capsys.readouterr()
        assert "ERREUR" in captured.out or "introuvable" in captured.out
