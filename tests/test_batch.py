"""Tests for non-interactive batch workflow."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

from src import batch
from src.tools.resolution_tools import ensure_agent_tables


class _FakeClient:
    def __init__(self, options):
        self.options = options
        self.queries: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def query(self, prompt: str):
        self.queries.append(prompt)


def test_batch_auto_continues_and_runs_final_sweep(monkeypatch, tmp_path, capsys):
    workspace = tmp_path / "workspace"
    (workspace / "service_ref" / "output").mkdir(parents=True)
    db_path = workspace / "service_ref" / "output" / "service_referential.sqlite"
    con = sqlite3.connect(str(db_path))
    con.execute("CREATE TABLE service_master_active (service_id TEXT PRIMARY KEY)")
    con.execute("INSERT INTO service_master_active VALUES ('SVC-001')")
    con.commit()
    con.close()
    ensure_agent_tables(db_path)

    fake_client = _FakeClient(options={"workspace": str(workspace)})

    monkeypatch.setattr(batch, "create_agent_options", lambda workspace=".": {"workspace": workspace})
    monkeypatch.setattr(batch, "ClaudeSDKClient", lambda options: fake_client)

    responses = iter([
        (["first-pass"], True),
        (["continued"], False),
        (["final-sweep"], False),
    ])

    async def fake_process_stream(client, rich_console=None, spinner=False):
        return next(responses)

    monkeypatch.setattr(batch, "_process_stream", fake_process_stream)

    asyncio.run(batch.batch_run("resolve services", workspace=str(workspace)))

    captured = capsys.readouterr()
    assert fake_client.queries == [
        "resolve services",
        "continue",
        "Avant de terminer, liste les resolutions proposed et valide chacune d'elles.",
    ]
    assert "first-pass" in captured.out
    assert "continued" in captured.out
    assert "final-sweep" in captured.out


def test_batch_warns_if_proposed_remain(monkeypatch, tmp_path, capsys):
    workspace = tmp_path / "workspace"
    (workspace / "service_ref" / "output").mkdir(parents=True)
    db_path = workspace / "service_ref" / "output" / "service_referential.sqlite"
    con = sqlite3.connect(str(db_path))
    con.execute("CREATE TABLE service_master_active (service_id TEXT PRIMARY KEY)")
    con.execute("INSERT INTO service_master_active VALUES ('SVC-001')")
    con.commit()
    con.close()
    ensure_agent_tables(db_path)

    con = sqlite3.connect(str(db_path))
    con.execute(
        """
        INSERT INTO agent_resolutions
        (resolution_id, service_id, confidence, justification, status, evidence_count)
        VALUES ('R1', 'SVC-001', 'low', 'still proposed', 'proposed', 1)
        """
    )
    con.commit()
    con.close()

    fake_client = _FakeClient(options={"workspace": str(workspace)})

    monkeypatch.setattr(batch, "create_agent_options", lambda workspace=".": {"workspace": workspace})
    monkeypatch.setattr(batch, "ClaudeSDKClient", lambda options: fake_client)

    async def fake_process_stream(client, rich_console=None, spinner=False):
        return (["done"], False)

    monkeypatch.setattr(batch, "_process_stream", fake_process_stream)

    asyncio.run(batch.batch_run("resolve services", workspace=str(workspace)))

    captured = capsys.readouterr()
    assert "still in proposed status" in captured.out
