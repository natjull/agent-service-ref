"""Batch mode: run a single prompt non-interactively."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from .agent import create_agent_options, _process_stream
from .sdk_compat import ClaudeSDKClient


def _count_proposed(db_path: Path) -> int | None:
    if not db_path.exists():
        return None
    con = sqlite3.connect(str(db_path))
    try:
        has_agent = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='agent_resolutions'"
        ).fetchone()
        if not has_agent:
            return None
        return con.execute(
            "SELECT COUNT(*) FROM agent_resolutions WHERE status = 'proposed'"
        ).fetchone()[0]
    finally:
        con.close()


async def batch_run(prompt: str, workspace: str = "."):
    """Run a single batch prompt and print results."""
    options = create_agent_options(workspace=workspace)
    db_path = Path(workspace).resolve() / "service_ref" / "output" / "service_referential.sqlite"

    async with ClaudeSDKClient(options=options) as client:
        await client.query(prompt)
        text_blocks, hit_max_turns = await _process_stream(client, rich_console=None, spinner=False)

        # Print any remaining text blocks (not already flushed by rich console)
        for text in text_blocks:
            print(text)

        max_auto_continue = 5
        auto_continue_count = 0
        while hit_max_turns and auto_continue_count < max_auto_continue:
            auto_continue_count += 1
            await client.query("continue")
            text_blocks, hit_max_turns = await _process_stream(client, rich_console=None, spinner=False)
            for text in text_blocks:
                print(text)

        if hit_max_turns:
            print(
                f"[WARNING] Agent stopped after {max_auto_continue} automatic 'continue' attempts."
            )
        else:
            await client.query(
                "Avant de terminer, liste les resolutions proposed et valide chacune d'elles."
            )
            text_blocks, _ = await _process_stream(client, rich_console=None, spinner=False)
            for text in text_blocks:
                print(text)

        proposed_count = _count_proposed(db_path)
        if proposed_count:
            print(f"[WARNING] {proposed_count} resolution(s) still in proposed status after batch run.")
