"""Batch mode: run a single prompt non-interactively."""

from __future__ import annotations

from claude_agent_sdk import ClaudeSDKClient

from .agent import create_agent_options, _process_stream


async def batch_run(prompt: str, workspace: str = "."):
    """Run a single batch prompt and print results."""
    options = create_agent_options(workspace=workspace)

    async with ClaudeSDKClient(options=options) as client:
        await client.query(prompt)
        text_blocks, _ = await _process_stream(client, rich_console=None, spinner=False)

        # Print any remaining text blocks (not already flushed by rich console)
        for text in text_blocks:
            print(text)
