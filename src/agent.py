"""Main agent assembly: MCP server + Claude SDK client + interactive loop."""

from __future__ import annotations

import asyncio
import os

from .sdk_compat import (
    AgentDefinition,
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    MessageParseError,
    ResultMessage,
    SystemMessage,
    TextBlock,
    ToolUseBlock,
    create_sdk_mcp_server,
    message_parser,
)

# --- Patch Claude SDK message parser to prevent stream crash on unknown events ---
_orig_parse = message_parser.parse_message


def _safe_parse(data):
    try:
        return _orig_parse(data)
    except Exception as e:
        if data.get("type", "").endswith("_event"):
            return SystemMessage(subtype="ignored_event", data=data)
        raise e


message_parser.parse_message = _safe_parse
# ---------------------------------------------------------------------------------

from pathlib import Path

from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.status import Status

from .prompts.system_prompt import build_system_prompt
from .tools import db_tools, config_tools, resolution_tools, scoring_tools
from .tools.db_tools import (
    query_db,
    list_tables,
    describe_table,
    fetch_service_context,
    get_service_decision_pack,
    resolve_lea_signal_candidates,
    resolve_network_candidates,
    resolve_optical_candidates,
    resolve_party_candidates,
    resolve_spatial_candidates,
    hunt_site_anchor,
    hunt_vlan,
    hunt_route,
    get_co_cluster,
)
from .tools.config_tools import search_configs, read_config_file
from .tools.resolution_tools import (
    submit_resolution,
    submit_and_validate,
    submit_declared_gap,
    validate_resolution,
    list_resolutions,
)
from .tools.scoring_tools import reconciliation_scorecard, get_review_queue_summary

console = Console()


def create_service_ref_server():
    """Create the MCP server with all service-ref tools."""
    return create_sdk_mcp_server(
        name="service-ref",
        version="1.0.0",
        tools=[
            # Database tools
            query_db,
            list_tables,
            describe_table,
            fetch_service_context,
            get_service_decision_pack,
            resolve_lea_signal_candidates,
            resolve_optical_candidates,
            resolve_network_candidates,
            resolve_party_candidates,
            resolve_spatial_candidates,
            # Hunt tools (chasse attributs cibles)
            hunt_site_anchor,
            hunt_vlan,
            hunt_route,
            get_co_cluster,
            # Config tools
            search_configs,
            read_config_file,
            # Resolution tools
            submit_resolution,
            submit_and_validate,
            submit_declared_gap,
            validate_resolution,
            list_resolutions,
            # Scoring tools
            reconciliation_scorecard,
            get_review_queue_summary,
        ],
    )


def create_agent_options(
    workspace: str = ".",
    api_key: str = "",
    model: str = "opus",
) -> ClaudeAgentOptions:
    """Build the agent configuration.

    Args:
        workspace: Working directory (root of agent-service-ref).
        api_key: Anthropic API key. If empty, falls back to Claude Max auth.
    """
    ws = Path(workspace).resolve()
    db_path = ws / "service_ref" / "output" / "service_referential.sqlite"
    config_dir = ws / "unzipped_equip"
    project_context_path = ws / "project_context.md"

    # Configure all tool modules
    db_tools.configure(db_path)
    config_tools.configure(config_dir)
    resolution_tools.configure(db_path)
    scoring_tools.configure(db_path)

    server = create_service_ref_server()

    system_prompt = build_system_prompt(
        db_path=db_path,
        project_context_path=project_context_path,
    )

    # Prevent nested Claude Code detection when launched from a Claude Code session
    os.environ.pop("CLAUDECODE", None)

    # Auth: if a real API key is provided, pass it through.
    # Otherwise clear ANTHROPIC_API_KEY so the CLI falls back to Claude Max OAuth.
    agent_env: dict[str, str] = {"CLAUDECODE": "", "ANTHROPIC_API_KEY": ""}
    if api_key:
        agent_env["ANTHROPIC_API_KEY"] = api_key

    sub_model = "sonnet" if model == "opus" else model

    return ClaudeAgentOptions(
        system_prompt=system_prompt,
        mcp_servers={"service-ref": server},
        allowed_tools=[
            "Read",
            "Glob",
            "Grep",
            "mcp__service-ref__query_db",
            "mcp__service-ref__list_tables",
            "mcp__service-ref__describe_table",
            "mcp__service-ref__fetch_service_context",
            "mcp__service-ref__get_service_decision_pack",
            "mcp__service-ref__resolve_lea_signal_candidates",
            "mcp__service-ref__resolve_optical_candidates",
            "mcp__service-ref__resolve_network_candidates",
            "mcp__service-ref__resolve_party_candidates",
            "mcp__service-ref__resolve_spatial_candidates",
            "mcp__service-ref__hunt_site_anchor",
            "mcp__service-ref__hunt_vlan",
            "mcp__service-ref__hunt_route",
            "mcp__service-ref__get_co_cluster",
            "mcp__service-ref__search_configs",
            "mcp__service-ref__read_config_file",
            "mcp__service-ref__submit_resolution",
            "mcp__service-ref__submit_and_validate",
            "mcp__service-ref__validate_resolution",
            "mcp__service-ref__list_resolutions",
            "mcp__service-ref__reconciliation_scorecard",
            "mcp__service-ref__get_review_queue_summary",
        ],
        disallowed_tools=[
            "Bash",
            "Write",
            "Edit",
            "AskUserQuestion",
            "EnterPlanMode",
            "ExitPlanMode",
            "WebSearch",
            "WebFetch",
        ],
        model=model,
        agents={
            "general-purpose": AgentDefinition(
                description="General-purpose agent for research and multi-step tasks",
                prompt="",
                model=sub_model,
            ),
            "Explore": AgentDefinition(
                description="Fast agent for exploring codebases",
                prompt="",
                model=sub_model,
            ),
        },
        permission_mode="default",
        max_turns=200,
        cwd=workspace,
        env=agent_env,
    )


def _tool_summary(block: ToolUseBlock) -> str:
    """Return a human-readable one-liner for a tool call."""
    name = block.name.replace("mcp__service-ref__", "")
    inp = block.input if hasattr(block, "input") else {}
    if not isinstance(inp, dict):
        inp = {}

    if name == "Bash":
        cmd = inp.get("command", "")
        return f"Bash — {cmd[:60]}{'…' if len(cmd) > 60 else ''}"
    elif name in ("Read", "Write"):
        path = inp.get("file_path", "?")
        short = os.path.basename(path) if "/" in path else path
        return f"{name} — {short}"
    elif name == "Glob":
        return f"Glob — {inp.get('pattern', '?')}"
    elif name == "Grep":
        return f"Grep — {inp.get('pattern', '?')}"
    elif name == "query_db":
        sql = inp.get("sql", "")
        return f"query_db — {sql[:50]}{'…' if len(sql) > 50 else ''}"
    elif name == "search_configs":
        return f"search_configs — {inp.get('pattern', '?')}"
    elif name == "get_service_decision_pack":
        return f"get_service_decision_pack — {inp.get('service_id', '?')}"
    elif name == "resolve_lea_signal_candidates":
        return f"resolve_lea_signal_candidates — {inp.get('service_id', '?')}"
    elif name == "resolve_optical_candidates":
        return f"resolve_optical_candidates — {inp.get('service_id', '?')}"
    elif name == "resolve_network_candidates":
        return f"resolve_network_candidates — {inp.get('service_id', '?')}"
    elif name == "resolve_spatial_candidates":
        return f"resolve_spatial_candidates — {inp.get('service_id', '?')}"
    elif name == "submit_resolution":
        return f"submit_resolution — {inp.get('service_id', '?')}"
    elif name == "submit_and_validate":
        return f"submit_and_validate — {inp.get('service_id', '?')}"
    elif name == "validate_resolution":
        return f"validate_resolution — {inp.get('service_id', '?')}"
    elif name == "resolve_party_candidates":
        return f"resolve_party_candidates — {inp.get('service_id', '?')}"
    else:
        first_val = next(iter(inp.values()), "") if inp else ""
        return f"{name} — {str(first_val)[:50]}"


_STREAM_TIMEOUT = 120  # seconds per receive_messages iteration


async def _process_stream(
    client: ClaudeSDKClient,
    rich_console: Console | None = None,
    spinner: bool = True,
) -> tuple[list[str], bool, float, int]:
    """Process the agent response stream. Shared between interactive and batch modes.

    Returns:
        (text_blocks, hit_max_turns, total_cost_usd, num_turns) — collected
        text outputs, whether the agent hit the max-turns limit, and the
        execution metrics exposed by the SDK result message.
    """
    text_buffer: list[str] = []
    tool_count = 0
    status: Status | None = None
    hit_max_turns = False
    total_cost_usd = 0.0
    num_turns = 0

    stream = client.receive_messages().__aiter__()
    while True:
        try:
            message = await asyncio.wait_for(
                stream.__anext__(), timeout=_STREAM_TIMEOUT
            )
        except StopAsyncIteration:
            break
        except asyncio.TimeoutError:
            if status is not None:
                status.stop()
                status = None
            text_buffer.append(
                "\n[Timeout: l'agent n'a pas repondu en "
                f"{_STREAM_TIMEOUT}s. Vous pouvez continuer avec un nouveau message.]"
            )
            break
        except MessageParseError:
            continue

        if isinstance(message, AssistantMessage):
            for block in message.content:
                if isinstance(block, TextBlock):
                    if status is not None:
                        status.stop()
                        status = None
                        tool_count = 0
                    text_buffer.append(block.text)

                elif isinstance(block, ToolUseBlock):
                    if rich_console and text_buffer:
                        _flush_text(text_buffer, rich_console)
                        text_buffer.clear()

                    tool_count += 1
                    summary = _tool_summary(block)

                    if rich_console and spinner:
                        if status is None:
                            status = rich_console.status(
                                f"[yellow]{summary}[/yellow]",
                                spinner="dots",
                                spinner_style="yellow",
                            )
                            status.start()
                        else:
                            status.update(
                                f"[yellow]{summary}[/yellow]  [dim]({tool_count} outils)[/dim]"
                            )

        elif isinstance(message, ResultMessage):
            if status is not None:
                status.stop()
                status = None

            if rich_console and text_buffer:
                _flush_text(text_buffer, rich_console)
                text_buffer.clear()

            cost = getattr(message, "total_cost_usd", None)
            turns = getattr(message, "num_turns", None)
            is_end = getattr(message, "is_end_turn", True)
            total_cost_usd = float(cost or 0.0)
            num_turns = int(turns or 0)

            if rich_console:
                info_parts = []
                if cost:
                    info_parts.append(f"${cost:.4f}")
                if turns:
                    info_parts.append(f"{turns} turns")
                if info_parts:
                    rich_console.print(f"  [dim]{' · '.join(info_parts)}[/dim]")

            if not is_end or (turns and turns >= 190):
                hit_max_turns = True

            break

    if status is not None:
        status.stop()

    if rich_console and text_buffer:
        _flush_text(text_buffer, rich_console)
        text_buffer.clear()

    return text_buffer, hit_max_turns, total_cost_usd, num_turns


async def interactive_session(workspace: str = ".", model: str = "opus"):
    """Run the agent in interactive conversation mode."""
    options = create_agent_options(workspace=workspace, model=model)

    console.print(
        Panel(
            "[bold]Agent Service-Ref v0.1.0[/bold]\n"
            f"Espace de travail : [dim]{workspace}[/dim]\n"
            f"Modele : [dim]{model}[/dim]\n"
            "Tapez [bold]quit[/bold] pour quitter.",
            border_style="cyan",
        )
    )

    async with ClaudeSDKClient(options=options) as client:
        next_query: str | None = None

        while True:
            if next_query is not None:
                user_input = next_query
                next_query = None
                console.print("  [dim]↻ auto-continue[/dim]")
            else:
                try:
                    user_input = input("\n\033[32mVous >\033[0m ")
                except (EOFError, KeyboardInterrupt):
                    console.print("\n[dim]Au revoir.[/dim]")
                    break

                if user_input.strip().lower() in ("quit", "exit", "q"):
                    console.print("[dim]Au revoir.[/dim]")
                    break

                if not user_input.strip():
                    continue

            await client.query(user_input)
            _, hit_max_turns, _, _ = await _process_stream(
                client,
                rich_console=console,
                spinner=True,
            )

            if hit_max_turns:
                next_query = "continue"


def _flush_text(buffer: list[str], target_console: Console | None = None) -> None:
    """Render accumulated agent text as markdown inside a panel."""
    text = "\n".join(buffer)
    if not text.strip():
        return
    c = target_console or console
    c.print()
    c.print(
        Panel(
            Markdown(text),
            title="[bold cyan]Agent[/bold cyan]",
            title_align="left",
            border_style="cyan",
            padding=(1, 2),
        )
    )
