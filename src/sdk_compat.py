"""Compatibility layer for claude_agent_sdk during local tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Callable

try:
    from claude_agent_sdk import (  # type: ignore
        AgentDefinition,
        AssistantMessage,
        ClaudeAgentOptions,
        ClaudeSDKClient,
        ResultMessage,
        TextBlock,
        ToolUseBlock,
        create_sdk_mcp_server,
        tool,
    )
    from claude_agent_sdk._errors import MessageParseError  # type: ignore
    import claude_agent_sdk._internal.message_parser as message_parser  # type: ignore
    from claude_agent_sdk.types import SystemMessage  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - exercised only in local test fallback
    class MessageParseError(Exception):
        """Fallback parse error."""


    @dataclass
    class SystemMessage:
        subtype: str
        data: dict[str, Any]


    @dataclass
    class AgentDefinition:
        description: str
        prompt: str
        model: str


    @dataclass
    class ClaudeAgentOptions:
        system_prompt: str
        mcp_servers: dict[str, Any]
        allowed_tools: list[str]
        disallowed_tools: list[str]
        model: str
        agents: dict[str, AgentDefinition]
        permission_mode: str
        max_turns: int
        cwd: str
        env: dict[str, str] = field(default_factory=dict)


    @dataclass
    class TextBlock:
        text: str


    @dataclass
    class ToolUseBlock:
        name: str
        input: dict[str, Any]


    @dataclass
    class AssistantMessage:
        content: list[Any]


    @dataclass
    class ResultMessage:
        total_cost_usd: float | None = None
        num_turns: int | None = None
        is_end_turn: bool = True


    class ClaudeSDKClient:
        def __init__(self, options: ClaudeAgentOptions):
            self.options = options

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def query(self, prompt: str) -> None:
            return None

        async def receive_messages(self) -> AsyncIterator[Any]:
            if False:
                yield None


    class _ToolWrapper:
        def __init__(self, handler: Callable[[dict[str, Any]], Any], name: str, description: str, schema: dict[str, Any]):
            self.handler = handler
            self.name = name
            self.description = description
            self.schema = schema

        def __call__(self, *args: Any, **kwargs: Any) -> Any:
            return self.handler(*args, **kwargs)


    def tool(name: str, description: str, schema: dict[str, Any]):
        def decorator(func: Callable[[dict[str, Any]], Any]) -> _ToolWrapper:
            return _ToolWrapper(func, name=name, description=description, schema=schema)

        return decorator


    def create_sdk_mcp_server(name: str, version: str, tools: list[Any]) -> dict[str, Any]:
        return {"name": name, "version": version, "tools": tools}


    class _MessageParserModule:
        @staticmethod
        def parse_message(data: dict[str, Any]) -> dict[str, Any]:
            return data


    message_parser = _MessageParserModule()
