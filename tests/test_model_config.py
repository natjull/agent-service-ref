"""Tests for model selection and agent option propagation."""

from __future__ import annotations

from pathlib import Path

from src import agent


def test_default_model_is_opus(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(agent, "build_system_prompt", lambda **_: "prompt")
    options = agent.create_agent_options(workspace=str(tmp_path))
    assert options.model == "opus"


def test_model_propagates_to_options(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(agent, "build_system_prompt", lambda **_: "prompt")
    options = agent.create_agent_options(workspace=str(tmp_path), model="sonnet")
    assert options.model == "sonnet"


def test_sub_agents_use_sonnet_when_opus(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(agent, "build_system_prompt", lambda **_: "prompt")
    options = agent.create_agent_options(workspace=str(tmp_path), model="opus")
    assert options.agents["general-purpose"].model == "sonnet"
    assert options.agents["Explore"].model == "sonnet"


def test_sub_agents_match_main_when_sonnet(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(agent, "build_system_prompt", lambda **_: "prompt")
    options = agent.create_agent_options(workspace=str(tmp_path), model="sonnet")
    assert options.agents["general-purpose"].model == "sonnet"
    assert options.agents["Explore"].model == "sonnet"
