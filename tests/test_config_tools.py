"""Integration tests for config_tools."""

from __future__ import annotations

import asyncio

import pytest

from src.tools import config_tools

_search = config_tools.search_configs.handler
_read = config_tools.read_config_file.handler


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture
def config_dir(tmp_path):
    """Create a temp config directory with sample config files."""
    cfg = tmp_path / "configs"
    cfg.mkdir()

    # Cisco config
    cisco = cfg / "DSP_TELOISE_rancidIP_par1-co-1.teloise.net_FILTRED.txt"
    cisco.write_text(
        "interface GigabitEthernet0/0/1\n"
        " description ACME L2L SVC-001\n"
        " switchport mode access\n"
        " switchport access vlan 100\n"
        "!\n"
        "interface GigabitEthernet0/0/2\n"
        " description GLOBEX TRUNK\n"
        "!\n",
        encoding="utf-8",
    )

    # Huawei config
    huawei = cfg / "sw-lyo1-config.txt"
    huawei.write_text(
        "#\n"
        "interface GE0/0/1\n"
        " description ACME_L2L_100M\n"
        " port link-type access\n"
        " port default vlan 200\n"
        "#\n",
        encoding="utf-8",
    )

    # RAD config
    rad = cfg / "rad_cpe_3328.txt"
    rad.write_text(
        "system name RAD-3328-ACME\n"
        "eth 1/1\n"
        " description ACME site Paris\n"
        "!\n",
        encoding="utf-8",
    )

    config_tools.configure(cfg)
    return cfg


class TestSearchConfigs:
    def test_finds_matches_with_context(self, config_dir):
        result = _run(_search({"pattern": "ACME"}))
        text = result["content"][0]["text"]
        assert "ACME" in text
        assert "matches" in text

    def test_device_filter(self, config_dir):
        result = _run(_search({"pattern": "description", "device_filter": "par1-co-1"}))
        text = result["content"][0]["text"]
        assert "par1-co-1" in text

    def test_vendor_filter(self, config_dir):
        result = _run(_search({"pattern": "description", "vendor_filter": "huawei"}))
        text = result["content"][0]["text"]
        # Huawei file should match
        assert "Huawei" in text
        # Cisco file should NOT match
        assert "par1-co-1" not in text

    def test_no_match(self, config_dir):
        result = _run(_search({"pattern": "ZZZZNONEXISTENT"}))
        text = result["content"][0]["text"]
        assert "No matches" in text


class TestReadConfigFile:
    def test_reads_content(self, config_dir):
        result = _run(_read({"file_name": "rad_cpe_3328.txt"}))
        text = result["content"][0]["text"]
        assert "RAD-3328-ACME" in text
        assert "RAD" in text  # vendor detected

    def test_refuses_file_outside_config_dir(self, config_dir, tmp_path):
        # Create a file outside config_dir
        outside = tmp_path / "secret.txt"
        outside.write_text("SECRET DATA")
        result = _run(_read({"file_name": "../secret.txt"}))
        text = result["content"][0]["text"]
        # Should not return the secret data
        assert "SECRET DATA" not in text

    def test_rejects_non_txt(self, config_dir):
        result = _run(_read({"file_name": "something.py"}))
        text = result["content"][0]["text"]
        assert "ERROR" in text
        assert ".txt" in text
