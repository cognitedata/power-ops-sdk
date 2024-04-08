from __future__ import annotations

from pathlib import Path

from cognite.powerops import PowerOpsClient


def apply2(config_dir: Path, client: PowerOpsClient | None = None) -> None:
    client = client or PowerOpsClient.from_settings()
