import pytest
import contextlib

import os
from cognite.powerops.clients.powerops_client import get_powerops_client
from cognite.powerops.clients.powerops_client import PowerOpsClient
from pathlib import Path
from tests.constants import REPO_ROOT


@contextlib.contextmanager
def chdir(new_dir: Path) -> None:
    current_working_dir = os.getcwd()
    os.chdir(new_dir)

    try:
        yield

    finally:
        os.chdir(current_working_dir)


@pytest.fixture(scope="session")
def powerops_client() -> PowerOpsClient:
    settings_toml = Path("settings.toml")
    if not settings_toml.exists():
        with chdir(REPO_ROOT):
            client = get_powerops_client()
    else:
        client = get_powerops_client()
    return client
