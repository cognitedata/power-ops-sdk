from pathlib import Path

import pytest

from cognite.powerops.client.powerops_client import PowerOpsClient
from cognite.powerops.utils.serialization import chdir
from tests.constants import REPO_ROOT


@pytest.fixture(scope="session")
def powerops_client() -> PowerOpsClient:
    settings_toml = Path("settings.toml")
    if not settings_toml.exists():
        with chdir(REPO_ROOT):
            client = PowerOpsClient.from_settings()
    else:
        client = PowerOpsClient.from_settings()
    return client
