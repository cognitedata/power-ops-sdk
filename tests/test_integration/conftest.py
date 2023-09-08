from pathlib import Path

import pytest

from cognite.powerops.client.powerops_client import PowerOpsClient, get_powerops_client
from cognite.powerops.utils.serialization import chdir
from tests.constants import REPO_ROOT


@pytest.fixture(scope="session")
def powerops_client() -> PowerOpsClient:
    settings_toml = Path("settings.toml")
    if not settings_toml.exists():
        with chdir(REPO_ROOT):
            client = get_powerops_client()
    else:
        client = get_powerops_client()
    return client
