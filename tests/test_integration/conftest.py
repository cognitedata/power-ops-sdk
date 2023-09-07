import pytest

from cognite.powerops.client.powerops_client import get_powerops_client
from cognite.powerops.client.powerops_client import PowerOpsClient
from pathlib import Path
from tests.constants import REPO_ROOT
from tests.utils import chdir


@pytest.fixture(scope="session")
def powerops_client() -> PowerOpsClient:
    settings_toml = Path("settings.toml")
    if not settings_toml.exists():
        with chdir(REPO_ROOT):
            client = get_powerops_client()
    else:
        client = get_powerops_client()
    return client
