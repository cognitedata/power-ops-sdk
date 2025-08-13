import uuid

import pytest

from cognite.powerops.client.powerops_client import PowerOpsClient
from tests.constants import REPO_ROOT


@pytest.fixture(scope="session")
def power_ops_client() -> PowerOpsClient:
    config_path = REPO_ROOT / "power_ops_config.yaml"

    po_client = PowerOpsClient.from_config(config_path)

    return po_client


def random_external_id(prefix: str) -> str:
    """Generate a random external ID for testing purposes."""
    return f"{prefix}_{uuid.uuid4().hex}"
