from cognite.powerops.client.powerops_client import PowerOpsClient
from tests.constants import REPO_ROOT


def test_from_config():
    config_path = REPO_ROOT / "power_ops_config.yaml"

    po_client = PowerOpsClient.from_config(config_path)  # TODO: remove from configuration the datasets

    assert po_client
