from rich import print

from cognite.powerops.client.powerops_client import PowerOpsClient
from tests.constants import REPO_ROOT


def test_from_yaml():
    config_path = REPO_ROOT / "powerops_config.yaml"

    po_client = PowerOpsClient.from_config(config_path)

    print(po_client)

    assert po_client
