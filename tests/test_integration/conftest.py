import uuid
from unittest.mock import MagicMock

import pytest

from cognite.powerops.client._generated.data_classes._core import DomainModelWrite
from cognite.powerops.client.powerops_client import _MAX_DOMAIN_LENGTH, PowerOpsClient
from cognite.powerops.client.shop.cogshop_api import CogShopAPI
from cognite.pygen.utils.external_id_factories import ExternalIdFactory
from tests.mock_powerops import MockPowerOpsModelsClient


@pytest.fixture(scope="session")
def power_ops_client() -> PowerOpsClient:
    mock_cdf = MagicMock()
    mock_cdf.config.project = "power-ops-mock-project"
    mock_cdf.config.base_url = "https://mock_cluster.cognitedata.com"
    mock_powermodel = MockPowerOpsModelsClient()

    # Mirror what PowerOpsClient.__init__ does: set the global external_id_factory
    DomainModelWrite.external_id_factory = ExternalIdFactory.create_external_id_factory(
        prefix_ext_id_factory=ExternalIdFactory(
            ExternalIdFactory.domain_name_factory(),
            shorten_length=_MAX_DOMAIN_LENGTH,
        ),
        override_external_id=False,
    )

    # Bypass PowerOpsClient.__init__ to avoid CogniteClient type validation
    client = object.__new__(PowerOpsClient)
    client.cdf = mock_cdf
    client.powermodel = mock_powermodel
    client.cogshop = CogShopAPI(mock_cdf, mock_powermodel, None)
    return client


def random_external_id(prefix: str) -> str:
    """Generate a random external ID for testing purposes."""
    return f"{prefix}_{uuid.uuid4().hex}"
