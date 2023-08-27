from itertools import chain

import pytest
from cognite.powerops.resync import models
from cognite.powerops.resync.config.resync_config import ReSyncConfig
from tests.constants import ReSync
from cognite.powerops.resync.to_models.to_production_model import to_production_model


@pytest.fixture(scope="session")
def production_model() -> models.ProductionModel:
    config = ReSyncConfig.from_yamls(ReSync.demo, "power-ops-staging")
    return to_production_model(config.production)


def test_serialize_production_model_as_cdf(production_model: models.ProductionModel) -> None:
    # Arrange
    local_production = production_model.model_copy(deep=True)
    # In the first serialization, we do not support the content of sequences and files
    for item in chain(local_production.files(), local_production.sequences()):
        item.content = None
    local_production.sort_listed_asset_types()

    # Act
    serialized = local_production.dump_as_cdf_resource()
    loaded = models.ProductionModel.load_from_cdf_resources(serialized)

    # Assert
    loaded.sort_listed_asset_types()
    assert loaded.model_dump() == local_production.model_dump()
