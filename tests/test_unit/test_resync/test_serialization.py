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
    # Act
    serialized = production_model.dump_as_cdf_resource()
    loaded = models.ProductionModel.load_from_cdf_resources(serialized)

    # Assert
    assert loaded == production_model
