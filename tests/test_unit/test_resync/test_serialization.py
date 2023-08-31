from itertools import chain

import pytest
from cognite.powerops.resync import models
from cognite.powerops.resync.config.resync_config import ReSyncConfig
from tests.constants import ReSync
from cognite.powerops.resync.to_models.to_production_model import to_production_model
from cognite.powerops.resync.to_models.to_market_model import to_market_asset_model
from cognite.powerops.resync.to_models.to_cogshop_model import to_cogshop_asset_model


@pytest.fixture(scope="session")
def resync_config() -> ReSyncConfig:
    return ReSyncConfig.from_yamls(ReSync.demo, "power-ops-staging")


@pytest.fixture(scope="session")
def production_model(resync_config: ReSyncConfig) -> models.ProductionModel:
    return to_production_model(resync_config.production)


@pytest.fixture(scope="session")
def market_model(resync_config: ReSyncConfig, production_model: models.ProductionModel) -> models.MarketModel:
    return to_market_asset_model(resync_config.market, production_model.price_areas, "Dayahead")


@pytest.fixture(scope="session")
def cogshop1_model(resync_config: ReSyncConfig, production_model: models.ProductionModel) -> models.CogShop1Asset:
    return to_cogshop_asset_model(resync_config.cogshop, production_model.watercourses, "14.4.3.0")


def test_serialize_production_model_as_cdf(production_model: models.ProductionModel) -> None:
    # Arrange
    local_production = production_model.model_copy(deep=True)
    # In the first serialization, we do not support the content of sequences and files
    for item in chain(local_production.files(), local_production.sequences()):
        item.content = None
    local_production.standardize()

    # Act
    serialized = local_production.dump_as_cdf_resource()
    loaded = models.ProductionModel.load_from_cdf_resources(serialized)

    # Assert
    loaded.standardize()
    assert loaded.model_dump() == local_production.model_dump()


def test_serialize_market_model_as_cdf(market_model: models.MarketModel) -> None:
    # Arrange
    local_market = market_model.model_copy(deep=True)
    # In the first serialization, we do not support the content of sequences and files
    for item in chain(local_market.files(), local_market.sequences()):
        item.content = None
    local_market.standardize()

    # Act
    serialized = local_market.dump_as_cdf_resource()
    loaded = models.MarketModel.load_from_cdf_resources(serialized)

    # Assert
    loaded.standardize()
    assert loaded.model_dump() == local_market.model_dump()


def test_serialize_cogshop1_model_as_cdf(cogshop1_model: models.CogShop1Asset) -> None:
    # Arrange
    local_cogshop = cogshop1_model.model_copy(deep=True)
    # In the first serialization, we do not support the content of sequences and files
    for item in chain(local_cogshop.files(), local_cogshop.sequences()):
        item.content = None
    for external_id in local_cogshop.mappings:
        assert (
            local_cogshop.mappings[external_id].model_dump() == cogshop1_model.mappings[external_id].model_dump()
        ), "Standardization does not maintain the transformation order"
    local_cogshop.standardize()

    # Act
    serialized = local_cogshop.dump_as_cdf_resource()
    loaded = models.CogShop1Asset.load_from_cdf_resources(serialized)

    # Assert
    loaded.standardize()
    # Doing field by field comparison because the model_dump() yields a too large diff which
    # is difficult to read
    for field in loaded.model_fields:
        loaded_value = getattr(loaded, field)
        local_value = getattr(local_cogshop, field)
        if isinstance(loaded_value, list):
            assert [item.model_dump() for item in loaded_value] == [
                item.model_dump() for item in local_value
            ], f"Comparison failed for field {field}"
        elif isinstance(loaded_value, dict):
            assert {k: v.model_dump() for k, v in loaded_value.items()} == {
                k: v.model_dump() for k, v in local_value.items()
            }, f"Comparison failed for field {field}"
        else:
            assert False, f"Comparison failed for field {field}"

    assert loaded.model_dump() == local_cogshop.model_dump()
