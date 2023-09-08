import pytest
from cognite.powerops.resync import models
from cognite.powerops.resync.config._main import ReSyncConfig
from tests.constants import ReSync
from cognite.powerops.resync.models.v1.config_to_model import (
    to_production_model,
    to_market_asset_model,
    to_cogshop_asset_model,
)


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
def cogshop1_model(
    resync_config: ReSyncConfig, production_model: models.ProductionModel, market_model: models.MarketModel
) -> models.CogShop1Asset:
    return to_cogshop_asset_model(
        resync_config.cogshop, production_model.watercourses, "14.4.3.0", market_model.dayahead_processes
    )
