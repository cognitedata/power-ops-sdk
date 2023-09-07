import pandas as pd
import pytest

from cognite.powerops.client.powerops_client import PowerOpsClient
from cognite.powerops.resync import models
from cognite.powerops.resync.config.resync_config import ReSyncConfig
from cognite.powerops.resync.models.cdf_resources import CDFSequence
from cognite.powerops.resync.to_models.to_market_model import to_market_asset_model
from cognite.powerops.resync.to_models.to_production_model import to_production_model
from tests.constants import ReSync


@pytest.fixture(scope="session")
def resync_config() -> ReSyncConfig:
    return ReSyncConfig.from_yamls(ReSync.demo, "power-ops-staging")


@pytest.fixture(scope="session")
def production_model(resync_config: ReSyncConfig) -> models.ProductionModel:
    return to_production_model(resync_config.production)


@pytest.fixture(scope="session")
def market_model(resync_config: ReSyncConfig, production_model: models.ProductionModel) -> models.MarketModel:
    return to_market_asset_model(resync_config.market, production_model.price_areas, "Dayahead")


def test_sha256_difference(market_model: models.MarketModel, powerops_client: PowerOpsClient):
    # Arrange
    sequence: CDFSequence = next(
        s
        for s in market_model.sequences()
        if s.external_id == "SHOP_Fornebu_incremental_mapping_Fornebu_week_down_day_3-prices_20MW-200MW_+20pct_0MW"
    )
    cdf = powerops_client.cdf
    cdf_sequence = cdf.sequences.retrieve(external_id=sequence.external_id)
    cdf_content = cdf.sequences.data.retrieve_dataframe(0, None, external_id=sequence.external_id)

    # Arrange
    pd.testing.assert_frame_equal(cdf_content, sequence.content)
    key = CDFSequence.content_key_hash
    assert cdf_sequence.metadata[key] == sequence.content.metadata[key]
