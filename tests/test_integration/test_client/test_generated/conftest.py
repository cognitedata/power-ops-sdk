import pytest

from cognite.powerops.client import PowerOpsClient
from cognite.powerops.client._generated.v1.data_classes import (
    BidConfigurationDayAhead,
    BidConfigurationDayAheadWrite,
    DateSpecification,
    DateSpecificationWrite,
    MarketConfiguration,
    MarketConfigurationWrite,
    PriceAreaDayAhead,
    PriceAreaDayAheadWrite,
    ShopBasedPartialBidConfigurationList,
    ShopBasedPartialBidConfigurationWrite,
    ShopScenarioSetWrite,
    ShopScenarioWrite,
    WaterValueBasedPartialBidConfigurationList,
    WaterValueBasedPartialBidConfigurationWrite,
)


@pytest.fixture
def new_bid_configuration(power_ops_client) -> BidConfigurationDayAhead:
    external_id = "pytest_bid_configuration"
    bid_config = BidConfigurationDayAheadWrite(
        external_id=external_id,
        name="Test Bid Configuration",
        market_configuration=None,
        price_area=None,
        bid_date_specification=None,
        partials=None,
    )
    power_ops_client.v1.upsert(bid_config)
    yield power_ops_client.v1.day_ahead_configuration.bid_configuration_day_ahead.retrieve(external_id=external_id)
    # Clean up after the test
    power_ops_client.v1.delete(external_id=external_id)
    assert (
        power_ops_client.v1.day_ahead_configuration.bid_configuration_day_ahead.retrieve(external_id=external_id)
        is None
    )


@pytest.fixture
def new_market_configuration(power_ops_client) -> MarketConfiguration:
    external_id = "pytest_market_configuration"
    market_config = MarketConfigurationWrite(
        external_id=external_id,
        name="Test Market Configuration",
        max_price=1000.5,
        min_price=0,
        timezone="UTC",
        price_unit="EUR/MWh",
        price_steps=100,
        tick_size=0.01,
        time_unit="15m",
        trade_lot=0.2,
    )
    power_ops_client.v1.upsert(market_config)
    yield power_ops_client.v1.day_ahead_configuration.market_configuration.retrieve(external_id=external_id)
    # Clean up after the test
    power_ops_client.v1.delete(external_id=external_id)
    assert power_ops_client.v1.day_ahead_configuration.market_configuration.retrieve(external_id=external_id) is None


@pytest.fixture
def new_price_area(power_ops_client) -> PriceAreaDayAhead:
    external_id = "pytest_price_area"
    price_area = PriceAreaDayAheadWrite(
        external_id=external_id,
        name="Test Price Area",
        default_bid_configuration=None,
        main_price_scenario=None,
        price_scenarios=[],
    )
    power_ops_client.v1.upsert(price_area)
    yield power_ops_client.v1.day_ahead_configuration.price_area_day_ahead.retrieve(external_id=external_id)
    # Clean up after the test
    power_ops_client.v1.delete(external_id=external_id)
    assert power_ops_client.v1.day_ahead_configuration.price_area_day_ahead.retrieve(external_id=external_id) is None


@pytest.fixture
def new_date_specification(power_ops_client) -> DateSpecification:
    external_id = "pytest_date_specification"
    date_specification = DateSpecificationWrite(
        external_id=external_id,
        name="Test Date Specification",
        processing_timezone="UTC",
        resulting_timezone="UTC",
        floor_frame="week",
        shift_definition={
            "day": 1,
            "week": 5,
        },
    )
    power_ops_client.v1.upsert(date_specification)
    yield power_ops_client.v1.day_ahead_configuration.date_specification.retrieve(external_id=external_id)
    # Clean up after the test
    power_ops_client.v1.delete(external_id=external_id)
    assert power_ops_client.v1.day_ahead_configuration.date_specification.retrieve(external_id=external_id) is None


@pytest.fixture
def new_partials_shop(power_ops_client) -> ShopBasedPartialBidConfigurationList:
    external_id_prefix = "pytest_partial_shop"
    partials = []
    external_ids = []
    scenarios = [
        ShopScenarioWrite(
            external_id=f"{external_id_prefix}_scenario_{i}",
            name=f"Test Scenario {i}",
        )
        for i in range(2)
    ]
    scenario_set = ShopScenarioSetWrite(
        external_id=f"{external_id_prefix}_scenario_set",
        name="Test Scenario Set",
        scenarios=scenarios,
    )
    for i in range(2):
        external_id = f"{external_id_prefix}_{i}"
        external_ids.append(external_id)

        partial = ShopBasedPartialBidConfigurationWrite(
            external_id=external_id,
            name=f"Test Partial {i}",
            method="SHOP",
            power_asset=None,
            add_steps=False,
            scenario_set=scenario_set,
        )
        partials.append(partial)

    power_ops_client.v1.upsert(partials)

    yield power_ops_client.v1.day_ahead_configuration.shop_based_partial_bid_configuration.retrieve(
        external_id=external_ids,
    )
    # Clean up after the test
    power_ops_client.v1.delete(external_id=external_ids)
    assert (
        len(power_ops_client.v1.day_ahead_configuration.partial_bid_configuration.retrieve(external_id=external_ids))
        == 0
    )


@pytest.fixture
def new_partials_water(power_ops_client) -> WaterValueBasedPartialBidConfigurationList:
    external_id_prefix = "pytest_partial_water"
    partials = []
    external_ids = []

    for i in range(2):
        external_id = f"{external_id_prefix}_shop_{i}"
        external_ids.append(external_id)

        partial = WaterValueBasedPartialBidConfigurationWrite(
            external_id=external_id,
            name=f"Test Partial {i}",
            method="Water Value",
            power_asset=None,
            add_steps=False,
        )
        partials.append(partial)

    power_ops_client.v1.upsert(partials)

    yield power_ops_client.v1.day_ahead_configuration.water_value_based_partial_bid_configuration.retrieve(
        external_id=external_ids,
    )
    # Clean up after the test
    power_ops_client.v1.delete(external_id=external_ids)
    assert (
        len(power_ops_client.v1.day_ahead_configuration.partial_bid_configuration.retrieve(external_id=external_ids))
        == 0
    )


@pytest.fixture
def new_bid_configuration_shop(
    power_ops_client: PowerOpsClient,
    new_bid_configuration: BidConfigurationDayAhead,
    new_market_configuration: MarketConfiguration,
    new_price_area: PriceAreaDayAhead,
    new_date_specification: DateSpecification,
    new_partials_shop: ShopBasedPartialBidConfigurationList,
) -> BidConfigurationDayAhead:
    updated_bid_config = new_bid_configuration.as_write()
    updated_bid_config.market_configuration = new_market_configuration.as_write()
    updated_bid_config.price_area = new_price_area.as_write()
    updated_bid_config.bid_date_specification = new_date_specification.as_write()
    updated_bid_config.partials = [partial.as_write() for partial in new_partials_shop]

    power_ops_client.v1.upsert(updated_bid_config)

    yield power_ops_client.v1.day_ahead_configuration.bid_configuration_day_ahead.retrieve(
        external_id=updated_bid_config.external_id,
        retrieve_connections="full",
    )
