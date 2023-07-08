from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from cognite.powerops.bootstrap.data_classes.bootstrap_config import MarketConfigs
from cognite.powerops.bootstrap.data_classes.core.watercourse import WatercourseConfig
from cognite.powerops.bootstrap.data_classes.marked_configuration import BenchmarkingConfig, PriceScenario
from cognite.powerops.bootstrap.data_classes.marked_configuration.dayahead import (
    BidMatrixGeneratorConfig,
    BidProcessConfig,
)
from cognite.powerops.bootstrap.data_classes.marked_configuration.rkom import (
    RKOMBidCombinationConfig,
    RKOMBidProcessConfig,
    RkomMarketConfig,
)
from cognite.powerops.bootstrap.data_classes.resource_collection import ResourceCollection
from cognite.powerops.bootstrap.models import MarketModel
from cognite.powerops.bootstrap.models import market as market_models
from cognite.powerops.bootstrap.models.core import Watercourse


def process_bid_process_configs(
    path: Path,
    bid_process_configs: list[BidProcessConfig],
    bidmatrix_generators: list[BidMatrixGeneratorConfig],
    price_scenarios_by_id: dict[str, PriceScenario],
    watercourses: list[WatercourseConfig],
    benchmark: BenchmarkingConfig,
    existing_bootstrap_resources: ResourceCollection,
) -> tuple[ResourceCollection, MarketModel]:
    new_resources = ResourceCollection()
    model = MarketModel()
    for bid_process_config in bid_process_configs:
        new_resources += bid_process_config.to_bootstrap_resources(
            path=path,
            bootstrap_resources=existing_bootstrap_resources,
            price_scenarios_by_id=price_scenarios_by_id,
            bid_matrix_generator_configs=bidmatrix_generators,
            watercourses=watercourses,
            benchmark=benchmark,
        )

    return new_resources, model


def process_rkom_bid_configs(
    rkom_bid_combination_configs: Optional[List[RKOMBidCombinationConfig]],
    rkom_market_config: Optional[RkomMarketConfig],
    rkom_bid_process: List[RKOMBidProcessConfig],
    price_scenarios_by_id: dict[str, PriceScenario],
    market_name: str,
) -> tuple[ResourceCollection, MarketModel]:
    bootstrap_resource_collection = ResourceCollection()
    model = MarketModel()

    if not rkom_market_config:
        rkom_market_config = RkomMarketConfig.default()
    bootstrap_resource_collection.add(rkom_market_config.cdf_asset)

    for rkom_process_config in rkom_bid_process:
        bootstrap_resource_collection += rkom_process_config.to_bootstrap_resources(
            price_scenarios_by_id=price_scenarios_by_id, market_name=market_name
        )

    if rkom_bid_combination_configs:
        for rkom_bid_combination_config in rkom_bid_combination_configs:
            bootstrap_resource_collection.add(rkom_bid_combination_config.cdf_asset)

    return bootstrap_resource_collection, model


def market_to_cdf_resources(
    bootstrap_resources: ResourceCollection,
    config: MarketConfigs,
    market_name: str,
    watercourse_configs: list[WatercourseConfig],
    source_path: Path,
    watercourses: list[Watercourse],
) -> tuple[ResourceCollection, MarketModel]:
    print(watercourses[0].name)
    # PowerOps configuration resources
    # nord_pool = market_models.NordPoolMarket(
    #     **config.market.model_dump("external_id"),
    # )
    bootstrap_resources.add(config.market.cdf_asset)
    benchmarking_config_assets = [config.cdf_asset for config in config.benchmarks]
    bootstrap_resources.add(benchmarking_config_assets)
    created, dayahead_market = process_bid_process_configs(
        path=source_path,
        bid_process_configs=config.bidprocess,
        bidmatrix_generators=config.bidmatrix_generators,
        price_scenarios_by_id=config.price_scenario_by_id,
        watercourses=watercourse_configs,
        benchmark=config.benchmarks[0],
        existing_bootstrap_resources=bootstrap_resources.model_copy(),
    )
    bootstrap_resources += created
    created, rkom = process_rkom_bid_configs(
        rkom_bid_combination_configs=config.rkom_bid_combination,
        rkom_market_config=config.rkom_market,
        rkom_bid_process=config.rkom_bid_process,
        price_scenarios_by_id=config.price_scenario_by_id,
        market_name=market_name,
    )
    bootstrap_resources += created

    model = MarketModel(
        # markets=[nord_pool] + rkom.markets,
        bids=dayahead_market.bids + rkom.bids,
        processes=dayahead_market.processes + rkom.processes,
        combinations=rkom.combinations,
    )

    return bootstrap_resources, model
