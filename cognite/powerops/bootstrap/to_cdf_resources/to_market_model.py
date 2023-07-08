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


def process_bid_process_configs(
    path: Path,
    bid_process_configs: list[BidProcessConfig],
    bidmatrix_generators: list[BidMatrixGeneratorConfig],
    price_scenarios_by_id: dict[str, PriceScenario],
    watercourses: list[WatercourseConfig],
    benchmark: BenchmarkingConfig,
    existing_bootstrap_resources: ResourceCollection,
) -> ResourceCollection:
    new_resources = ResourceCollection()
    for bid_process_config in bid_process_configs:
        new_resources += bid_process_config.to_bootstrap_resources(
            path=path,
            bootstrap_resources=existing_bootstrap_resources,
            price_scenarios_by_id=price_scenarios_by_id,
            bid_matrix_generator_configs=bidmatrix_generators,
            watercourses=watercourses,
            benchmark=benchmark,
        )

    return new_resources


def process_rkom_bid_configs(
    rkom_bid_combination_configs: Optional[List[RKOMBidCombinationConfig]],
    rkom_market_config: Optional[RkomMarketConfig],
    rkom_bid_process: List[RKOMBidProcessConfig],
    price_scenarios_by_id: dict[str, PriceScenario],
    market_name: str,
) -> ResourceCollection:
    bootstrap_resource_collection = ResourceCollection()

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

    return bootstrap_resource_collection


def market_to_cdf_resources(
    bootstrap_resources: ResourceCollection,
    markets: MarketConfigs,
    market_name: str,
    watercourses: list[WatercourseConfig],
    source_path: Path,
) -> ResourceCollection:
    # PowerOps configuration resources
    bootstrap_resources.add(markets.market.cdf_asset)
    benchmarking_config_assets = [config.cdf_asset for config in markets.benchmarks]
    bootstrap_resources.add(benchmarking_config_assets)
    bootstrap_resources += process_bid_process_configs(
        path=source_path,
        bid_process_configs=markets.bidprocess,
        bidmatrix_generators=markets.bidmatrix_generators,
        price_scenarios_by_id=markets.price_scenario_by_id,
        watercourses=watercourses,
        benchmark=markets.benchmarks[0],
        existing_bootstrap_resources=bootstrap_resources.model_copy(),
    )
    bootstrap_resources += process_rkom_bid_configs(
        rkom_bid_combination_configs=markets.rkom_bid_combination,
        rkom_market_config=markets.rkom_market,
        rkom_bid_process=markets.rkom_bid_process,
        price_scenarios_by_id=markets.price_scenario_by_id,
        market_name=market_name,
    )
    return bootstrap_resources
