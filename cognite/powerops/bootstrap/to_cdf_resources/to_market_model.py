from __future__ import annotations

import json
from pathlib import Path
from typing import List, Optional

from cognite.powerops.bootstrap.data_classes.bootstrap_config import MarketConfigs
from cognite.powerops.bootstrap.data_classes.core.watercourse import WatercourseConfig
from cognite.powerops.bootstrap.data_classes.marked_configuration import BenchmarkingConfig, PriceScenario
from cognite.powerops.bootstrap.data_classes.marked_configuration._core import RelativeTime
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


def relative_date_to_date_transformation(relative_date: RelativeTime) -> list[market_models.DateTransformation]:
    date_transformations = []
    for operation in relative_date.operations or []:
        transformation_name, args = operation
        arguments = {}
        if isinstance(args, dict):
            arguments["kwargs"] = args
        else:
            arguments["args"] = args if isinstance(args, list) else [args]

        date_transformations.append(
            market_models.DateTransformation(
                transformation=transformation_name,
                **arguments,
            )
        )

    return date_transformations


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

    nord_pool = market_models.NordPoolMarket(**config.market.model_dump())
    # The external_id is not following the naming convention, so we need to set it manually
    nord_pool.external_id = config.market.external_id

    benchmarking = market_models.MarketModel()
    if len(config.benchmarks) > 1:
        # The external id for the benchmarking is hardcoded and thus there can be only one.
        raise NotImplementedError("Only one benchmarking config is supported")
    for benchmarking_config in config.benchmarks:
        bid = market_models.BenchmarkBid(
            date=json.dumps(benchmarking_config.bid_date.operations),
            market_config_external_id=benchmarking_config.market_config_external_id,
        )
        process = market_models.BenchmarkProcess(
            name="Benchmarking config DA",
            bid=bid,
            shop=market_models.ShopTransformation(
                starttime=json.dumps(benchmarking_config.shop_start.operations),
                endtime=json.dumps(benchmarking_config.shop_end.operations),
            ),
            production_plan_time_series=json.dumps(benchmarking_config.production_plan_time_series, ensure_ascii=False)
            if benchmarking_config.production_plan_time_series
            else [],  # ensure_ascii=False to treat Nordic letters properly,
            benchmarking_metrics=benchmarking_config.relevant_shop_objective_metrics,
        )
        process.external_id = "POWEROPS_dayahead_bidding_benchmarking_config"
        benchmarking.processes.append(process)

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
        markets=[nord_pool] + rkom.markets,
        bids=dayahead_market.bids + rkom.bids,
        processes=dayahead_market.processes + rkom.processes + benchmarking.processes,
        combinations=rkom.combinations,
    )

    return bootstrap_resources, model
