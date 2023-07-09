from __future__ import annotations

import json
from pathlib import Path
from typing import List, Optional

import pandas as pd
from cognite.client.data_classes import Sequence

from cognite.powerops.bootstrap.data_classes.bootstrap_config import MarketConfigs
from cognite.powerops.bootstrap.data_classes.core.watercourse import WatercourseConfig
from cognite.powerops.bootstrap.data_classes.marked_configuration import BenchmarkingConfig, PriceScenario
from cognite.powerops.bootstrap.data_classes.marked_configuration._core import RelativeTime, map_price_scenarios_by_name
from cognite.powerops.bootstrap.data_classes.marked_configuration.dayahead import (
    BidMatrixGeneratorConfig,
    BidProcessConfig,
)
from cognite.powerops.bootstrap.data_classes.marked_configuration.market import MARKET_BY_PRICE_AREA
from cognite.powerops.bootstrap.data_classes.marked_configuration.rkom import (
    RKOMBidCombinationConfig,
    RKOMBidProcessConfig,
    RkomMarketConfig,
)
from cognite.powerops.bootstrap.data_classes.resource_collection import ResourceCollection
from cognite.powerops.bootstrap.models import MarketModel
from cognite.powerops.bootstrap.models import market as market_models
from cognite.powerops.bootstrap.models.base import CDFSequence
from cognite.powerops.bootstrap.models.core import PriceArea, Watercourse


def process_bid_process_configs(
    path: Path,
    bid_process_configs: list[BidProcessConfig],
    bidmatrix_generators: list[BidMatrixGeneratorConfig],
    price_scenarios_by_id: dict[str, PriceScenario],
    watercourse_configs: list[WatercourseConfig],
    benchmark: BenchmarkingConfig,
    existing_bootstrap_resources: ResourceCollection,
    watercourses: list[Watercourse],
    benchmarking: market_models.BenchmarkProcess,
    price_areas: list[PriceArea],
) -> tuple[ResourceCollection, MarketModel]:
    new_resources = ResourceCollection()
    model = MarketModel()
    bidmatrix_generators_by_name: dict[str, BidMatrixGeneratorConfig] = {g.name: g for g in bidmatrix_generators}
    for process in bid_process_configs:
        price_scenarios_by_name = map_price_scenarios_by_name(
            process.price_scenarios, price_scenarios_by_id, MARKET_BY_PRICE_AREA[process.price_area_name]
        )

        price_area = next((pa for pa in price_areas if pa.name == process.price_area_name), None)
        if not price_area:
            raise ValueError(f"Price area {process.price_area_name} not found")

        # Create bid matrix generator config sequence
        bidmatrix_generator = bidmatrix_generators_by_name[process.bid_matrix_generator]
        column_def = [
            {"valueType": "STRING", "externalId": external_id}
            for external_id in bidmatrix_generator.column_external_ids
        ]
        bid_matrix_generator_sequence = Sequence(
            external_id=f"POWEROPS_bid_matrix_generator_config_{process.name}",
            name=f"POWEROPS bid matrix generator config {process.name}",
            description="Configuration of bid matrix generation method to use for each plant in the price area",
            columns=column_def,
            metadata={
                # TODO: Rename this from "shop:type" to something without "shop"
                #   (but check if e.g. power-ops-functions uses this)
                "bid:price_area": f"price_area_{process.price_area_name}",
                "shop:type": "bid_matrix_generator_config",
                "bid:bid_process_configuration_name": process.name,
            },
        )
        content = pd.DataFrame(
            [
                [plant.name, bidmatrix_generator.default_method, bidmatrix_generator.default_function_external_id]
                for plant in price_area.plants
            ],
            columns=bidmatrix_generator.column_external_ids,
        )

        # Create incremental mapping sequences
        incremental_mapping_sequences = []
        for watercourse in price_area.watercourses:
            price_scenarios: dict[str, PriceScenario] = price_scenarios_by_name
            if process.price_scenarios_per_watercourse:
                try:
                    price_scenarios = {
                        scenario_name: price_scenarios[scenario_name]
                        for scenario_name in process.price_scenarios_per_watercourse[watercourse.name]
                    }
                except KeyError as e:
                    raise KeyError(
                        f"Watercourse {watercourse.name} not defined in price_scenarios_per_watercourse "
                        f"for BidProcessConfig {process.name}"
                    ) from e
            for scenario_name, price_scenario in price_scenarios.items():
                time_series_mapping = price_scenario.to_time_series_mapping()
                metadata = {
                    "shop:watercourse": watercourse.name,
                    "shop:type": "incremental_mapping",
                    "bid:scenario_name": price_area.name,
                }
                external_id = f"SHOP_{watercourse.name}_incremental_mapping_{process.name}_{price_area.name}"
                name = external_id.replace("_", " ")
                sequence = Sequence(
                    name=name,
                    external_id=external_id,
                    description="Mapping between SHOP paths and CDF TimeSeries",
                    columns=time_series_mapping.column_definitions,
                    metadata=metadata,
                )
                content = time_series_mapping.to_dataframe()
                incremental_mapping = CDFSequence(sequence=sequence, content=content)
                incremental_mapping_sequences.append(incremental_mapping)

        # Create the DayAheadBidProcess Data Class
        dayahead_process = market_models.DayAheadProcess(
            name=f"Bid process configuration {process.name}",
            description="Bid process configuration defining how to run a bid matrix generation process",
            bid=market_models.DayAheadBid(
                date=str(process.bid_date or benchmarking.bid.date),
                market_config_external_id=benchmarking.bid.market_config_external_id,
                is_default_config_for_price_area=process.is_default_config_for_price_area,
                main_scenario=process.main_scenario,
                price_area=f"price_area_{process.price_area_name}",
                price_scenarios={
                    scenario_name: scenario.time_series_external_id
                    for scenario_name, scenario in price_scenarios_by_name.items()
                },
                no_shop=process.no_shop,
                bid_process_configuration_name=process.name,
                bid_matrix_generator_config_external_id=f"POWEROPS_bid_matrix_generator_config_{process.name}",
            ),
            shop=market_models.ShopTransformation(
                starttime=str(process.shop_start or benchmarking.shop.starttime),
                endtime=str(process.shop_end or benchmarking.shop.endtime),
            ),
            bid_matrix_generator_config=CDFSequence(
                sequence=bid_matrix_generator_sequence,
                content=content,
            ),
            # incremental_mapping=incremental_mapping_sequences,
        )
        # The IDs are inconsistently compared to the other data classes, so we need to set them manually
        dayahead_process.external_id = f"POWEROPS_bid_process_configuration_{process.name}"
        dayahead_process.parent_external_id = "bid_process_configurations"

        model.processes.append(dayahead_process)

        created = process.to_bootstrap_resources(
            path=path,
            bootstrap_resources=existing_bootstrap_resources,
            price_scenarios_by_id=price_scenarios_by_id,
            bid_matrix_generator_configs=bidmatrix_generators,
            watercourses=watercourse_configs,
            benchmark=benchmark,
        )
        if created is not None:
            new_resources += created

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
    price_areas: list[PriceArea],
) -> tuple[ResourceCollection, MarketModel]:
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
        watercourse_configs=watercourse_configs,
        benchmark=config.benchmarks[0],
        existing_bootstrap_resources=bootstrap_resources.model_copy(),
        watercourses=watercourses,
        benchmarking=benchmarking.processes[0],
        price_areas=price_areas,
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
