from __future__ import annotations

import json

import pandas as pd
from cognite.client.data_classes import Sequence

from cognite.powerops.bootstrap.data_classes.bootstrap_config import MarketConfigs
from cognite.powerops.bootstrap.data_classes.marked_configuration import BenchmarkingConfig, PriceScenario
from cognite.powerops.bootstrap.data_classes.marked_configuration._core import map_price_scenarios_by_name
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
from cognite.powerops.bootstrap.models.core import PriceArea


def _to_rkom_market(
    rkom_bid_process: list[RKOMBidProcessConfig],
    price_scenarios_by_id: dict[str, PriceScenario],
    market_name: str,
    rkom_bid_combination_configs: list[RKOMBidCombinationConfig] | None = None,
    rkom_market_config: RkomMarketConfig | None = None,
) -> tuple[ResourceCollection, MarketModel]:
    model = MarketModel()

    if not rkom_market_config:
        # Default market configuration
        rkom_market_config = RkomMarketConfig(
            name="RKOM weekly (Statnett)",
            timezone="Europe/Oslo",
            start_of_week=1,
            external_id="market_configuration_statnett_rkom_weekly",
        )
    rkom_market = market_models.RKOMMarket(**rkom_market_config.model_dump())
    rkom_market.external_id = rkom_market_config.external_id
    model.markets.append(rkom_market)

    bootstrap_resource_collection = ResourceCollection()
    for rkom_process_config in rkom_bid_process:
        bootstrap_resource_collection += rkom_process_config.to_bootstrap_resources(
            price_scenarios_by_id=price_scenarios_by_id, market_name=market_name
        )

    if rkom_bid_combination_configs:
        for config in rkom_bid_combination_configs:
            sequence_external_id = f"RKOM_bid_combination_configuration_{config.auction.value}_{config.name}"
            combination = market_models.RKOMBidCombination(
                name=sequence_external_id.replace("_", " "),
                description="Defining which RKOM bid methods should be combined (into the total bid form)",
                bid=market_models.RKOMCombinationBid(
                    auction=config.auction.value,
                    combination_name=config.name,
                    rkom_bid_configs=config.rkom_bid_config_external_ids,
                ),
            )
            combination.external_id = sequence_external_id
            model.combinations.append(combination)

    return bootstrap_resource_collection, model


def market_to_cdf_resources(
    bootstrap_resources: ResourceCollection,
    config: MarketConfigs,
    market_name: str,
    price_areas: list[PriceArea],
) -> tuple[ResourceCollection, MarketModel]:
    nord_pool = market_models.NordPoolMarket(**config.market.model_dump())
    # The external_id is not following the naming convention, so we need to set it manually
    nord_pool.external_id = config.market.external_id

    benchmarking_processes = _to_benchmarking_process(config.benchmarks)

    dayahead_processes = _to_dayahead_process(
        bid_process_configs=config.bidprocess,
        bidmatrix_generators=config.bidmatrix_generators,
        price_scenarios_by_id=config.price_scenario_by_id,
        benchmarking=benchmarking_processes[0],
        price_areas=price_areas,
    )

    created, rkom = _to_rkom_market(
        rkom_bid_process=config.rkom_bid_process,
        price_scenarios_by_id=config.price_scenario_by_id,
        market_name=market_name,
        rkom_bid_combination_configs=config.rkom_bid_combination,
        rkom_market_config=config.rkom_market,
    )
    bootstrap_resources += created

    model = MarketModel(
        markets=[nord_pool] + rkom.markets,
        processes=dayahead_processes + rkom.processes + benchmarking_processes,
        combinations=rkom.combinations,
    )

    return bootstrap_resources, model


def _to_benchmarking_process(benchmarks: list[BenchmarkingConfig]) -> list[market_models.BenchmarkProcess]:
    benchmarkings: list[market_models.BenchmarkProcess] = []
    if len(benchmarks) > 1:
        # The external id for the benchmarking is hardcoded and thus there can be only one.
        raise NotImplementedError("Only one benchmarking config is supported")
    for benchmarking_config in benchmarks:
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
        # The external_id is not following the naming convention, so we need to set it manually
        process.external_id = "POWEROPS_dayahead_bidding_benchmarking_config"
        benchmarkings.append(process)
    return benchmarkings


def _to_dayahead_process(
    bid_process_configs: list[BidProcessConfig],
    bidmatrix_generators: list[BidMatrixGeneratorConfig],
    price_scenarios_by_id: dict[str, PriceScenario],
    benchmarking: market_models.BenchmarkProcess,
    price_areas: list[PriceArea],
) -> list[market_models.DayAheadProcess]:
    dayahead_processes: list[market_models.DayAheadProcess] = []
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
                    "bid:scenario_name": scenario_name,
                }
                external_id = f"SHOP_{watercourse.name}_incremental_mapping_{process.name}_{scenario_name}"
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
            incremental_mapping=incremental_mapping_sequences,
        )
        # The IDs are inconsistently compared to the other data classes, so we need to set them manually
        dayahead_process.external_id = f"POWEROPS_bid_process_configuration_{process.name}"
        dayahead_process.parent_external_id = "bid_process_configurations"

        dayahead_processes.append(dayahead_process)

    return dayahead_processes
