from __future__ import annotations

import json
from typing import Any

import pandas as pd
from cognite.client.data_classes import Sequence

from cognite.powerops.resync import config
from cognite.powerops.resync.models._shared_v1_v2.market_model import _map_price_scenarios_by_name
from cognite.powerops.resync.models.base import CDFSequence
from cognite.powerops.resync.models.v1.market import (
    BenchmarkBid,
    BenchmarkProcess,
    DayAheadBid,
    DayAheadProcess,
    MarketModel,
    NordPoolMarket,
    RKOMBid,
    RKOMBidCombination,
    RKOMCombinationBid,
    RKOMMarket,
    RKOMPlants,
    RKOMProcess,
    ShopTransformation,
)
from cognite.powerops.resync.models.v1.production import PriceArea


def to_market_asset_model(
    configuration: config.MarketConfig, price_areas: list[PriceArea], market_name: str
) -> MarketModel:
    nord_pool = NordPoolMarket(**configuration.market.model_dump())
    # The external_id is not following the naming convention, so we need to set it manually
    nord_pool.external_id = configuration.market.external_id

    benchmarking_processes, benchmarking_deferred_attrs = _to_benchmarking_process(configuration.benchmarks)

    dayahead_processes = _to_dayahead_process(
        bid_process_configs=configuration.bidprocess,
        bidmatrix_generators=configuration.bidmatrix_generators,
        price_scenarios_by_id=configuration.price_scenario_by_id,
        benchmarking=benchmarking_processes[0],
        price_areas=price_areas,
    )

    benchmarking_processes = _resolve_deferred_attrs(
        benchmarking_processes=benchmarking_processes,
        deferred_attrs=benchmarking_deferred_attrs,
        dayahead_processes=dayahead_processes,
    )

    rkom = _to_rkom_market(
        rkom_bid_process=configuration.rkom_bid_process,
        price_scenarios_by_id=configuration.price_scenario_by_id,
        market_name=market_name,
        rkom_market_config=configuration.rkom_market,
        rkom_bid_combination_configs=configuration.rkom_bid_combination,
    )

    model = MarketModel(
        nordpool_market=[nord_pool],
        rkom_market=rkom.rkom_market,
        benchmark_processes=benchmarking_processes,
        dayahead_processes=dayahead_processes,
        rkom_processes=rkom.rkom_processes,
        combinations=rkom.combinations,
    )

    return model


_DeferredAttrsT = dict[str, dict[str, Any]]


def _to_benchmarking_process(
    benchmarks: list[config.BenchmarkingConfig],
) -> tuple[list[BenchmarkProcess], _DeferredAttrsT]:
    benchmarkings: list[BenchmarkProcess] = []
    if len(benchmarks) > 1:
        # The external id for the benchmarking is hardcoded and thus there can be only one.
        raise NotImplementedError("Only one benchmarking config is supported")
    deferred_attrs = {}
    for benchmarking_config in benchmarks:
        bid = BenchmarkBid(
            date=json.dumps(benchmarking_config.bid_date.operations),
            market_config_external_id=benchmarking_config.market_config_external_id,
        )
        process = BenchmarkProcess(
            name="Benchmarking config DA",
            bid=bid,
            shop=ShopTransformation(
                starttime=json.dumps(benchmarking_config.shop_start.operations),
                endtime=json.dumps(benchmarking_config.shop_end.operations),
            ),
            production_plan_time_series=json.dumps(benchmarking_config.production_plan_time_series, ensure_ascii=False)
            if benchmarking_config.production_plan_time_series
            else [],  # ensure_ascii=False to treat Nordic letters properly,
            benchmarking_metrics=benchmarking_config.relevant_shop_objective_metrics,
            bid_process_configuration_assets=[],
        )
        # The external_id is not following the naming convention, so we need to set it manually
        process.external_id = "POWEROPS_dayahead_bidding_benchmarking_config"
        benchmarkings.append(process)
        deferred_attrs[process.external_id] = {
            "bid_process_configuration_assets": [
                f"POWEROPS_bid_process_configuration_{name}"
                for name in benchmarking_config.bid_process_configuration_assets
            ],
        }

    return benchmarkings, deferred_attrs


def _resolve_deferred_attrs(
    benchmarking_processes: list[BenchmarkProcess],
    deferred_attrs: _DeferredAttrsT,
    dayahead_processes: list[DayAheadProcess],
) -> list[BenchmarkProcess]:
    for external_id, attrs in deferred_attrs.items():
        benchmarking_process = next(p for p in benchmarking_processes if p.external_id == external_id)
        for attr, deferred_value in attrs.items():
            if attr == "bid_process_configuration_assets":
                value = [p for ext_id in deferred_value for p in dayahead_processes if p.external_id == ext_id]
            else:
                raise NotImplementedError()
            setattr(benchmarking_process, attr, value)
    return benchmarking_processes


def _to_dayahead_process(
    bid_process_configs: list[config.BidProcessConfig],
    bidmatrix_generators: list[config.BidMatrixGeneratorConfig],
    price_scenarios_by_id: dict[str, config.PriceScenario],
    benchmarking: BenchmarkProcess,
    price_areas: list[PriceArea],
) -> list[DayAheadProcess]:
    dayahead_processes: list[DayAheadProcess] = []
    bidmatrix_generators_by_name: dict[str, config.BidMatrixGeneratorConfig] = {g.name: g for g in bidmatrix_generators}
    for process in bid_process_configs:
        price_scenarios_by_name = _map_price_scenarios_by_name(
            process.price_scenarios, price_scenarios_by_id, config.MARKET_BY_PRICE_AREA[process.price_area_name]
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
        bid_matrix_generator_sequence_content = pd.DataFrame(
            [
                [plant.name, bidmatrix_generator.default_method, bidmatrix_generator.default_function_external_id]
                for plant in price_area.plants
            ],
            columns=bidmatrix_generator.column_external_ids,
        )

        # Create incremental mapping sequences
        incremental_mapping_sequences = []
        for watercourse in price_area.watercourses:
            price_scenarios: dict[str, config.PriceScenario] = price_scenarios_by_name
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
                incremental_mapping = CDFSequence(sequence=sequence, content=time_series_mapping.to_dataframe())
                incremental_mapping_sequences.append(incremental_mapping)

        # Create the DayAheadBidProcess Data Class
        dayahead_process = DayAheadProcess(
            name=f"Bid process configuration {process.name}",
            description="Bid process configuration defining how to run a bid matrix generation process",
            bid=DayAheadBid(
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
            shop=ShopTransformation(
                starttime=str(process.shop_start or benchmarking.shop.starttime),
                endtime=str(process.shop_end or benchmarking.shop.endtime),
            ),
            bid_matrix_generator_config=CDFSequence(
                sequence=bid_matrix_generator_sequence, content=bid_matrix_generator_sequence_content
            ),
            incremental_mapping=incremental_mapping_sequences,
        )
        # The IDs are inconsistently compared to the other data classes, so we need to set them manually
        dayahead_process.external_id = f"POWEROPS_bid_process_configuration_{process.name}"
        dayahead_process.type_ = "POWEROPS_bid_process_configuration"

        dayahead_processes.append(dayahead_process)

    return dayahead_processes


def _to_rkom_market(
    rkom_bid_process: list[config.RKOMBidProcessConfig],
    price_scenarios_by_id: dict[str, config.PriceScenario],
    market_name: str,
    rkom_market_config: config.RkomMarketConfig,
    rkom_bid_combination_configs: list[config.RKOMBidCombinationConfig] | None = None,
) -> MarketModel:
    model = MarketModel()

    rkom_market = RKOMMarket(**rkom_market_config.model_dump())
    rkom_market.external_id = rkom_market_config.external_id
    model.rkom_market.append(rkom_market)

    for configuration in rkom_bid_process:
        price_scenarios_by_name = _map_price_scenarios_by_name(
            configuration.price_scenarios, price_scenarios_by_id, market_name
        )
        incremental_mapping_sequences = []
        for scenario_name, price_scenario in price_scenarios_by_name.items():
            mappings = price_scenario.to_time_series_mapping()
            for reserve_scenario in configuration.reserve_scenarios:
                reserve_mapping = reserve_scenario.to_time_series_mapping()
                metadata = {
                    "shop:watercourse": configuration.watercourse,
                    "shop:type": "rkom_incremental_mapping",
                    "bid:scenario_name": scenario_name,
                    "bid:reserve_volume": str(reserve_scenario.volume),
                }
                external_id = (
                    f"SHOP_{configuration.watercourse}_incremental_mapping_"
                    f"{configuration.name}_{scenario_name}_{reserve_scenario.volume}MW"
                )
                name = f"{configuration.watercourse} {scenario_name} {reserve_scenario.volume} MW"
                sequence = Sequence(
                    name=name,
                    external_id=external_id,
                    description="Mapping between SHOP paths and CDF TimeSeries",
                    columns=(mappings + reserve_mapping).column_definitions,
                    metadata=metadata,
                )
                content = (mappings + reserve_mapping).to_dataframe()
                incremental_mapping = CDFSequence(sequence=sequence, content=content)
                incremental_mapping_sequences.append(incremental_mapping)

        process = RKOMProcess(
            name=configuration.name,
            description=f"RKOM bid generation config for {configuration.watercourse}",
            bid=RKOMBid(
                date=str(configuration.bid_date),
                auction=configuration.reserve_scenarios.auction.value,
                block=configuration.reserve_scenarios.block,
                product=configuration.reserve_scenarios.product,
                watercourse=configuration.watercourse,
                method=configuration.method,
                minimum_price=str(configuration.minimum_price),
                price_premium=str(configuration.price_premium),
                price_scenarios=json.dumps(list(price_scenarios_by_name)),
                reserve_scenarios=str(configuration.reserve_scenarios),
            ),
            shop=ShopTransformation(starttime=str(configuration.shop_start), endtime=str(configuration.shop_end)),
            timezone=configuration.timezone,
            rkom=RKOMPlants(plants=json.dumps(sorted(configuration.rkom_plants))),
            incremental_mapping=incremental_mapping_sequences,
        )
        process.external_id = configuration.external_id
        process.type_ = "POWEROPS"
        model.rkom_processes.append(process)

    for configuration in rkom_bid_combination_configs or []:
        sequence_external_id = f"RKOM_bid_combination_configuration_{configuration.auction.value}_{configuration.name}"
        combination = RKOMBidCombination(
            name=sequence_external_id.replace("_", " "),
            description="Defining which RKOM bid methods should be combined (into the total bid form)",
            bid=RKOMCombinationBid(
                auction=configuration.auction.value,
                combination_name=configuration.name,
                rkom_bid_configs=configuration.rkom_bid_config_external_ids,
            ),
        )
        combination.external_id = sequence_external_id
        model.combinations.append(combination)

    return model
