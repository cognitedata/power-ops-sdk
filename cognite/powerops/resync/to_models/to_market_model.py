from __future__ import annotations

import json

import pandas as pd
from cognite.client.data_classes import Sequence

from cognite.powerops.clients.data_classes import (
    BenchmarkBidApply,
    BenchmarkProcesApply,
    BidMatrixGeneratorApply,
    DayAheadBidApply,
    DayAheadProcesApply,
    NordPoolMarketApply,
    PriceAreaApply,
    ProductionPlanTimeSeriesApply,
    ReserveScenarioApply,
    RKOMBidApply,
    RKOMBidCombinationApply,
    RKOMCombinationBidApply,
    RKOMMarketApply,
    RKOMProcesApply,
    ScenarioMappingApply,
)
from cognite.powerops.resync.config.market import BenchmarkingConfig, PriceScenario, PriceScenarioID
from cognite.powerops.resync.config.market.dayahead import BidMatrixGeneratorConfig, BidProcessConfig
from cognite.powerops.resync.config.market.market import MARKET_BY_PRICE_AREA
from cognite.powerops.resync.config.market.rkom import RKOMBidCombinationConfig, RKOMBidProcessConfig, RkomMarketConfig
from cognite.powerops.resync.config.resync_config import MarketConfig
from cognite.powerops.resync.models import MarketModel
from cognite.powerops.resync.models import market as market_models
from cognite.powerops.resync.models.cdf_resources import CDFSequence
from cognite.powerops.resync.models.market_dm import (
    BenchmarkMarketDataModel,
    DayAheadMarketDataModel,
    RKOMMarketDataModel,
)
from cognite.powerops.resync.models.production import PriceArea
from cognite.powerops.resync.utils.common import make_ext_id

from ._to_instances import (
    _to_date_transformations,
    _to_scenario_mapping,
    _to_shop_transformation,
    _to_input_timeseries_mapping,
)


def to_market_asset_model(config: MarketConfig, price_areas: list[PriceArea], market_name: str) -> MarketModel:
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

    rkom = _to_rkom_market(
        rkom_bid_process=config.rkom_bid_process,
        price_scenarios_by_id=config.price_scenario_by_id,
        market_name=market_name,
        rkom_market_config=config.rkom_market,
        rkom_bid_combination_configs=config.rkom_bid_combination,
    )

    model = MarketModel(
        markets=rkom.markets + [nord_pool],
        benchmark_processes=benchmarking_processes,
        dayahead_processes=dayahead_processes,
        rkom_processes=rkom.rkom_processes,
        combinations=rkom.combinations,
    )

    return model


def to_benchmark_data_model(configs: list[BenchmarkingConfig]) -> BenchmarkMarketDataModel:
    if len(configs) > 1:
        # The external id for the benchmarking is hardcoded and thus there can be only one.
        raise NotImplementedError("Only one benchmarking config is supported")
    model = BenchmarkMarketDataModel()
    for config in configs:
        bid = BenchmarkBidApply(
            external_id=make_ext_id(
                [config.bid_date.model_dump_json(), config.market_config_external_id], BenchmarkBidApply
            ),
            date=_to_date_transformations(config.bid_date),
            market=config.market_config_external_id,
        )
        model.bids[bid.external_id] = bid
        process = BenchmarkProcesApply(
            external_id="benchmarking_config_day_ahead",
            name="Benchmarking config DayAhead",
            bid=bid.external_id,
            shop=_to_shop_transformation(config.shop_start, config.shop_end),
            production_plan_time_series=[
                ProductionPlanTimeSeriesApply(
                    external_id=make_ext_id([name, series], ProductionPlanTimeSeriesApply),
                    name=name,
                    series=series,
                )
                for name, series in config.production_plan_time_series
            ],
            run_events=[],
            metrics=config.relevant_shop_objective_metrics,
        )
        model.benchmarking.append(process)
    return model


def to_dayahead_data_model(
    config: MarketConfig,
    dayahead_benchmark: BenchmarkProcesApply,
    benchmark_bid: BenchmarkBidApply,
    price_areas: list[PriceAreaApply],
) -> DayAheadMarketDataModel:
    model = DayAheadMarketDataModel()

    bidmatrix_generators_by_name: dict[str, BidMatrixGeneratorConfig] = {g.name: g for g in config.bidmatrix_generators}

    model.nordpool_market = NordPoolMarketApply(**config.market.model_dump())

    for process in config.bidprocess:
        price_scenarios_by_name = _map_price_scenarios_by_name(
            process.price_scenarios, config.price_scenario_by_id, MARKET_BY_PRICE_AREA[process.price_area_name]
        )

        price_area = next((pa for pa in price_areas if pa.name == process.price_area_name), None)
        if not price_area:
            raise ValueError(f"Price area {process.price_area_name} not found")

        # Create bid matrix generators
        bid_gen_config = bidmatrix_generators_by_name[process.bid_matrix_generator]
        bid_matrix_generators = _to_bid_matrix_generator(bid_gen_config, price_area, process.name)
        model.bid_matrix_generator.update(bid_matrix_generators)

        # Create incremental mapping sequences
        incremental_mappings = []
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
                external_id = f"SHOP_{watercourse.name}_incremental_mapping_{process.name}_{scenario_name}"
                scenario = _to_scenario_mapping(external_id, scenario_name, price_scenario.to_time_series_mapping())
                incremental_mappings.append(scenario)

        shop = _to_shop_transformation(
            process.shop_start or dayahead_benchmark.shop.start, process.shop_end or dayahead_benchmark.shop.end
        )

        bid = DayAheadBidApply(
            external_id=f"POWEROPS_bid_process_configuration_{process.name}_bid",
            date=_to_date_transformations(process.bid_date or benchmark_bid.date),
            market=model.nordpool_market.external_id,
            is_default_config_for_price_area=process.is_default_config_for_price_area,
            main_scenario=process.main_scenario,
            price_area=f"price_area_{process.price_area_name}",
            # watercourse=process.watercourse,
            price_scenarios=[
                ScenarioMappingApply(
                    external_id=f"SHOP_incremental_mapping_{process.name}_{scenario_name}",
                    mapping_override=[
                        _to_input_timeseries_mapping(entry) for entry in price_scenario.to_time_series_mapping()
                    ],
                    name=scenario_name,
                    shop_type="incremental_mapping",
                )
                for scenario_name, price_scenario in price_scenarios_by_name.items()
            ],
            no_shop=process.no_shop,
            bid_process_configuration_name=process.name,
            bid_matrix_generator_config_external_id=f"POWEROPS_bid_matrix_generator_config_{process.name}",
        )
        model.bids[bid.external_id] = bid

        # Create the DayAheadBidProcess Data Class
        dayahead_process = DayAheadProcesApply(
            name=f"Bid process configuration {process.name}",
            external_id=f"POWEROPS_bid_process_configuration_{process.name}",
            bid=bid.external_id,
            shop=shop,
            bid_matrix_generator_config=list(bid_matrix_generators.values()),
            incremental_mappings=incremental_mappings,
        )

        model.dayahead_processes.append(dayahead_process)

    return model


def _to_bid_matrix_generator(
    bid_gen_config: BidMatrixGeneratorConfig, price_area: PriceAreaApply, process_name: str
) -> dict[str, BidMatrixGeneratorApply]:
    return {
        (
            external_id := f"POWEROPS_bid_matrix_generator_config_{process_name}"
            # make_ext_id(
            #     [plant.name, bid_gen_config.default_method, bid_gen_config.default_function_external_id],
            #     BidMatrixGeneratorApply,
            # )
        ): BidMatrixGeneratorApply(
            external_id=external_id,
            shop_plant=plant.name,
            methods=bid_gen_config.default_method,
            function_external_id=bid_gen_config.default_function_external_id,
        )
        for plant in price_area.plants
    }


def to_rkom_data_model(config: MarketConfig, market_name: str) -> RKOMMarketDataModel:
    model = RKOMMarketDataModel()

    model.rkom_market = RKOMMarketApply(**config.rkom_market.model_dump())

    for process in config.rkom_bid_process:
        price_scenarios_by_name = _map_price_scenarios_by_name(
            process.price_scenarios,
            config.price_scenario_by_id,
            market_name,
        )

        incremental_mappings = []
        reserve_scenarios = []
        scenario_mappings = []
        for scenario_name, price_scenario in price_scenarios_by_name.items():
            external_id = f"SHOP_RKOM_{process.watercourse}_incremental_mapping_{process.name}_{scenario_name}"
            scenario_mapping = _to_scenario_mapping(external_id, scenario_name, price_scenario.to_time_series_mapping())
            scenario_mappings.append(scenario_mapping)
            for reserve_scenario in process.reserve_scenarios.list_scenarios():
                name = (
                    f"{reserve_scenario.auction.name}_{reserve_scenario.block}_{reserve_scenario.product}_"
                    f"{reserve_scenario.reserve_group}_{reserve_scenario.volume}"
                )
                external_id = (
                    f"SHOP_{process.watercourse}_incremental_mapping_"
                    f"{process.name}_{scenario_name}_{reserve_scenario.volume}MW"
                )
                reserve_mapping = _to_scenario_mapping(
                    external_id,
                    name,
                    price_scenario.to_time_series_mapping() + reserve_scenario.to_time_series_mapping(),
                )
                incremental_mappings.append(reserve_mapping)

                external_id = make_ext_id(
                    reserve_mapping.model_dump(exclude={"obligation_external_id", "mip_plant_time_series"}),
                    ReserveScenarioApply,
                )
                apply = ReserveScenarioApply(
                    external_id=external_id,
                    auction=reserve_scenario.auction.name,
                    block=reserve_scenario.block,
                    product=reserve_scenario.product,
                    reserve_group=reserve_scenario.reserve_group,
                    volume=reserve_scenario.volume,
                )
                reserve_scenarios.append(apply)

        bid = RKOMBidApply(
            external_id=f"{process.external_id}_bid",
            date=_to_date_transformations(process.bid_date),
            watercourse=process.watercourse,
            method=process.method,
            minimum_price=process.minimum_price,
            price_premium=process.price_premium,
            price_scenarios=scenario_mappings,
            reserve_scenarios=reserve_scenarios,
            market=model.rkom_market.external_id,
        )
        model.bids[bid.external_id] = bid
        shop = _to_shop_transformation(process.shop_start, process.shop_end)

        apply = RKOMProcesApply(
            external_id=process.external_id,
            name=process.name,
            bid=bid.external_id,
            shop=shop,
            timezone=process.timezone,
            plants=process.rkom_plants,
            incremental_mappings=incremental_mappings,
        )
        model.rkom_processes.append(apply)

    for comb in config.rkom_bid_combination or []:
        sequence_external_id = f"RKOM_bid_combination_configuration_{comb.auction.value}_{comb.name}"

        bid = RKOMCombinationBidApply(
            external_id=make_ext_id(
                [comb.auction.value, comb.name] + comb.rkom_bid_config_external_ids, RKOMCombinationBidApply
            ),
            auction=comb.auction.value,
            name=comb.name,
            rkom_bid_configs=comb.rkom_bid_config_external_ids,
        )

        combination = RKOMBidCombinationApply(
            external_id=sequence_external_id,
            name=sequence_external_id.replace("_", " "),
            bid=bid,
            auction=comb.auction.value,
        )
        model.rkom_bid_combinations.append(combination)

    return model


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
        price_scenarios_by_name = _map_price_scenarios_by_name(
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
                incremental_mapping = CDFSequence(sequence=sequence, content=time_series_mapping.to_dataframe())
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
                content=bid_matrix_generator_sequence_content,
            ),
            incremental_mapping=incremental_mapping_sequences,
        )
        # The IDs are inconsistently compared to the other data classes, so we need to set them manually
        dayahead_process.external_id = f"POWEROPS_bid_process_configuration_{process.name}"
        dayahead_process.type_ = "POWEROPS_bid_process_configuration"

        dayahead_processes.append(dayahead_process)

    return dayahead_processes


def _to_rkom_market(
    rkom_bid_process: list[RKOMBidProcessConfig],
    price_scenarios_by_id: dict[str, PriceScenario],
    market_name: str,
    rkom_market_config: RkomMarketConfig,
    rkom_bid_combination_configs: list[RKOMBidCombinationConfig] | None = None,
) -> MarketModel:
    model = MarketModel()

    rkom_market = market_models.RKOMMarket(**rkom_market_config.model_dump())
    rkom_market.external_id = rkom_market_config.external_id
    model.markets.append(rkom_market)

    for config in rkom_bid_process:
        price_scenarios_by_name = _map_price_scenarios_by_name(
            config.price_scenarios,
            price_scenarios_by_id,
            market_name,
        )
        incremental_mapping_sequences = []
        for scenario_name, price_scenario in price_scenarios_by_name.items():
            mappings = price_scenario.to_time_series_mapping()
            for reserve_scenario in config.reserve_scenarios:
                reserve_mapping = reserve_scenario.to_time_series_mapping()
                metadata = {
                    "shop:watercourse": config.watercourse,
                    "shop:type": "rkom_incremental_mapping",
                    "bid:scenario_name": scenario_name,
                    "bid:reserve_volume": str(reserve_scenario.volume),
                }
                external_id = (
                    f"SHOP_{config.watercourse}_incremental_mapping_"
                    f"{config.name}_{scenario_name}_{reserve_scenario.volume}MW"
                )
                name = f"{config.watercourse} {scenario_name} {reserve_scenario.volume} MW"
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

        process = market_models.RKOMProcess(
            name=config.name,
            description=f"RKOM bid generation config for {config.watercourse}",
            bid=market_models.RKOMBid(
                date=str(config.bid_date),
                auction=config.reserve_scenarios.auction.value,
                block=config.reserve_scenarios.block,
                product=config.reserve_scenarios.product,
                watercourse=config.watercourse,
                method=config.method,
                minimum_price=str(config.minimum_price),
                price_premium=str(config.price_premium),
                price_scenarios=json.dumps(list(price_scenarios_by_name)),
                reserve_scenarios=str(config.reserve_scenarios),
            ),
            shop=market_models.ShopTransformation(
                starttime=str(config.shop_start),
                endtime=str(config.shop_end),
            ),
            timezone=config.timezone,
            rkom=market_models.RKOMPlants(plants=json.dumps(sorted(config.rkom_plants))),
            incremental_mapping=incremental_mapping_sequences,
        )
        process.external_id = config.external_id
        process.type_ = "POWEROPS"
        model.rkom_processes.append(process)

    for config in rkom_bid_combination_configs or []:
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

    return model


def _map_price_scenarios_by_name(
    scenario_ids: list[PriceScenarioID], price_scenarios_by_id: dict[str, PriceScenario], market_name: str
) -> dict[str, PriceScenario]:
    scenario_by_name = {}
    for identifier in scenario_ids:
        ref_scenario = price_scenarios_by_id[identifier.id]
        name = identifier.rename or ref_scenario.name or identifier.id
        scenario_by_name[name] = PriceScenario(name=market_name, **ref_scenario.model_dump(exclude={"name"}))
    return scenario_by_name
