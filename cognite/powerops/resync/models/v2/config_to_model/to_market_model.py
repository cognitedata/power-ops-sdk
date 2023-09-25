from __future__ import annotations

from cognite.powerops.client.data_classes import (
    BenchmarkBidApply,
    BenchmarkProcessApply,
    BidMatrixGeneratorApply,
    DayAheadBidApply,
    DayAheadProcessApply,
    NordPoolMarketApply,
    PriceAreaApply,
    ProductionPlanTimeSeriesApply,
    ReserveScenarioApply,
    RKOMBidApply,
    RKOMBidCombinationApply,
    RKOMCombinationBidApply,
    RKOMMarketApply,
    RKOMProcessApply,
    ScenarioMappingApply,
)
from cognite.powerops.resync.config._main import MarketConfig
from cognite.powerops.resync.config.market import BenchmarkingConfig, PriceScenario
from cognite.powerops.resync.config.market.dayahead import BidMatrixGeneratorConfig
from cognite.powerops.resync.config.market.market import MARKET_BY_PRICE_AREA
from cognite.powerops.resync.models._shared_v1_v2._to_instances import (
    _to_date_transformations,
    _to_input_timeseries_mapping,
    _to_scenario_mapping,
    _to_shop_transformation,
    make_ext_id,
)
from cognite.powerops.resync.models._shared_v1_v2.market_model import _map_price_scenarios_by_name
from cognite.powerops.resync.models.v2.market_dm import (
    BenchmarkMarketDataModel,
    DayAheadMarketDataModel,
    RKOMMarketDataModel,
)


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
        process = BenchmarkProcessApply(
            external_id="benchmarking_config_day_ahead",
            name="Benchmarking config DayAhead",
            bid=bid.external_id,
            shop=_to_shop_transformation(config.shop_start, config.shop_end),
            production_plan_time_series=[
                ProductionPlanTimeSeriesApply(
                    external_id=make_ext_id([name, series], ProductionPlanTimeSeriesApply), name=name, series=series
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
    dayahead_benchmark: BenchmarkProcessApply,
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
        dayahead_process = DayAheadProcessApply(
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
            #     BidMatrixGeneratorApply,
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
            process.price_scenarios, config.price_scenario_by_id, market_name
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

        apply = RKOMProcessApply(
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
                [comb.auction.value, comb.name, *comb.rkom_bid_config_external_ids], RKOMCombinationBidApply
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
