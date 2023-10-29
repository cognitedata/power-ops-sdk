from __future__ import annotations

import re

from cognite.client.data_classes import TimeSeries

from cognite.powerops.resync import config
from cognite.powerops.resync.models._shared_v1_v2.production_model import (
    _create_generator_efficiency_curve,
    _create_turbine_efficiency_curve,
    _plant_to_inlet_reservoir_with_losses,
    head_loss_factor_fallback,
    p_max_fallback,
    p_min_fallback,
)
from cognite.powerops.resync.models.v1.production import (
    Generator,
    Plant,
    PriceArea,
    ProductionModel,
    Reservoir,
    Watercourse,
    WaterCourseShop,
)


def to_production_model(configuration: config.ProductionConfig) -> ProductionModel:
    """
    Create Assets for:
        - price_area,
        - watercourse
        - plant
        - reservoir
        - generator

    Create Relationships for:
        - price_area -> plant
        - price_area -> watercourse
        - watercourse -> plant
        - watercourse -> production obligation time series
        - plant -> reservoir
        - plant -> generator
        - generator -> generator efficiency
        - generator -> turbine efficiency

    Create sequences for
        - Generator efficiency
        - Turbine efficiency

    Create the sequence data for the
        - Generator efficiency
        - Turbine efficiency

    Parameters
    ----------
    configuration : List[CoreConfigs]
        The configurations for the core model

    Returns
    -------
    CoreModel
        An instance of the Core Data Model
    """
    plant_time_series_mappings_by_name = {
        mapping.plant_name: mapping for mapping in (configuration.plant_time_series_mappings or [])
    }
    start_stop_cost_time_series_by_generator = {
        mapping.generator_name: mapping.start_stop_cost
        for mapping in (configuration.generator_time_series_mappings or [])
    }
    is_generator_available = {
        mapping.generator_name: mapping.is_available for mapping in (configuration.generator_time_series_mappings or [])
    }

    model = ProductionModel()
    for watercourse_config in configuration.watercourses:
        watercourse = Watercourse(
            name=watercourse_config.name,
            shop=WaterCourseShop(penalty_limit=str(watercourse_config.shop_penalty_limit)),
            config_version=watercourse_config.version,
            model_file=watercourse_config.yaml_raw_path,
            processed_model_file=watercourse_config.yaml_processed_path,
            plants=[],
            production_obligation_time_series=[
                TimeSeries(external_id=id_) for id_ in watercourse_config.production_obligation_ts_ext_ids
            ],
            write_back_model_file=watercourse_config.write_back_model_file,
        )
        model.watercourses.append(watercourse)

        shop_case = watercourse_config.shop_model_template

        for reservoir_name in shop_case["model"]["reservoir"]:
            reservoir = Reservoir(
                name=reservoir_name,
                **dict(
                    zip(
                        ["display_name", "ordering"],
                        watercourse_config.reservoir_display_names_and_order.get(
                            reservoir_name, (re.sub(r"\([0-9]+\)", "", reservoir_name), "999")
                        ),
                    )
                ),
            )
            model.reservoirs.append(reservoir)

        generator_attributes_by_name = watercourse_config.shop_model_template["model"]["generator"]
        for generator_config in watercourse_config.generators:
            generator_name = generator_config.name
            start_stop_cost = start_stop_cost_time_series_by_generator.get(generator_name)
            is_available = is_generator_available.get(generator_name)
            generator = Generator(
                name=generator_name,
                penstock=generator_config.penstock,
                p_min=generator_config.p_min,
                startcost=generator_config.startcost,
                start_stop_cost_time_series=TimeSeries(external_id=start_stop_cost) if start_stop_cost else None,
                is_available_time_series=TimeSeries(external_id=is_available) if is_available else None,
            )
            generator_attributes = generator_attributes_by_name[generator_name]
            efficiency_curve = _create_generator_efficiency_curve(
                generator_attributes, generator.name, generator.external_id
            )
            generator.generator_efficiency_curve = efficiency_curve

            turbine_efficiency_curve = _create_turbine_efficiency_curve(
                generator_attributes, generator.name, generator.external_id
            )
            generator.turbine_efficiency_curve = turbine_efficiency_curve
            model.generators.append(generator)

        generators_by_name = {generator.name: generator for generator in model.generators}
        plants = []
        for plant_name, attributes in shop_case["model"]["plant"].items():
            display_name, order = watercourse_config.plant_display_names_and_order.get(
                plant_name, (re.sub(r"\([0-9]+\)", "", plant_name), "999")
            )
            mapping = plant_time_series_mappings_by_name.get(plant_name)
            if mapping:
                mappings = dict(
                    water_value_time_series=TimeSeries(external_id=mapping.water_value)
                    if mapping.water_value
                    else None,
                    inlet_level_time_series=TimeSeries(external_id=mapping.inlet_reservoir_level)
                    if mapping.inlet_reservoir_level
                    else None,
                    outlet_level_time_series=TimeSeries(external_id=mapping.outlet_reservoir_level)
                    if mapping.outlet_reservoir_level
                    else None,
                    feeding_fee_time_series=TimeSeries(external_id=mapping.feeding_fee)
                    if mapping.feeding_fee
                    else None,
                    p_min_time_series=TimeSeries(external_id=mapping.p_min) if mapping.p_min else None,
                    p_max_time_series=TimeSeries(external_id=mapping.p_max) if mapping.p_max else None,
                    head_direct_time_series=TimeSeries(external_id=mapping.head_direct)
                    if mapping.head_direct
                    else None,
                )
            else:
                mappings = {}

            all_connections = shop_case["connections"]
            all_junctions = shop_case["model"].get("junction", {})
            all_tunnels = shop_case["model"].get("tunnel", {})
            inlet_reservoir_name, connection_losses = _plant_to_inlet_reservoir_with_losses(
                plant_name, all_connections, all_junctions, all_tunnels, {r.name for r in model.reservoirs}
            )

            plant = Plant(
                name=plant_name,
                display_name=display_name,
                ordering=str(order),
                outlet_level=float(attributes.get("outlet_line", 0)),
                p_min=float(attributes.get("p_min", p_min_fallback)),
                p_max=float(attributes.get("p_max", p_max_fallback)),
                head_loss_factor=float(attributes.get("main_loss", [head_loss_factor_fallback])[0]),
                connection_losses=connection_losses,
                penstock_head_loss_factors={
                    str(penstock_number): float(loss_factor)
                    for penstock_number, loss_factor in enumerate(
                        attributes.get("penstock_loss", [head_loss_factor_fallback]), start=1
                    )
                },
                **mappings,
            )

            selected_reservoir = next((r for r in model.reservoirs if r.name == inlet_reservoir_name), None)
            plant.inlet_reservoir = selected_reservoir

            parsed_connections = [config.Connection(**connection) for connection in all_connections]

            # Add the generators to the plant
            plant.generators = [
                g
                for connection in parsed_connections
                if (g := connection.to_from_any(generators_by_name, plant.name, plant.type_))
            ] + [
                g
                for connection in parsed_connections
                if (g := connection.from_to_any(plant.name, plant.type_, generators_by_name))
            ]
            plants.append(plant)

            prod_area = str(next(iter(attributes["prod_area"].values())))
            price_area_name = watercourse_config.market_to_price_area[prod_area]
            price_area = PriceArea(name=price_area_name)
            if price_area_name not in {a.name for a in model.price_areas}:
                if price_area_name in configuration.dayahead_price_timeseries:
                    price_area.dayahead_price_time_series = TimeSeries(
                        external_id=configuration.dayahead_price_timeseries[price_area_name]
                    )
                model.price_areas.append(price_area)
            price_area = next(a for a in model.price_areas if a.name == price_area_name)
            watercourse.plants.append(plant)
            price_area.plants.append(plant)
            if watercourse.name not in {w.name for w in price_area.watercourses}:
                price_area.watercourses.append(watercourse)

        model.plants.extend(plants)
    return model
