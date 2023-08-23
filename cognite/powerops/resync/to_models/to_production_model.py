from __future__ import annotations

import re
from typing import Optional

import pandas as pd
from cognite.client.data_classes import Sequence, TimeSeries

from cognite.powerops.clients.data_classes import (
    GeneratorApply,
    PlantApply,
    PriceAreaApply,
    ReservoirApply,
    WatercourseApply,
    WatercourseShopApply,
)
from cognite.powerops.resync.config.production.connections import Connection
from cognite.powerops.resync.config.resync_config import ProductionConfig
from cognite.powerops.resync.models import production
from cognite.powerops.resync.models.cdf_resources import CDFSequence
from cognite.powerops.resync.models.production_dm import ProductionModelDM
from cognite.powerops.resync.utils.common import make_ext_id
from cognite.powerops.resync.utils.serializer import load_yaml

p_min_fallback = 0.0
p_max_fallback = 100_000_000_000_000_000_00.0

head_loss_factor_fallback = 0.0


def to_production_model(config: ProductionConfig) -> production.ProductionModel:
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
    config : List[CoreConfigs]
        The configurations for the core model

    Returns
    -------
    CoreModel
        An instance of the Core Data Model
    """
    plant_time_series_mappings_by_name = {
        mapping.plant_name: mapping for mapping in (config.plant_time_series_mappings or [])
    }
    start_stop_cost_time_series_by_generator = {
        mapping.generator_name: mapping.start_stop_cost for mapping in (config.generator_time_series_mappings or [])
    }
    is_generator_available = {
        mapping.generator_name: mapping.is_available for mapping in (config.generator_time_series_mappings or [])
    }

    model = production.ProductionModel()
    for watercourse_config in config.watercourses:
        watercourse = production.Watercourse(
            name=watercourse_config.name,
            shop=production.WaterCourseShop(penalty_limit=str(watercourse_config.shop_penalty_limit)),
            config_version=watercourse_config.version,
            model_file=watercourse_config.yaml_raw_path,
            processed_model_file=watercourse_config.yaml_processed_path,
            plants=[],
            production_obligation_time_series=[
                TimeSeries(external_id=id_) for id_ in watercourse_config.production_obligation_ts_ext_ids
            ],
        )
        model.watercourses.append(watercourse)

        shop_case = load_yaml(watercourse_config.yaml_raw_path, clean_data=True)

        for reservoir_name in shop_case["model"]["reservoir"]:
            reservoir = production.Reservoir(
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

        for generator_name, generator_attributes in shop_case["model"]["generator"].items():
            start_stop_cost = start_stop_cost_time_series_by_generator.get(generator_name)
            is_available = is_generator_available.get(generator_name)
            generator = production.Generator(
                name=generator_name,
                penstock=str(generator_attributes.get("penstock", "1")),
                p_min=float(generator_attributes.get("p_min", 0.0)),
                startcost=float(_get_single_value(generator_attributes.get("startcost", 0.0))),
                start_stop_cost_time_series=TimeSeries(external_id=start_stop_cost) if start_stop_cost else None,
                is_available_time_series=TimeSeries(external_id=is_available) if is_available else None,
            )

            efficiency_curve = _create_generator_efficiency_curve(
                generator_attributes, generator.name, generator.external_id
            )
            generator.generator_efficiency_curve = efficiency_curve

            turbine_efficiency_curve = _create_turbine_efficency_curve(
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

            plant = production.Plant(
                name=plant_name,
                display_name=display_name,
                ordering=str(order),
                outlet_level=float(attributes.get("outlet_line", 0)),
                p_min=float(attributes.get("p_min", p_min_fallback)),
                p_max=float(attributes.get("p_max", p_max_fallback)),
                head_loss_factor=float(
                    attributes.get("main_loss", [head_loss_factor_fallback])[0]
                ),  # For some reason, SHOP defines this as a list, but we only need the first (and only) value
                penstock_head_loss_factors={
                    str(penstock_number): float(loss_factor)
                    for penstock_number, loss_factor in enumerate(
                        attributes.get("penstock_loss", [head_loss_factor_fallback]), start=1
                    )
                },
                **mappings,
            )
            all_connections = shop_case["connections"]
            inlet_reservoir_name = _plant_to_inlet_reservoir_breadth_first_search(
                plant_name, all_connections, {r.name for r in model.reservoirs}
            )
            selected_reservoir = next((r for r in model.reservoirs if r.name == inlet_reservoir_name), None)
            plant.inlet_reservoir = selected_reservoir

            parsed_connections = [Connection(**connection) for connection in all_connections]

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

            prod_area = str(list(attributes["prod_area"].values())[0])
            price_area_name = watercourse_config.market_to_price_area[prod_area]
            price_area = production.PriceArea(name=price_area_name)
            if price_area_name not in {a.name for a in model.price_areas}:
                if price_area_name in config.dayahead_price_timeseries:
                    price_area.dayahead_price_time_series = TimeSeries(
                        external_id=config.dayahead_price_timeseries[price_area_name]
                    )
                model.price_areas.append(price_area)
            price_area = next(a for a in model.price_areas if a.name == price_area_name)
            watercourse.plants.append(plant)
            price_area.plants.append(plant)
            if watercourse.name not in {w.name for w in price_area.watercourses}:
                price_area.watercourses.append(watercourse)

        model.plants.extend(plants)
    return model


def to_production_data_model(config: ProductionConfig) -> ProductionModelDM:
    model = ProductionModelDM()
    plant_time_series_mappings_by_name = {
        mapping.plant_name: mapping for mapping in (config.plant_time_series_mappings or [])
    }
    start_stop_cost_time_series_by_generator = {
        mapping.generator_name: mapping.start_stop_cost for mapping in (config.generator_time_series_mappings or [])
    }
    is_generator_available = {
        mapping.generator_name: mapping.is_available for mapping in (config.generator_time_series_mappings or [])
    }

    for watercourse_config in config.watercourses:
        watercourse = WatercourseApply(
            external_id=f"watercourse:{watercourse_config.name}",
            name=watercourse_config.name,
            shop=WatercourseShopApply(
                external_id=make_ext_id(watercourse_config.shop_penalty_limit, WatercourseShopApply),
                penalty_limit=watercourse_config.shop_penalty_limit,
            ),
            plants=[],
            production_obligation=watercourse_config.production_obligation_ts_ext_ids,
        )
        model.watercourses.append(watercourse)
        # config_version = watercourse_config.version,
        # model_file = watercourse_config.yaml_raw_path,
        # processed_model_file = watercourse_config.yaml_processed_path,

        shop_case = load_yaml(watercourse_config.yaml_raw_path, clean_data=True)

        reservoirs = []
        for reservoir_name in shop_case["model"]["reservoir"]:
            reservoir = ReservoirApply(
                external_id=f"reservoir:{reservoir_name}",
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
            reservoirs.append(reservoir)
        model.reservoirs.extend(reservoirs)

        for generator_name, generator_attributes in shop_case["model"]["generator"].items():
            start_stop_cost = start_stop_cost_time_series_by_generator.get(generator_name)
            is_available = is_generator_available.get(generator_name)
            generator = GeneratorApply(
                external_id=f"generator:{generator_name}",
                name=generator_name,
                penstock=int(generator_attributes.get("penstock", 1)),
                p_min=float(generator_attributes.get("p_min", 0.0)),
                startcost=float(_get_single_value(generator_attributes.get("startcost", 0.0))),
                start_stop_cost=start_stop_cost,
                is_available=is_available,
            )
            efficiency_curve = _create_generator_efficiency_curve(
                generator_attributes, generator.name, generator.external_id
            )
            model.cdf_sequences.append(efficiency_curve)
            generator.generator_efficiency_curve = efficiency_curve.external_id
            turbine_efficiency_curve = _create_turbine_efficency_curve(
                generator_attributes, generator.name, generator.external_id
            )

            generator.turbine_efficiency_curve = turbine_efficiency_curve.external_id
            model.generators.append(generator)
            model.cdf_sequences.append(turbine_efficiency_curve)

        generators_by_name = {generator.name: generator for generator in model.generators}
        plants = []
        for plant_name, attributes in shop_case["model"]["plant"].items():
            display_name, order = watercourse_config.plant_display_names_and_order.get(
                plant_name, (re.sub(r"\([0-9]+\)", "", plant_name), "999")
            )
            mapping = plant_time_series_mappings_by_name.get(plant_name)
            if mapping:
                mappings = dict(
                    water_value=mapping.water_value,
                    inlet_level=mapping.inlet_reservoir_level,
                    outlet_level_time_series=mapping.outlet_reservoir_level,
                    feeding_fee=mapping.feeding_fee,
                    p_min_time_series=mapping.p_min,
                    p_max_time_series=mapping.p_max,
                    head_direct_time_series=mapping.head_direct,
                )
            else:
                mappings = {}

            plant = PlantApply(
                external_id=f"plant:{plant_name}",
                name=plant_name,
                display_name=display_name,
                ordering=order,
                watercourse=watercourse,
                outlet_level=float(attributes.get("outlet_line", 0)),
                p_min=float(attributes.get("p_min", p_min_fallback)),
                p_max=float(attributes.get("p_max", p_max_fallback)),
                head_loss_factor=float(
                    attributes.get("main_loss", [head_loss_factor_fallback])[0]
                ),  # For some reason, SHOP defines this as a list, but we only need the first (and only) value
                penstock_head_loss_factors={
                    str(penstock_number): float(loss_factor)
                    for penstock_number, loss_factor in enumerate(
                        attributes.get("penstock_loss", [head_loss_factor_fallback]), start=1
                    )
                },
                **mappings,
            )
            all_connections = shop_case["connections"]
            inlet_reservoir_name = _plant_to_inlet_reservoir_breadth_first_search(
                plant_name, all_connections, {r.name for r in model.reservoirs}
            )
            selected_reservoir = next((r for r in model.reservoirs if r.name == inlet_reservoir_name), None)
            plant.inlet_reservoirs = [selected_reservoir]

            parsed_connections = [Connection(**connection) for connection in all_connections]
            # Add the generators to the plant
            plant.generators = [
                g
                for connection in parsed_connections
                if (g := connection.to_from_any(generators_by_name, plant.name, "plant"))
            ] + [
                g
                for connection in parsed_connections
                if (g := connection.from_to_any(plant.name, "plant", generators_by_name))
            ]
            plants.append(plant)

            prod_area = str(list(attributes["prod_area"].values())[0])
            price_area_name = watercourse_config.market_to_price_area[prod_area]
            price_area = PriceAreaApply(
                name=price_area_name,
                external_id=f"price_area:{price_area_name}",
            )
            if price_area_name not in {a.name for a in model.price_areas}:
                if price_area_name in config.dayahead_price_timeseries:
                    price_area.day_ahead_price = config.dayahead_price_timeseries[price_area_name]
                model.price_areas.append(price_area)
            price_area = next(a for a in model.price_areas if a.name == price_area_name)

            watercourse.plants.append(plant)
            price_area.plants.append(plant)
            if watercourse.name not in {w.name for w in price_area.watercourses}:
                price_area.watercourses.append(watercourse)
        model.plants.extend(plants)

    return model


def _create_generator_efficiency_curve(generator_attributes, generator_name, generator_external_id) -> CDFSequence:
    x_col_name = "generator_power"
    y_col_name = "generator_efficiency"
    sequence = Sequence(
        external_id=f"{generator_external_id}_generator_efficiency_curve",
        name=f"{generator_name} generator efficiency curve",
        columns=[
            {"valueType": "DOUBLE", "externalId": x_col_name},
            {"valueType": "DOUBLE", "externalId": y_col_name},
        ],
    )
    data = generator_attributes["gen_eff_curve"]
    efficiency_curve = CDFSequence(
        sequence=sequence,
        content=pd.DataFrame(
            {
                x_col_name: data["x"],
                y_col_name: data["y"],
            },
            dtype=float,
        ),
    )
    return efficiency_curve


def _create_turbine_efficency_curve(generator_attributes, generator_name, generator_external_id) -> CDFSequence:
    data = generator_attributes["turb_eff_curves"]
    ref_col_name = "head"
    x_col_name = "flow"
    y_col_name = "turbine_efficiency"
    sequence = Sequence(
        external_id=f"{generator_external_id}_turbine_efficiency_curve",
        name=f"{generator_name} turbine efficiency curve",
        columns=[
            {"valueType": "DOUBLE", "externalId": ref_col_name},
            {"valueType": "DOUBLE", "externalId": x_col_name},
            {"valueType": "DOUBLE", "externalId": y_col_name},
        ],
    )
    df = pd.DataFrame(
        {
            ref_col_name: [entry["ref"] for entry in data for _ in range(len(entry["x"]))],
            x_col_name: [item for entry in data for item in entry["x"]],
            y_col_name: [item for entry in data for item in entry["y"]],
        },
        dtype=float,
    )
    turbine_efficiency_curve = CDFSequence(
        sequence=sequence,
        content=df,
    )
    return turbine_efficiency_curve


def _get_single_value(value_or_time_series: float | dict) -> float:
    """Get the single value from a time series, or a value
    returns the value if value_or_time_series is a value, otherwise the first value in the time series

    Parameters
    ----------
    value_or_time_series : float | dict
        Either a simple numeric value, or a dictionary (time series, {datetime string: value})
    """
    if isinstance(value_or_time_series, dict):
        return next(iter(value_or_time_series.values()))
    return value_or_time_series


def _plant_to_inlet_reservoir_breadth_first_search(
    plant_name: str,
    all_connections: list[dict],
    reservoirs: set[str],
) -> Optional[str]:
    """Search for a reservoir connected to a plant, starting from the plant and searching breadth first.

    Parameters
    ----------
    plant_name : str
        The plant we want to find a connection from
    all_connections : list[dict]
        All connections in the model.
    reservoirs : dict
        All reservoirs in the model. Keys are reservoir names.

    Returns
    -------
    Optional[str]
        The name of the reservoir connected to the plant, or None if no reservoir was found.
    """
    queue = []
    for connection in all_connections:
        if (
            connection["to"] == plant_name and connection.get("to_type", "plant") == "plant"
        ):  # if to_type is specified, it must be "plant"
            queue.append(connection)
            break
    visited = []
    while queue:
        connection = queue.pop(0)
        if connection not in visited:
            # Check if the given connection is from a reservoir
            # If we have "from_type" we can check directly if the object is a reservoir
            try:
                if connection["from_type"] == "reservoir":
                    return connection["from"]
            # If we don't have "from_type" we have to check if the name of the object is in the
            # list of reservoirs
            except KeyError:
                if connection["from"] in reservoirs:
                    return connection["from"]

            visited.append(connection)
            for candidate_connection in all_connections:
                # if the candidate connection is extension from the current connection, traverse it
                if candidate_connection["to"] == connection["from"]:
                    queue.append(candidate_connection)
    return None
