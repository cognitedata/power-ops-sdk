from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
from cognite.client.data_classes import Relationship, Sequence, TimeSeries

import cognite.powerops.bootstrap.models.base
from cognite.powerops.bootstrap.data_classes.core.generator import GeneratorTimeSeriesMapping
from cognite.powerops.bootstrap.data_classes.core.plant import (
    PlantTimeSeriesMapping,
    plant_to_inlet_reservoir_breadth_first_search,
)
from cognite.powerops.bootstrap.data_classes.core.watercourse import WatercourseConfig
from cognite.powerops.bootstrap.data_classes.model_file import Connection
from cognite.powerops.bootstrap.models import core
from cognite.powerops.bootstrap.to_cdf_resources.create_asset_types import price_area_asset
from cognite.powerops.bootstrap.to_cdf_resources.create_relationship_types import price_area_to_dayahead_price
from cognite.powerops.bootstrap.utils.serializer import load_yaml

p_min_fallback = 0
p_max_fallback = 1e20

head_loss_factor_fallback = 0.0


def to_core_model(
    watercourse_configs: list[WatercourseConfig],
    plant_time_series_mappings: list[PlantTimeSeriesMapping] = None,
    generator_time_series_mappings: list[GeneratorTimeSeriesMapping] = None,
) -> core.CoreModel:
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
    watercourse_configs : List[WatercourseConfig]
        List of watercourse configs
    plant_time_series_mappings : List[PlantTimeSeriesMapping]
        List of plant time series mappings
    generator_time_series_mappings : List[GeneratorTimeSeriesMapping]
        List of generator time series mappings

    Returns
    -------
    CDFResourceCollection
        Collection of Assets, Sequences, Relationships and sequence data
    """
    plant_time_series_mappings_by_name = {mapping.plant_name: mapping for mapping in (plant_time_series_mappings or [])}
    start_stop_cost_time_series_by_generator = {
        mapping.generator_name: mapping.start_stop_cost for mapping in (generator_time_series_mappings or [])
    }

    model = core.CoreModel()
    for watercourse_config in watercourse_configs:
        watercourse = core.Watercourse(
            name=watercourse_config.name,
            shop_penalty_limit=str(watercourse_config.shop_penalty_limit),
            plants=[],
            production_obligation_time_series=[
                TimeSeries(external_id=id_) for id_ in watercourse_config.production_obligation_ts_ext_ids
            ],
        )
        model.watercourses.append(watercourse)

        shop_case = load_yaml(Path(watercourse_config.yaml_raw_path), clean_data=True)

        for reservoir_name in shop_case["model"]["reservoir"]:
            reservoir = core.Reservoir(
                reservoir_name,
                *watercourse_config.reservoir_display_names_and_order.get(
                    reservoir_name, (re.sub(r"\([0-9]+\)", "", reservoir_name), "999")
                ),
            )
            model.reservoirs.append(reservoir)

        for generator_name, generator_attributes in shop_case["model"]["generator"].items():
            start_stop_cost = start_stop_cost_time_series_by_generator.get(generator_name)
            generator = core.Generator(
                generator_name,
                penstock=str(generator_attributes.get("penstock", "1")),
                p_min=float(generator_attributes.get("p_min", 0.0)),
                startcost=float(get_single_value(generator_attributes.get("startcost", 0.0))),
                start_stop_cost_time_series=TimeSeries(external_id=start_stop_cost) if start_stop_cost else None,
            )
            x_col_name = "generator_power"
            y_col_name = "generator_efficiency"
            sequence = Sequence(
                external_id=f"{generator.external_id}_generator_efficiency_curve",
                name=f"{generator.name} generator efficiency curve",
                columns=[
                    {"valueType": "DOUBLE", "externalId": x_col_name},
                    {"valueType": "DOUBLE", "externalId": y_col_name},
                ],
            )
            data = generator_attributes["gen_eff_curve"]
            efficiency_curve = cognite.powerops.bootstrap.models.core.CDFSequence(
                sequence=sequence,
                content=pd.DataFrame(
                    {
                        x_col_name: data["x"],
                        y_col_name: data["y"],
                    },
                    dtype=float,
                ),
            )
            generator.generator_efficiency_curve = efficiency_curve
            data = generator_attributes["turb_eff_curves"]
            ref_col_name = "head"
            x_col_name = "flow"
            y_col_name = "turbine_efficiency"
            sequence = Sequence(
                external_id=f"{generator.external_id}_turbine_efficiency_curve",
                name=f"{generator.name} turbine efficiency curve",
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

            turbine_efficiency_curve = cognite.powerops.bootstrap.models.core.CDFSequence(
                sequence=sequence,
                content=df,
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

            plant = core.Plant(
                name=plant_name,
                display_name=display_name,
                ordering=order,
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
            inlet_reservoir_name = plant_to_inlet_reservoir_breadth_first_search(
                plant_name, all_connections, {r.name for r in model.reservoirs}
            )
            selected_reservoir = next((r for r in model.reservoirs if r.name == inlet_reservoir_name), None)
            plant.inlet_reservoir = selected_reservoir

            parsed_connections = [Connection(**connection) for connection in all_connections]

            # Add the generators to the plant
            plant.generators = [
                g for connection in parsed_connections if (g := connection.to_from_any(generators_by_name, plant))
            ] + [g for connection in parsed_connections if (g := connection.from_to_any(plant, generators_by_name))]
            plants.append(plant)

            prod_area = str(list(attributes["prod_area"].values())[0])
            price_area_name = watercourse_config.market_to_price_area[prod_area]
            price_area = core.PriceArea(name=price_area_name)
            if price_area_name not in {a.name for a in model.price_areas}:
                model.price_areas.append(price_area)
            price_area = next(a for a in model.price_areas if a.name == price_area_name)
            watercourse.plants.append(plant)
            price_area.plants.append(plant)
            if watercourse.name not in {w.name for w in price_area.watercourses}:
                price_area.watercourses.append(watercourse)

        model.plants.extend(plants)

    return model


def get_single_value(value_or_time_series: float | dict) -> float:
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


def generate_relationships_from_price_area_to_price(
    price_ts_ext_id_per_price_area: dict[str, str]
) -> list[Relationship]:
    return [
        price_area_to_dayahead_price(price_area_asset(price_area_name), ts_ext_id)
        for price_area_name, ts_ext_id in price_ts_ext_id_per_price_area.items()
    ]
