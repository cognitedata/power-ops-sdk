from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
from cognite.client.data_classes import Relationship, Sequence, TimeSeries

from cognite.powerops.bootstrap.data_classes.core.generator import GeneratorTimeSeriesMapping
from cognite.powerops.bootstrap.data_classes.core.plant import (
    Plant,
    PlantTimeSeriesMapping,
    plant_to_inlet_reservoir_breadth_first_search,
)
from cognite.powerops.bootstrap.data_classes.core.watercourse import WatercourseConfig
from cognite.powerops.bootstrap.data_classes.model_file import Connection
from cognite.powerops.bootstrap.data_classes.resource_collection import ResourceCollection
from cognite.powerops.bootstrap.to_cdf_resources.create_asset_types import price_area_asset, watercourse_asset
from cognite.powerops.bootstrap.to_cdf_resources.create_relationship_types import (
    price_area_to_dayahead_price,
    price_area_to_plant,
    price_area_to_watercourse,
    watercourse_to_plant,
    watercourse_to_production_obligation,
)
from cognite.powerops.bootstrap.utils.serializer import load_yaml

from . import core_model

p_min_fallback = 0
p_max_fallback = 1e20

head_loss_factor_fallback = 0.0


def generate_resources_and_data(
    watercourse_configs: list[WatercourseConfig],
    plant_time_series_mappings: list[PlantTimeSeriesMapping] = None,
    generator_time_series_mappings: list[GeneratorTimeSeriesMapping] = None,
) -> tuple[ResourceCollection, core_model.CoreModel]:
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

    resources = ResourceCollection()
    model = core_model.CoreModel()
    for watercourse_config in watercourse_configs:
        # Todo delete
        watercourse = watercourse_asset(
            name=watercourse_config.name,
            shop_penalty_limit=watercourse_config.shop_penalty_limit,
        )
        resources.add(watercourse)

        watercourse2 = core_model.Watercourse(
            name=watercourse_config.name,
            shop_penalty_limit=watercourse_config.shop_penalty_limit,
            plants=[],
            production_obligation_time_series=[
                TimeSeries(external_id=id_) for id_ in watercourse_config.production_obligation_ts_ext_ids
            ],
        )
        model.watercourses.append(watercourse2)

        shop_case = load_yaml(Path(watercourse_config.yaml_raw_path), clean_data=True)

        reservoirs = []
        for reservoir_name in shop_case["model"]["reservoir"]:
            reservoir = core_model.Reservoir(
                reservoir_name,
                *watercourse_config.reservoir_display_names_and_order.get(
                    reservoir_name, (re.sub(r"\([0-9]+\)", "", reservoir_name), "999")
                ),
            )
            reservoirs.append(reservoir)
        model.reservoirs.extend(reservoirs)

        generators = []
        for generator_name, generator_attributes in shop_case["model"]["generator"].items():
            start_stop_cost = start_stop_cost_time_series_by_generator.get(generator_name)
            generator = core_model.Generator(
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
            efficiency_curve = core_model.CDFSequence(
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

            turbine_efficiency_curve = core_model.CDFSequence(
                sequence=sequence,
                content=df,
            )

            generator.turbine_efficiency_curve = turbine_efficiency_curve

            generators.append(generator)
        model.generators.extend(generators)

        generators_by_name = {generator.name: generator for generator in generators}
        plants2 = []
        for plant_name, attributes in shop_case["model"]["plant"].items():
            display_name, order = watercourse_config.plant_display_names_and_order.get(
                plant_name, (re.sub(r"\([0-9]+\)", "", plant_name), "999")
            )
            mapping = plant_time_series_mappings_by_name.get(plant_name)
            if mapping:
                mappings = dict(
                    water_value_time_series=mapping.water_value,
                    inlet_level_time_series=mapping.inlet_reservoir_level,
                    outlet_level_time_series=mapping.outlet_reservoir_level,
                    feeding_fee_time_series=mapping.feeding_fee,
                    p_min_time_series=mapping.p_min,
                    p_max_time_series=mapping.p_max,
                    head_direct_time_series=mapping.head_direct,
                )
            else:
                mappings = {}

            plant2 = core_model.Plant(
                name=plant_name,
                display_name=display_name,
                ordering=order,
                outlet_level=attributes.get("outlet_line", 0),
                p_min=attributes.get("p_min", p_min_fallback),
                p_max=attributes.get("p_max", p_max_fallback),
                head_loss_factor=attributes.get("main_loss", [head_loss_factor_fallback])[
                    0
                ],  # For some reason, SHOP defines this as a list, but we only need the first (and only) value
                penstock_head_loss_factors={
                    str(penstock_number): loss_factor
                    for penstock_number, loss_factor in enumerate(
                        attributes.get("penstock_loss", [head_loss_factor_fallback]), start=1
                    )
                },
                **mappings,
            )
            all_connections = shop_case["connections"]
            inlet_reservoir_name = plant_to_inlet_reservoir_breadth_first_search(
                plant_name, all_connections, {r.name for r in reservoirs}
            )
            selected_reservoir = next((r for r in reservoirs if r.name == inlet_reservoir_name), None)
            plant2.inlet_reservoir = selected_reservoir

            parsed_connections = [Connection(**connection) for connection in all_connections]

            # Add the generators to the plant
            plant2.generators = [
                g for connection in parsed_connections if (g := connection.to_from_any(generators_by_name, plant2))
            ] + [g for connection in parsed_connections if (g := connection.from_to_any(plant2, generators_by_name))]
            plants2.append(plant2)

            prod_area = str(list(attributes["prod_area"].values())[0])
            price_area_name = watercourse_config.market_to_price_area[prod_area]
            price_area = core_model.PriceArea(name=price_area_name)
            if price_area_name not in {a.name for a in model.price_areas}:
                model.price_areas.append(price_area)
            price_area = next(a for a in model.price_areas if a.name == price_area_name)
            watercourse2.plants.append(plant2)
            price_area.plants.append(plant2)
            if watercourse2.name not in {w.name for w in price_area.watercourses}:
                price_area.watercourses.append(watercourse2)

        model.plants.extend(plants2)

        plants = Plant.from_shop_case(shop_case=shop_case)
        if plant_time_series_mappings:
            Plant.add_time_series_mapping(
                plant_time_series_mappings=plant_time_series_mappings,
                plants=plants,
            )
        # Add display name and ordering key.
        for plant in plants.values():
            plant.display_name = watercourse_config.plant_display_name(plant.name)
            plant.ordering_key = watercourse_config.plant_ordering_key(plant.name)

            resources += plant.to_bootstrap_resources()

        # Add relationships that are not covered by the Plant class
        for plant_name, attributes in shop_case["model"]["plant"].items():
            # Using the first value of the prod_area time series as the prod_area (assuming it does not change
            prod_area = str(list(attributes["prod_area"].values())[0])
            price_area_name = watercourse_config.market_to_price_area[prod_area]
            price_area_external_id = f"price_area_{price_area_name}"
            if price_area_external_id not in resources.assets:
                resources.add(price_area_asset(name=price_area_name))

            plant_external_id = f"plant_{plant_name}"
            resources.add(watercourse_to_plant(watercourse=watercourse, plant=plant_external_id))
            resources.add(price_area_to_plant(price_area=price_area_external_id, plant=plant_external_id))
            resources.add(price_area_to_watercourse(price_area=price_area_external_id, watercourse=watercourse))

        if watercourse_config.production_obligation_ts_ext_ids:
            for ts_ext_id in watercourse_config.production_obligation_ts_ext_ids:
                rel = watercourse_to_production_obligation(
                    watercourse=watercourse, production_obligation_ts_ext_id=ts_ext_id
                )
                resources.add(rel)

    return resources, model


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
