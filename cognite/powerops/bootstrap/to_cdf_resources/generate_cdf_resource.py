from __future__ import annotations

import re
from pathlib import Path
from typing import Optional, TypedDict

import pandas as pd
from cognite.client.data_classes import Asset, Relationship, Sequence, TimeSeries

from cognite.powerops.bootstrap.data_classes.core.generator import Generator, GeneratorTimeSeriesMapping
from cognite.powerops.bootstrap.data_classes.core.plant import Plant, PlantTimeSeriesMapping
from cognite.powerops.bootstrap.data_classes.core.watercourse import WatercourseConfig
from cognite.powerops.bootstrap.data_classes.resource_collection import ResourceCollection
from cognite.powerops.bootstrap.data_classes.to_delete import SequenceContent
from cognite.powerops.bootstrap.to_cdf_resources.create_asset_types import price_area_asset, watercourse_asset
from cognite.powerops.bootstrap.to_cdf_resources.create_relationship_types import (
    generator_to_generator_efficiency_curve,
    generator_to_turbine_efficiency_curve,
    price_area_to_dayahead_price,
    price_area_to_plant,
    price_area_to_watercourse,
    watercourse_to_plant,
    watercourse_to_production_obligation,
)
from cognite.powerops.bootstrap.utils.serializer import load_yaml

from . import core_model


class ShopEfficiencyCurve(TypedDict):
    ref: str
    x: list[float]
    y: list[float]


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

        generators = shop_case["model"]["generator"]
        generators2 = []
        for generator_name, generator_attributes in generators.items():
            start_stop_cost = start_stop_cost_time_series_by_generator.get(generator_name)
            generator2 = core_model.Generator(
                generator_name,
                penstock=str(generator_attributes.get("penstock", "1")),
                p_min=float(generator_attributes.get("p_min", 0.0)),
                startcost=float(get_single_value(generator_attributes.get("startcost", 0.0))),
                start_stop_cost_time_series=TimeSeries(external_id=start_stop_cost) if start_stop_cost else None,
            )
            x_col_name = "generator_power"
            y_col_name = "generator_efficiency"
            sequence = Sequence(
                external_id=f"{generator2.external_id}_generator_efficiency_curve",
                name=f"{generator2.name} generator efficiency curve",
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
            generator2.efficiency_curve = efficiency_curve
            data = generator_attributes["turb_eff_curves"]
            ref_col_name = "head"
            x_col_name = "flow"
            y_col_name = "turbine_efficiency"
            sequence = Sequence(
                external_id=f"{generator2.external_id}_turbine_efficiency_curve",
                name=f"{generator2.name} turbine efficiency curve",
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

            generator2.turbine_efficiency_curve = turbine_efficiency_curve

            generators2.append(generator2)
        model.generators.extend(generators2)

        # Todo delete
        resources += add_generators_and_efficiency_curves(generators, generator_time_series_mappings)

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
        for plant_name, plant_attributes in shop_case["model"]["plant"].items():
            # Using the first value of the prod_area time series as the prod_area (assuming it does not change
            prod_area = str(list(plant_attributes["prod_area"].values())[0])
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


def create_generator_efficiency_curve_sequence(
    efficiency_curve: ShopEfficiencyCurve,
    generator_asset: Asset,
) -> ResourceCollection:
    """
    Create the cognite resource for the generator efficiency curve (Sequence) and the connection
    (Relationship) between the generator and the efficiency curve.

    Parameters
    ----------
    efficiency_curve : dict[Literal["ref", "x", "y"], float | list[float]]
        Dictionary of the generator efficiency curve
    generator_asset : Asset
        The generator asset to which the efficiency curve belongs

    Returns
    -------
    ResourceCollection
        The new resources (incl. sequence data)
    """
    resources = ResourceCollection()
    x_col_name = "generator_power"
    y_col_name = "generator_efficiency"

    sequence = Sequence(
        external_id=f"{generator_asset.external_id}_generator_efficiency_curve",
        name=f"{generator_asset.name} generator efficiency curve",
        columns=[
            {"valueType": "DOUBLE", "externalId": x_col_name},
            {"valueType": "DOUBLE", "externalId": y_col_name},
        ],
    )
    resources.add(sequence)
    resources.add(
        SequenceContent(
            sequence_external_id=sequence.external_id,
            data=pd.DataFrame(
                {
                    x_col_name: efficiency_curve["x"],
                    y_col_name: efficiency_curve["y"],
                },
                dtype=float,
            ),
        )
    )

    relationship = generator_to_generator_efficiency_curve(
        generator=generator_asset, generator_efficiency_curve=sequence
    )

    resources.add(relationship)

    return resources


def create_turbine_efficiency_curve_sequence(
    efficiency_curves: list[ShopEfficiencyCurve],
    generator_asset: Asset,
) -> ResourceCollection:
    """
    Create the cognite resource for turbine efficiency curve (Sequence) and the connection
    (Relationship) between the generator and the efficiency curve.

    Parameters
    ----------
    efficiency_curves : list[dict[Literal["ref", "x", "y"], float | list[float]]]
        List of dictionaries of the turbine efficiency curves
    generator_asset : Asset
        The generator asset to which the efficiency curve belongs

    Returns
    -------
    tuple[ResourceDict, dict[str, pd.DataFrame]]
        The new resources and sequence data dictionary
    """
    resources = ResourceCollection()
    ref_col_name = "head"
    x_col_name = "flow"
    y_col_name = "turbine_efficiency"

    sequence_external_id = f"{generator_asset.external_id}_turbine_efficiency_curve"
    # Start by creating the sequence
    sequence = Sequence(
        external_id=sequence_external_id,
        name=f"{generator_asset.name} turbine efficiency curve",
        columns=[
            {"valueType": "DOUBLE", "externalId": ref_col_name},
            {"valueType": "DOUBLE", "externalId": x_col_name},
            {"valueType": "DOUBLE", "externalId": y_col_name},
        ],
    )
    resources.add(sequence)

    # Then create the sequence data
    df_list = [
        pd.DataFrame(
            {
                ref_col_name: [],
                x_col_name: [],
                y_col_name: [],
            }
        )
    ]

    for efficiency_curve in efficiency_curves:
        ref = efficiency_curve["ref"]
        x = efficiency_curve["x"]
        y = efficiency_curve["y"]
        temp_df = pd.DataFrame(
            {
                ref_col_name: [ref] * len(x),
                x_col_name: x,
                y_col_name: y,
            }
        )
        df_list.append(temp_df)
    df = pd.concat(df_list, ignore_index=True)

    resources.add(
        SequenceContent(
            sequence_external_id=sequence.external_id,
            data=df,
        )
    )

    # Finally create the relationship
    relationship = generator_to_turbine_efficiency_curve(generator=generator_asset, turbine_efficiency_curve=sequence)
    resources.add(relationship)

    return resources


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


def add_generators_and_efficiency_curves(
    shop_generator_dict: dict,
    generator_time_series_mappings: Optional[list[GeneratorTimeSeriesMapping]],
) -> ResourceCollection:
    """Create the cognite resource for the generator (Asset) and the generator- and turbine efficiencies (sequences), as
        well as the connection (Relationship) between the generator and the efficiency curves.

    Parameters
    ----------
    shop_generator_dict : dict
        Dictionary of generators objects from the shop case file
    generator_time_series_mappings : GeneratorTimeSeriesMapping
        Mapping of generator time series

    Returns
    -------
    ResourceCollection
        Collection of assets, sequences, relationships and sequence data
    """
    generators: {str: Generator} = {}
    resources = ResourceCollection()
    for generator_name, generator_information in shop_generator_dict.items():
        generator = Generator(
            name=generator_name,
            penstock=str(generator_information.get("penstock", "1")),
            p_min=float(generator_information.get("p_min", 0.0)),
            startcost=float(get_single_value(generator_information.get("startcost", 0.0))),
        )
        generators[generator_name] = generator
        generator_asset = generator.asset()

        resources += create_generator_efficiency_curve_sequence(
            generator_information["gen_eff_curve"],
            generator_asset,
        )

        resources += create_turbine_efficiency_curve_sequence(
            generator_information["turb_eff_curves"],
            generator_asset,
        )

    if generator_time_series_mappings:
        Generator.add_time_series_mapping(
            generator_time_series_mappings=generator_time_series_mappings, generators=generators
        )

    for generator in generators.values():
        resources += generator.to_bootstrap_resources()

    return resources


def generate_relationships_from_price_area_to_price(
    price_ts_ext_id_per_price_area: dict[str, str]
) -> list[Relationship]:
    return [
        price_area_to_dayahead_price(price_area_asset(price_area_name), ts_ext_id)
        for price_area_name, ts_ext_id in price_ts_ext_id_per_price_area.items()
    ]
