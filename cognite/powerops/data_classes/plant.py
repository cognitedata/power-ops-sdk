from __future__ import annotations

import json
from pathlib import Path
from typing import List, NamedTuple, Optional, Tuple

from cognite.client.data_classes import Asset, Label, Relationship
from pydantic import BaseModel, validator

from cognite.powerops.config import PlantTimeSeriesMapping
from cognite.powerops.data_classes.cdf_resource_collection import BootstrapResourceCollection
from cognite.powerops.data_classes.time_series_mapping import TimeSeriesMapping
from cognite.powerops.utils.common import print_warning
from cognite.powerops.utils.labels import AssetLabels as al
from cognite.powerops.utils.labels import RelationshipLabels as rl
from cognite.powerops.utils.relationship_types import asset_to_time_series, plant_to_generator, plant_to_inlet_reservoir
from cognite.powerops.utils.serializer import load_yaml

p_min_fallback = 0
p_max_fallback = 1e20

head_loss_factor_fallback = 0.0

ExternalId = str
PlantName = str


class Plant(BaseModel):
    name: PlantName
    external_id: ExternalId
    outlet_level: float  # meters above sea level
    p_min: float = p_min_fallback
    p_max: float = p_max_fallback  # arbitrary max power output if not specified
    head_loss_factor: float = 0.0
    penstock_head_loss_factors: Optional[dict[str, float]] = None
    display_name: Optional[str] = None
    ordering_key: Optional[float] = None

    generator_ext_ids: list[ExternalId] = []  # external IDs of generator assets
    inlet_reservoir_ext_id: Optional[ExternalId] = None  # external ID of reservoir asset

    # TODO: Consider splitting this into a separate class/subclass
    inlet_level_time_series: Optional[ExternalId] = None  # external ID of time series with values in m.a.s.l.
    outlet_level_time_series: Optional[ExternalId] = None  # external ID of time series with values in m.a.s.l.
    water_value_time_series: Optional[ExternalId] = None  # external ID of time series with values in €/MWh
    feeding_fee_time_series: Optional[ExternalId] = None  # external ID of time series with values in percent
    p_min_time_series: Optional[ExternalId] = None  # external ID of time series with values in MW
    p_max_time_series: Optional[ExternalId] = None  # external ID of time series with values in MW
    head_direct_time_series: Optional[ExternalId] = None  # external ID of time series with values in m

    @validator("penstock_head_loss_factors", pre=True)
    def parse_dict(cls, value):
        if isinstance(value, str):
            value = json.loads(value)
        return value

    def asset(self) -> Asset:
        """Returns the Asset representation of this Plant (without relationships)"""
        return Asset(
            name=self.name,
            external_id=self.external_id,
            parent_external_id="plants",
            metadata={
                "outlet_level": self.outlet_level,
                "p_min": self.p_min,
                "p_max": self.p_max,
                "head_loss_factor": self.head_loss_factor,
                "penstock_head_loss_factors": json.dumps(self.penstock_head_loss_factors),
                "display_name": self.display_name or self.name,
                "ordering": str(self.ordering_key or 999),
            },
            labels=[Label(external_id=al.PLANT)],
        )

    def relationships(self) -> list[Relationship]:
        time_series_to_append_if_not_none = {
            self.inlet_level_time_series: rl.INLET_LEVEL_TIME_SERIES,
            self.outlet_level_time_series: rl.OUTLET_LEVEL_TIME_SERIES,
            self.water_value_time_series: rl.WATER_VALUE_TIME_SERIES,
            self.feeding_fee_time_series: rl.FEEDING_FEE_TIME_SERIES,
            self.p_min_time_series: rl.P_MIN_TIME_SERIES,
            self.p_max_time_series: rl.P_MAX_TIME_SERIES,
            self.head_direct_time_series: rl.HEAD_DIRECT_TIME_SERIES,
        }
        relationships: list[Relationship] = [
            asset_to_time_series(self.external_id, time_series, label)
            for time_series, label in time_series_to_append_if_not_none.items()
            if time_series
        ]

        relationships.extend(
            plant_to_generator(plant=self.external_id, generator=generator) for generator in self.generator_ext_ids
        )
        if self.inlet_reservoir_ext_id:
            relationships.append(
                plant_to_inlet_reservoir(plant=self.external_id, reservoir=self.inlet_reservoir_ext_id)
            )

        return relationships

    def to_bootstrap_resources(self) -> BootstrapResourceCollection:
        asset = self.asset()
        relationships = self.relationships()
        return BootstrapResourceCollection(
            assets={asset.external_id: asset}, relationships={rel.external_id: rel for rel in relationships}
        )

    @classmethod
    def from_cdf_resources(cls, asset: Asset, relationships: list[Relationship], **kwargs) -> "Plant":
        """Initialise a Plant from CDF Asset and Relationships

        Args: asset (Asset): The plant Asset relationships (list[Relationship]): Relationships to related resources (
        will be mapped to attributes based on labels) **kwargs: Any other attributes that are not part of the Asset
        """
        # Initialise plant from Asset
        plant = cls(
            name=asset.name,
            external_id=asset.external_id,
            outlet_level=asset.metadata["outlet_level"],
            p_min=asset.metadata["p_min"],
            p_max=asset.metadata["p_max"],
            head_loss_factor=asset.metadata["head_loss_factor"],
            penstock_head_loss_factors=json.loads(asset.metadata.get("penstock_head_loss_factors")),
            **kwargs,  # kwargs to set any other attributes that are not part of the Asset
        )

        # Find time series based on relationships
        time_series_ext_ids_and_labels = [
            (rel.target_external_id, [label.external_id for label in rel.labels])
            for rel in relationships
            if rel.target_type == "TIMESERIES" and rel.source_external_id == plant.external_id
        ]
        for ts_ext_id, labels in time_series_ext_ids_and_labels:
            if rl.INLET_LEVEL_TIME_SERIES in labels:
                plant.inlet_level_time_series = ts_ext_id
            elif rl.OUTLET_LEVEL_TIME_SERIES in labels:
                plant.outlet_level_time_series = ts_ext_id
            elif rl.WATER_VALUE_TIME_SERIES in labels:
                plant.water_value_time_series = ts_ext_id
            elif rl.FEEDING_FEE_TIME_SERIES in labels:
                plant.feeding_fee_time_series = ts_ext_id
            elif rl.P_MIN_TIME_SERIES in labels:
                plant.p_min_time_series = ts_ext_id
            elif rl.P_MAX_TIME_SERIES in labels:
                plant.p_max_time_series = ts_ext_id
            elif rl.HEAD_DIRECT_TIME_SERIES in labels:
                plant.head_direct_time_series = ts_ext_id

        # Find generators based on relationships
        plant.generator_ext_ids = [
            rel.target_external_id for rel in relationships if label_in_labels(rl.GENERATOR, rel.labels)
        ]

        # Find inlet reservoir based on relationships
        for rel in relationships:
            if (
                rel.target_type == "ASSET"
                and label_in_labels(rl.INLET_RESERVOIR, rel.labels)
                and rel.source_external_id == plant.external_id
            ):
                plant.inlet_reservoir_ext_id = rel.target_external_id
                break

        return plant

    @classmethod
    def from_shop_case(cls, shop_case: dict, kwargs_per_plant: dict | None = None) -> dict[str, "Plant"]:
        """Returns a dict of Plant objects from a shop case dict
        Plants are only populated with the data that is in the shop case
        """
        if not kwargs_per_plant:
            kwargs_per_plant = {}
        plants = {
            name: cls(
                name=name,
                external_id=f"plant_{name}",
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
                **kwargs_per_plant.get(name, {}),
            )
            for name, attributes in shop_case["model"]["plant"].items()
        }
        # Add inlet reservoirs and generators from shop_case["connections"]
        all_connections = shop_case["connections"]
        reservoirs = shop_case["model"]["reservoir"]
        generators = shop_case["model"]["generator"]
        for plant_name, plant in plants.items():
            reservoir_name = plant_to_inlet_reservoir_breadth_first_search(plant_name, all_connections, reservoirs)
            plant.inlet_reservoir_ext_id = f"reservoir_{reservoir_name}"
            plant.generator_ext_ids = [
                f"generator_{gen_name}" for gen_name in generators_for_plant(plant_name, all_connections, generators)
            ]

        return plants

    @classmethod
    def add_time_series_mapping(
        cls,
        plant_time_series_mappings: list[PlantTimeSeriesMapping],
        plants: dict[PlantName, "Plant"],
    ) -> None:
        for mapping in plant_time_series_mappings:
            plant_name = mapping.plant_name
            # check if the plant is in the given watercourse (defined by the plants dict)
            if plant_name not in plants:
                continue

            plants[plant_name].water_value_time_series = mapping.water_value
            plants[plant_name].inlet_level_time_series = mapping.inlet_reservoir_level
            plants[plant_name].outlet_level_time_series = mapping.outlet_reservoir_level
            plants[plant_name].feeding_fee_time_series = mapping.feeding_fee
            plants[plant_name].p_min_time_series = mapping.p_min
            plants[plant_name].p_max_time_series = mapping.p_max
            plants[plant_name].head_direct_time_series = mapping.head_direct


def label_in_labels(label_external_id: str, labels: list[Label]) -> bool:
    """Check if a label with a given external id is in a list of labels."""
    return label_external_id in [label.external_id for label in labels]


def plant_to_inlet_reservoir_breadth_first_search(
    plant_name: str,
    all_connections: list[dict],
    reservoirs: dict,
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


def generators_for_plant(plant_name: str, all_connections: list[dict], generators) -> list[str]:
    """Returns a list of generators connected to a plant.

    Parameters
    ----------
    plant_name : str
        The name of the plant
    all_connections : list[dict]
        All connections in the model
    generators : list[str]
        Used to determine if the target of a connection is a generator (if to_type is not specified)

    Returns
    -------
    list[str]
        A list of generator names connected to the plant
    """
    return [
        connection["from"]
        for connection in all_connections
        if connection["to"] == plant_name
        and connection.get("to_type", "plant") == "plant"
        and (connection.get("from_type") == "generator" or connection["from"] in generators)
    ] + [  # Connections between plant and generator can be defined in either direction (it seems)
        connection["to"]
        for connection in all_connections
        if connection["from"] == plant_name
        and connection.get("from_type", "plant") == "plant"
        and (connection.get("to_type") == "generator" or connection["to"] in generators)
    ]


class Connection(NamedTuple):
    to_name: str
    from_name: str
    from_type: str
    to_type: str


def mip_plants(reserve_group: str, shop_case_yaml: str) -> List[str]:
    shop_case = load_yaml(Path(shop_case_yaml), clean_data=True)
    raw_connections = shop_case["connections"]
    connections = [
        Connection(
            to_name=str(connection["to"]),
            from_name=str(connection["from"]),
            to_type=connection["to_type"],
            from_type=connection["from_type"],
        )
        for connection in raw_connections
    ]
    # Find generators in the reserve groups
    reserve_group_to_generator = [
        connection.to_name
        for connection in connections
        if connection.to_type == "generator"
        and connection.from_type == "reserve_group"
        and connection.from_name == reserve_group
    ]
    generator_to_reserve_group = [
        connection.from_name
        for connection in connections
        if connection.from_type == "generator"
        and connection.to_type == "reserve_group"
        and connection.to_name == reserve_group
    ]
    generators_in_reserve_group = [*reserve_group_to_generator, *generator_to_reserve_group]

    # Find plants that the generators belong to
    generator_to_plant = [
        connection.to_name
        for connection in connections
        if connection.to_type == "plant"
        and connection.from_type == "generator"
        and connection.from_name in generators_in_reserve_group
    ]
    plant_to_generator = [
        connection.from_name
        for connection in connections
        if connection.from_type == "plant"
        and connection.to_type == "generator"
        and connection.to_name in generators_in_reserve_group
    ]
    return list({*generator_to_plant, *plant_to_generator})


def _match_plants_and_mip_flag(plants: List[str], mapping: TimeSeriesMapping) -> List[Tuple[str, Optional[str]]]:
    res: List[Tuple[str, Optional[str]]] = []
    for plant in plants:
        found = False
        for row in mapping:
            if row.object_name == plant and row.object_type == "plant" and row.attribute_name == "mip_flag":
                res.append((plant, row.time_series_external_id))
                print(f"\033[92m{plant}.mip_flag = {row.time_series_external_id}\033[0m")
                found = True
                break
        if not found:
            res.append((plant, None))
            print_warning(f"Did not find the `mip_flag` time series for {plant}!")
    print("\033[92m FINISHED \033[0m")
    return res
