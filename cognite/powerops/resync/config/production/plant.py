from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Optional

from cognite.client.data_classes import Asset, Label, Relationship
from pydantic import BaseModel, field_validator

from cognite.powerops.cdf_labels import RelationshipLabel as rl
from cognite.powerops.resync.config._shared import ExternalId

p_min_fallback = 0
p_max_fallback = 1e20

head_loss_factor_fallback = 0.0

PlantName = str


class Plant(BaseModel):
    name: PlantName
    external_id: ExternalId
    outlet_level: float  # meters above sea level
    p_min: float = p_min_fallback
    p_max: float = p_max_fallback  # arbitrary max power output if not specified
    head_loss_factor: float = 0.0
    penstock_head_loss_factors: Optional[dict[str, float]] = None
    connections_losses: Optional[float] = None
    display_name: Optional[str] = None
    ordering_key: Optional[float] = None

    generator_ext_ids: list[ExternalId] = []  # external IDs of generator assets
    inlet_reservoir_ext_id: Optional[ExternalId] = None  # external ID of reservoir asset

    inlet_level_time_series: Optional[ExternalId] = None  # external ID of time series with values in m.a.s.l.
    outlet_level_time_series: Optional[ExternalId] = None  # external ID of time series with values in m.a.s.l.
    water_value_time_series: Optional[ExternalId] = None  # external ID of time series with values in €/MWh
    feeding_fee_time_series: Optional[ExternalId] = None  # external ID of time series with values in percent
    p_min_time_series: Optional[ExternalId] = None  # external ID of time series with values in MW
    p_max_time_series: Optional[ExternalId] = None  # external ID of time series with values in MW
    head_direct_time_series: Optional[ExternalId] = None  # external ID of time series with values in m

    @field_validator("penstock_head_loss_factors", mode="before")
    def parse_dict(cls, value):
        if isinstance(value, str):
            value = json.loads(value)
        return value

    @classmethod
    def from_cdf_resources(cls, asset: Asset, relationships: list[Relationship], **kwargs) -> Plant:
        """Initialise a Plant from CDF Asset and Relationships

        Args:
            asset: The plant Asset relationships
            relationships: Relationships to related resources (will be mapped to attributes based on labels)
            kwargs: Any other attributes that are not part of the Asset

        """
        # Initialise plant from Asset
        plant = cls(
            name=asset.name,
            external_id=asset.external_id,
            outlet_level=float(asset.metadata["outlet_level"]),
            p_min=float(asset.metadata["p_min"]),
            p_max=float(asset.metadata["p_max"]),
            head_loss_factor=float(asset.metadata["head_loss_factor"]),
            connection_losses=asset.metadata.get("connection_losses") or None,
            penstock_head_loss_factors=json.loads(asset.metadata.get("penstock_head_loss_factors") or "{}"),
            **kwargs,  # kwargs to set any other attributes that are not part of the Asset
        )

        # Find time series based on relationships
        time_series_ext_ids_and_labels = [
            (rel.target_external_id, [label.external_id for label in (rel.labels or [])])
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
            rel.target_external_id for rel in relationships if label_in_labels(rl.GENERATOR, rel.labels or [])
        ]

        # Find inlet reservoir based on relationships
        for rel in relationships:
            if (
                rel.target_type == "ASSET"
                and label_in_labels(rl.INLET_RESERVOIR, rel.labels or [])
                and rel.source_external_id == plant.external_id
            ):
                plant.inlet_reservoir_ext_id = rel.target_external_id
                break

        return plant


def label_in_labels(label_external_id: str, labels: Sequence[Label]) -> bool:
    """Check if a label with a given external id is in a list of labels."""
    return label_external_id in [label.external_id for label in labels]


class PlantTimeSeriesMapping(BaseModel):
    plant_name: str
    water_value: Optional[ExternalId] = None
    inlet_reservoir_level: Optional[ExternalId] = None
    outlet_reservoir_level: Optional[ExternalId] = None
    p_min: Optional[ExternalId] = None
    p_max: Optional[ExternalId] = None
    feeding_fee: Optional[ExternalId] = None
    head_direct: Optional[ExternalId] = None

    @field_validator("*", mode="before")
    def parse_number_to_string(cls, value):
        return str(value) if isinstance(value, (int, float)) else value
