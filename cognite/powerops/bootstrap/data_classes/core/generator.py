from __future__ import annotations

from typing import Optional

from cognite.client.data_classes import Asset, Label, Relationship
from pydantic import BaseModel, field_validator

from cognite.powerops._shared_data_classes import AssetLabels
from cognite.powerops._shared_data_classes import RelationshipLabels as rl
from cognite.powerops.bootstrap.data_classes.bootstrap_resource_collection import BootstrapResourceCollection
from cognite.powerops.bootstrap.data_classes.core._core import ExternalId
from cognite.powerops.bootstrap.to_cdf_resources.relationship_types import asset_to_time_series

GeneratorName = str


class Generator(BaseModel):
    name: GeneratorName
    penstock: str
    startcost: float
    p_min: float

    start_stop_cost_time_series: Optional[ExternalId] = None  # external ID of time series with values in m

    @property
    def external_id(self) -> ExternalId:
        return f"generator_{self.name}"

    @classmethod
    def add_time_series_mapping(
        cls,
        generator_time_series_mappings: list[GeneratorTimeSeriesMapping],
        generators: dict[GeneratorName, "Generator"],
    ):
        for mapping in generator_time_series_mappings:
            generator_name = mapping.generator_name
            # check if the generator is in the given watercourse (defined by the plants dict)
            if generator_name not in generators:
                continue

            generators[generator_name].start_stop_cost_time_series = mapping.start_stop_cost

    def to_bootstrap_resources(self) -> BootstrapResourceCollection:
        asset = self.asset()
        relationships = self.relationships()
        return BootstrapResourceCollection(
            assets={asset.external_id: asset}, relationships={rel.external_id: rel for rel in relationships}
        )

    def asset(self) -> Asset:
        return Asset(
            external_id=f"generator_{self.name}",
            name=self.name,
            parent_external_id="generators",
            labels=[Label(AssetLabels.GENERATOR)],
            metadata={
                "penstock": self.penstock,
                "startcost": self.startcost,
                "p_min": self.p_min,
            },
        )

    def relationships(self) -> list[Relationship]:
        time_series_to_append_if_not_none = {
            self.start_stop_cost_time_series: rl.START_STOP_COST_TIME_SERIES,
        }
        return [
            asset_to_time_series(self.external_id, time_series, label)
            for time_series, label in time_series_to_append_if_not_none.items()
            if time_series
        ]


class GeneratorTimeSeriesMapping(BaseModel):
    generator_name: str
    start_stop_cost: Optional[ExternalId] = None

    @field_validator("start_stop_cost", mode="before")
    def parset_number_to_string(cls, value):
        return str(value) if isinstance(value, (int, float)) else value
