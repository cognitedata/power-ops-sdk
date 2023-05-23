from typing import Optional

from cognite.client.data_classes import Asset, Label, Relationship

from cognite.powerops.config import GeneratorTimeSeriesMapping
from cognite.powerops.data_classes.cdf_resource_collection import BootstrapResourceCollection
from cognite.powerops.data_classes.shared import AssetModel
from cognite.powerops.utils.labels import AssetLabels
from cognite.powerops.utils.labels import RelationshipLabels as rl
from cognite.powerops.utils.relationship_types import asset_to_time_series

ExternalId = str
GeneratorName = str


class Generator(AssetModel):
    external_id: ExternalId
    name: GeneratorName
    penstock: str
    startcost: float

    start_stop_cost_time_series: Optional[ExternalId] = None  # external ID of time series with values in m

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
            metadata={"penstock": self.penstock, "startcost": self.startcost},
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
