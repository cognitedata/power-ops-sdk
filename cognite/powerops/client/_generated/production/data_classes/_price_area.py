from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelCore,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
    TimeSeries,
)

if TYPE_CHECKING:
    from ._plant import Plant, PlantApply
    from ._watercourse import Watercourse, WatercourseApply


__all__ = [
    "PriceArea",
    "PriceAreaApply",
    "PriceAreaList",
    "PriceAreaApplyList",
    "PriceAreaFields",
    "PriceAreaTextFields",
]


PriceAreaTextFields = Literal["name", "description", "dayahead_price_time_series"]
PriceAreaFields = Literal["name", "description", "dayahead_price_time_series"]

_PRICEAREA_PROPERTIES_BY_FIELD = {
    "name": "name",
    "description": "description",
    "dayahead_price_time_series": "dayaheadPriceTimeSeries",
}


class PriceArea(DomainModel):
    """This represents the reading version of price area.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price area.
        name: The name field.
        description: The description field.
        dayahead_price_time_series: The dayahead price time series field.
        plants: The plant field.
        watercourses: The watercourse field.
        created_time: The created time of the price area node.
        last_updated_time: The last updated time of the price area node.
        deleted_time: If present, the deleted time of the price area node.
        version: The version of the price area node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    name: Optional[str] = None
    description: Optional[str] = None
    dayahead_price_time_series: Union[TimeSeries, str, None] = Field(None, alias="dayaheadPriceTimeSeries")
    plants: Union[list[Plant], list[str], None] = Field(default=None, repr=False)
    watercourses: Union[list[Watercourse], list[str], None] = Field(default=None, repr=False)

    def as_apply(self) -> PriceAreaApply:
        """Convert this read version of price area to the writing version."""
        return PriceAreaApply(
            space=self.space,
            external_id=self.external_id,
            name=self.name,
            description=self.description,
            dayahead_price_time_series=self.dayahead_price_time_series,
            plants=[plant.as_apply() if isinstance(plant, DomainModel) else plant for plant in self.plants or []],
            watercourses=[
                watercourse.as_apply() if isinstance(watercourse, DomainModel) else watercourse
                for watercourse in self.watercourses or []
            ],
        )


class PriceAreaApply(DomainModelApply):
    """This represents the writing version of price area.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the price area.
        name: The name field.
        description: The description field.
        dayahead_price_time_series: The dayahead price time series field.
        plants: The plant field.
        watercourses: The watercourse field.
        existing_version: Fail the ingestion request if the price area version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    name: Optional[str] = None
    description: Optional[str] = None
    dayahead_price_time_series: Union[TimeSeries, str, None] = Field(None, alias="dayaheadPriceTimeSeries")
    plants: Union[list[PlantApply], list[str], None] = Field(default=None, repr=False)
    watercourses: Union[list[WatercourseApply], list[str], None] = Field(default=None, repr=False)

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(PriceArea, dm.ViewId("power-ops", "PriceArea", "6849ae787cd368"))

        properties = {}

        if self.name is not None:
            properties["name"] = self.name

        if self.description is not None:
            properties["description"] = self.description

        if self.dayahead_price_time_series is not None:
            properties["dayaheadPriceTimeSeries"] = (
                self.dayahead_price_time_series
                if isinstance(self.dayahead_price_time_series, str)
                else self.dayahead_price_time_series.external_id
            )

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        edge_type = dm.DirectRelationReference("power-ops", "PriceArea.plants")
        for plant in self.plants or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, start_node=self, end_node=plant, edge_type=edge_type, view_by_read_class=view_by_read_class
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("power-ops", "PriceArea.watercourses")
        for watercourse in self.watercourses or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, start_node=self, end_node=watercourse, edge_type=edge_type, view_by_read_class=view_by_read_class
            )
            resources.extend(other_resources)

        if isinstance(self.dayahead_price_time_series, CogniteTimeSeries):
            resources.time_series.append(self.dayahead_price_time_series)

        return resources


class PriceAreaList(DomainModelList[PriceArea]):
    """List of price areas in the read version."""

    _INSTANCE = PriceArea

    def as_apply(self) -> PriceAreaApplyList:
        """Convert these read versions of price area to the writing versions."""
        return PriceAreaApplyList([node.as_apply() for node in self.data])


class PriceAreaApplyList(DomainModelApplyList[PriceAreaApply]):
    """List of price areas in the writing version."""

    _INSTANCE = PriceAreaApply


def _create_price_area_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    description: str | list[str] | None = None,
    description_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if name is not None and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if description is not None and isinstance(description, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("description"), value=description))
    if description and isinstance(description, list):
        filters.append(dm.filters.In(view_id.as_property_ref("description"), values=description))
    if description_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("description"), value=description_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
