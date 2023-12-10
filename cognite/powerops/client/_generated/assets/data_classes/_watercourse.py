from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
    TimeSeries,
)

if TYPE_CHECKING:
    from ._plant import Plant, PlantApply


__all__ = [
    "Watercourse",
    "WatercourseApply",
    "WatercourseList",
    "WatercourseApplyList",
    "WatercourseFields",
    "WatercourseTextFields",
]


WatercourseTextFields = Literal["name", "display_name", "production_obligation"]
WatercourseFields = Literal["name", "display_name", "production_obligation", "penalty_limit"]

_WATERCOURSE_PROPERTIES_BY_FIELD = {
    "name": "name",
    "display_name": "displayName",
    "production_obligation": "productionObligation",
    "penalty_limit": "penaltyLimit",
}


class Watercourse(DomainModel):
    """This represents the reading version of watercourse.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the watercourse.
        name: Name for the Watercourse.
        display_name: Display name for the Watercourse.
        production_obligation: The production obligation for the Watercourse.
        penalty_limit: The penalty limit for the watercourse (used by SHOP).
        plants: The plants that are connected to the Watercourse.
        created_time: The created time of the watercourse node.
        last_updated_time: The last updated time of the watercourse node.
        deleted_time: If present, the deleted time of the watercourse node.
        version: The version of the watercourse node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    name: Optional[str] = None
    display_name: Optional[str] = Field(None, alias="displayName")
    production_obligation: Optional[list[TimeSeries]] = Field(None, alias="productionObligation")
    penalty_limit: Optional[float] = Field(None, alias="penaltyLimit")
    plants: Union[list[Plant], list[str], None] = Field(default=None, repr=False)

    def as_apply(self) -> WatercourseApply:
        """Convert this read version of watercourse to the writing version."""
        return WatercourseApply(
            space=self.space,
            external_id=self.external_id,
            name=self.name,
            display_name=self.display_name,
            production_obligation=self.production_obligation,
            penalty_limit=self.penalty_limit,
            plants=[plant.as_apply() if isinstance(plant, DomainModel) else plant for plant in self.plants or []],
        )


class WatercourseApply(DomainModelApply):
    """This represents the writing version of watercourse.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the watercourse.
        name: Name for the Watercourse.
        display_name: Display name for the Watercourse.
        production_obligation: The production obligation for the Watercourse.
        penalty_limit: The penalty limit for the watercourse (used by SHOP).
        plants: The plants that are connected to the Watercourse.
        existing_version: Fail the ingestion request if the watercourse version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    name: str
    display_name: Optional[str] = Field(None, alias="displayName")
    production_obligation: Optional[list[TimeSeries]] = Field(None, alias="productionObligation")
    penalty_limit: Optional[float] = Field(None, alias="penaltyLimit")
    plants: Union[list[PlantApply], list[str], None] = Field(default=None, repr=False)

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "power-ops-assets", "Watercourse", "1"
        )

        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.display_name is not None:
            properties["displayName"] = self.display_name
        if self.production_obligation is not None:
            properties["productionObligation"] = self.production_obligation
        if self.penalty_limit is not None:
            properties["penaltyLimit"] = self.penalty_limit

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        edge_type = dm.DirectRelationReference("power-ops-types", "isSubAssetOf")
        for plant in self.plants or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, self, plant, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        if isinstance(self.production_obligation, CogniteTimeSeries):
            resources.time_series.append(self.production_obligation)

        return resources


class WatercourseList(DomainModelList[Watercourse]):
    """List of watercourses in the read version."""

    _INSTANCE = Watercourse

    def as_apply(self) -> WatercourseApplyList:
        """Convert these read versions of watercourse to the writing versions."""
        return WatercourseApplyList([node.as_apply() for node in self.data])


class WatercourseApplyList(DomainModelApplyList[WatercourseApply]):
    """List of watercourses in the writing version."""

    _INSTANCE = WatercourseApply


def _create_watercourse_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    display_name: str | list[str] | None = None,
    display_name_prefix: str | None = None,
    min_penalty_limit: float | None = None,
    max_penalty_limit: float | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if display_name and isinstance(display_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("displayName"), value=display_name))
    if display_name and isinstance(display_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("displayName"), values=display_name))
    if display_name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("displayName"), value=display_name_prefix))
    if min_penalty_limit or max_penalty_limit:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("penaltyLimit"), gte=min_penalty_limit, lte=max_penalty_limit)
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
