from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    ResourcesWrite,
    TimeSeries,
)

if TYPE_CHECKING:
    from ._plant import Plant, PlantWrite


__all__ = [
    "Watercourse",
    "WatercourseWrite",
    "WatercourseApply",
    "WatercourseList",
    "WatercourseWriteList",
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
        data_record: The data record of the watercourse node.
        name: Name for the Watercourse.
        display_name: Display name for the Watercourse.
        production_obligation: The production obligation for the Watercourse.
        penalty_limit: The penalty limit for the watercourse (used by SHOP).
        plants: The plants that are connected to the Watercourse.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power-ops-types", "Watercourse")
    name: str
    display_name: Optional[str] = Field(None, alias="displayName")
    production_obligation: Union[list[TimeSeries], list[str], None] = Field(None, alias="productionObligation")
    penalty_limit: Optional[float] = Field(None, alias="penaltyLimit")
    plants: Union[list[Plant], list[str], None] = Field(default=None, repr=False)

    def as_write(self) -> WatercourseWrite:
        """Convert this read version of watercourse to the writing version."""
        return WatercourseWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            display_name=self.display_name,
            production_obligation=self.production_obligation,
            penalty_limit=self.penalty_limit,
            plants=[plant.as_write() if isinstance(plant, DomainModel) else plant for plant in self.plants or []],
        )

    def as_apply(self) -> WatercourseWrite:
        """Convert this read version of watercourse to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class WatercourseWrite(DomainModelWrite):
    """This represents the writing version of watercourse.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the watercourse.
        data_record: The data record of the watercourse node.
        name: Name for the Watercourse.
        display_name: Display name for the Watercourse.
        production_obligation: The production obligation for the Watercourse.
        penalty_limit: The penalty limit for the watercourse (used by SHOP).
        plants: The plants that are connected to the Watercourse.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power-ops-types", "Watercourse")
    name: str
    display_name: Optional[str] = Field(None, alias="displayName")
    production_obligation: Union[list[TimeSeries], list[str], None] = Field(None, alias="productionObligation")
    penalty_limit: Optional[float] = Field(None, alias="penaltyLimit")
    plants: Union[list[PlantWrite], list[str], None] = Field(default=None, repr=False)

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(Watercourse, dm.ViewId("power-ops-assets", "Watercourse", "1"))

        properties: dict[str, Any] = {}

        if self.name is not None:
            properties["name"] = self.name

        if self.display_name is not None or write_none:
            properties["displayName"] = self.display_name

        if self.production_obligation is not None or write_none:
            properties["productionObligation"] = [
                value if isinstance(value, str) else value.external_id for value in self.production_obligation or []
            ] or None

        if self.penalty_limit is not None or write_none:
            properties["penaltyLimit"] = self.penalty_limit

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.data_record.existing_version,
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

        edge_type = dm.DirectRelationReference("power-ops-types", "isSubAssetOf")
        for plant in self.plants or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache, start_node=self, end_node=plant, edge_type=edge_type, view_by_read_class=view_by_read_class
            )
            resources.extend(other_resources)

        if isinstance(self.production_obligation, CogniteTimeSeries):
            resources.time_series.append(self.production_obligation)

        return resources


class WatercourseApply(WatercourseWrite):
    def __new__(cls, *args, **kwargs) -> WatercourseApply:
        warnings.warn(
            "WatercourseApply is deprecated and will be removed in v1.0. Use WatercourseWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Watercourse.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class WatercourseList(DomainModelList[Watercourse]):
    """List of watercourses in the read version."""

    _INSTANCE = Watercourse

    def as_write(self) -> WatercourseWriteList:
        """Convert these read versions of watercourse to the writing versions."""
        return WatercourseWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> WatercourseWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class WatercourseWriteList(DomainModelWriteList[WatercourseWrite]):
    """List of watercourses in the writing version."""

    _INSTANCE = WatercourseWrite


class WatercourseApplyList(WatercourseWriteList): ...


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
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if isinstance(display_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("displayName"), value=display_name))
    if display_name and isinstance(display_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("displayName"), values=display_name))
    if display_name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("displayName"), value=display_name_prefix))
    if min_penalty_limit is not None or max_penalty_limit is not None:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("penaltyLimit"), gte=min_penalty_limit, lte=max_penalty_limit)
        )
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
