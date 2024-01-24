from __future__ import annotations

from typing import Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
)


__all__ = [
    "Reservoir",
    "ReservoirApply",
    "ReservoirList",
    "ReservoirApplyList",
    "ReservoirFields",
    "ReservoirTextFields",
]


ReservoirTextFields = Literal["name", "display_name"]
ReservoirFields = Literal["name", "display_name", "ordering"]

_RESERVOIR_PROPERTIES_BY_FIELD = {
    "name": "name",
    "display_name": "displayName",
    "ordering": "ordering",
}


class Reservoir(DomainModel):
    """This represents the reading version of reservoir.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the reservoir.
        data_record: The data record of the reservoir node.
        name: Name for the PriceArea.
        display_name: Display name for the PriceArea.
        ordering: The ordering of the reservoirs
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power-ops-types", "Reservoir")
    name: str
    display_name: Optional[str] = Field(None, alias="displayName")
    ordering: Optional[int] = None

    def as_apply(self) -> ReservoirApply:
        """Convert this read version of reservoir to the writing version."""
        return ReservoirApply(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            display_name=self.display_name,
            ordering=self.ordering,
        )


class ReservoirApply(DomainModelApply):
    """This represents the writing version of reservoir.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the reservoir.
        data_record: The data record of the reservoir node.
        name: Name for the PriceArea.
        display_name: Display name for the PriceArea.
        ordering: The ordering of the reservoirs
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power-ops-types", "Reservoir")
    name: str
    display_name: Optional[str] = Field(None, alias="displayName")
    ordering: Optional[int] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(Reservoir, dm.ViewId("power-ops-assets", "Reservoir", "1"))

        properties: dict[str, Any] = {}

        if self.name is not None:
            properties["name"] = self.name

        if self.display_name is not None or write_none:
            properties["displayName"] = self.display_name

        if self.ordering is not None or write_none:
            properties["ordering"] = self.ordering

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

        return resources


class ReservoirList(DomainModelList[Reservoir]):
    """List of reservoirs in the read version."""

    _INSTANCE = Reservoir

    def as_apply(self) -> ReservoirApplyList:
        """Convert these read versions of reservoir to the writing versions."""
        return ReservoirApplyList([node.as_apply() for node in self.data])


class ReservoirApplyList(DomainModelApplyList[ReservoirApply]):
    """List of reservoirs in the writing version."""

    _INSTANCE = ReservoirApply


def _create_reservoir_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    display_name: str | list[str] | None = None,
    display_name_prefix: str | None = None,
    min_ordering: int | None = None,
    max_ordering: int | None = None,
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
    if display_name is not None and isinstance(display_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("displayName"), value=display_name))
    if display_name and isinstance(display_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("displayName"), values=display_name))
    if display_name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("displayName"), value=display_name_prefix))
    if min_ordering or max_ordering:
        filters.append(dm.filters.Range(view_id.as_property_ref("ordering"), gte=min_ordering, lte=max_ordering))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
