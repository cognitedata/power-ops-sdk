from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
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
        name: Name for the PriceArea.
        display_name: Display name for the PriceArea.
        ordering: The ordering of the reservoirs
        created_time: The created time of the reservoir node.
        last_updated_time: The last updated time of the reservoir node.
        deleted_time: If present, the deleted time of the reservoir node.
        version: The version of the reservoir node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    name: Optional[str] = None
    display_name: Optional[str] = Field(None, alias="displayName")
    ordering: Optional[int] = None

    def as_apply(self) -> ReservoirApply:
        """Convert this read version of reservoir to the writing version."""
        return ReservoirApply(
            space=self.space,
            external_id=self.external_id,
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
        name: Name for the PriceArea.
        display_name: Display name for the PriceArea.
        ordering: The ordering of the reservoirs
        existing_version: Fail the ingestion request if the reservoir version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    name: str
    display_name: Optional[str] = Field(None, alias="displayName")
    ordering: Optional[int] = None

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "power-ops-assets", "Reservoir", "1"
        )

        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.display_name is not None:
            properties["displayName"] = self.display_name
        if self.ordering is not None:
            properties["ordering"] = self.ordering

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                type=dm.DirectRelationReference("power-ops-types", "Reservoir"),
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
    if min_ordering or max_ordering:
        filters.append(dm.filters.Range(view_id.as_property_ref("ordering"), gte=min_ordering, lte=max_ordering))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
