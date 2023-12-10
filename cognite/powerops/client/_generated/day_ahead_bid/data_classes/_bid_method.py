from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

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
    "BidMethod",
    "BidMethodApply",
    "BidMethodList",
    "BidMethodApplyList",
    "BidMethodFields",
    "BidMethodTextFields",
]


BidMethodTextFields = Literal["name"]
BidMethodFields = Literal["name"]

_BIDMETHOD_PROPERTIES_BY_FIELD = {
    "name": "name",
}


class BidMethod(DomainModel):
    """This represents the reading version of bid method.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid method.
        name: Name for the BidMethod
        created_time: The created time of the bid method node.
        last_updated_time: The last updated time of the bid method node.
        deleted_time: If present, the deleted time of the bid method node.
        version: The version of the bid method node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    name: Optional[str] = None

    def as_apply(self) -> BidMethodApply:
        """Convert this read version of bid method to the writing version."""
        return BidMethodApply(
            space=self.space,
            external_id=self.external_id,
            name=self.name,
        )


class BidMethodApply(DomainModelApply):
    """This represents the writing version of bid method.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the bid method.
        name: Name for the BidMethod
        existing_version: Fail the ingestion request if the bid method version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    name: str

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "power-ops-day-ahead-bid", "BidMethod", "1"
        )

        properties = {}
        if self.name is not None:
            properties["name"] = self.name

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                type=dm.DirectRelationReference("power-ops-types", "DayAheadBidMethod"),
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


class BidMethodList(DomainModelList[BidMethod]):
    """List of bid methods in the read version."""

    _INSTANCE = BidMethod

    def as_apply(self) -> BidMethodApplyList:
        """Convert these read versions of bid method to the writing versions."""
        return BidMethodApplyList([node.as_apply() for node in self.data])


class BidMethodApplyList(DomainModelApplyList[BidMethodApply]):
    """List of bid methods in the writing version."""

    _INSTANCE = BidMethodApply


def _create_bid_method_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
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
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
