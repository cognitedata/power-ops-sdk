from __future__ import annotations

from typing import Any, Literal, Optional, Union

from cognite.client import data_modeling as dm

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
from ._bid_method import BidMethod, BidMethodApply


__all__ = [
    "WaterValueBased",
    "WaterValueBasedApply",
    "WaterValueBasedList",
    "WaterValueBasedApplyList",
    "WaterValueBasedFields",
    "WaterValueBasedTextFields",
]


WaterValueBasedTextFields = Literal["name"]
WaterValueBasedFields = Literal["name"]

_WATERVALUEBASED_PROPERTIES_BY_FIELD = {
    "name": "name",
}


class WaterValueBased(BidMethod):
    """This represents the reading version of water value based.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the water value based.
        data_record: The data record of the water value based node.
        name: Name for the BidMethod
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "power-ops-types", "DayAheadWaterValueBased"
    )

    def as_apply(self) -> WaterValueBasedApply:
        """Convert this read version of water value based to the writing version."""
        return WaterValueBasedApply(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
        )


class WaterValueBasedApply(BidMethodApply):
    """This represents the writing version of water value based.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the water value based.
        data_record: The data record of the water value based node.
        name: Name for the BidMethod
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "power-ops-types", "DayAheadWaterValueBased"
    )

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(
            WaterValueBased, dm.ViewId("power-ops-day-ahead-bid", "WaterValueBased", "1")
        )

        properties: dict[str, Any] = {}

        if self.name is not None:
            properties["name"] = self.name

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


class WaterValueBasedList(DomainModelList[WaterValueBased]):
    """List of water value baseds in the read version."""

    _INSTANCE = WaterValueBased

    def as_apply(self) -> WaterValueBasedApplyList:
        """Convert these read versions of water value based to the writing versions."""
        return WaterValueBasedApplyList([node.as_apply() for node in self.data])


class WaterValueBasedApplyList(DomainModelApplyList[WaterValueBasedApply]):
    """List of water value baseds in the writing version."""

    _INSTANCE = WaterValueBasedApply


def _create_water_value_based_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
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
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
