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


__all__ = [
    "TurbineEfficiencyCurve",
    "TurbineEfficiencyCurveApply",
    "TurbineEfficiencyCurveList",
    "TurbineEfficiencyCurveApplyList",
    "TurbineEfficiencyCurveFields",
]

TurbineEfficiencyCurveFields = Literal["head", "flow", "efficiency"]

_TURBINEEFFICIENCYCURVE_PROPERTIES_BY_FIELD = {
    "head": "head",
    "flow": "flow",
    "efficiency": "efficiency",
}


class TurbineEfficiencyCurve(DomainModel):
    """This represents the reading version of turbine efficiency curve.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the turbine efficiency curve.
        data_record: The data record of the turbine efficiency curve node.
        head: The reference head values
        flow: The flow values
        efficiency: The turbine efficiency values
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "power-ops-types", "TurbineEfficiencyCurve"
    )
    head: Optional[float] = None
    flow: Optional[list[float]] = None
    efficiency: Optional[list[float]] = None

    def as_apply(self) -> TurbineEfficiencyCurveApply:
        """Convert this read version of turbine efficiency curve to the writing version."""
        return TurbineEfficiencyCurveApply(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            head=self.head,
            flow=self.flow,
            efficiency=self.efficiency,
        )


class TurbineEfficiencyCurveApply(DomainModelApply):
    """This represents the writing version of turbine efficiency curve.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the turbine efficiency curve.
        data_record: The data record of the turbine efficiency curve node.
        head: The reference head values
        flow: The flow values
        efficiency: The turbine efficiency values
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "power-ops-types", "TurbineEfficiencyCurve"
    )
    head: Optional[float] = None
    flow: list[float]
    efficiency: list[float]

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
            TurbineEfficiencyCurve, dm.ViewId("power-ops-assets", "TurbineEfficiencyCurve", "1")
        )

        properties: dict[str, Any] = {}

        if self.head is not None or write_none:
            properties["head"] = self.head

        if self.flow is not None:
            properties["flow"] = self.flow

        if self.efficiency is not None:
            properties["efficiency"] = self.efficiency

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


class TurbineEfficiencyCurveList(DomainModelList[TurbineEfficiencyCurve]):
    """List of turbine efficiency curves in the read version."""

    _INSTANCE = TurbineEfficiencyCurve

    def as_apply(self) -> TurbineEfficiencyCurveApplyList:
        """Convert these read versions of turbine efficiency curve to the writing versions."""
        return TurbineEfficiencyCurveApplyList([node.as_apply() for node in self.data])


class TurbineEfficiencyCurveApplyList(DomainModelApplyList[TurbineEfficiencyCurveApply]):
    """List of turbine efficiency curves in the writing version."""

    _INSTANCE = TurbineEfficiencyCurveApply


def _create_turbine_efficiency_curve_filter(
    view_id: dm.ViewId,
    min_head: float | None = None,
    max_head: float | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_head or max_head:
        filters.append(dm.filters.Range(view_id.as_property_ref("head"), gte=min_head, lte=max_head))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
