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
    "GeneratorEfficiencyCurve",
    "GeneratorEfficiencyCurveApply",
    "GeneratorEfficiencyCurveList",
    "GeneratorEfficiencyCurveApplyList",
    "GeneratorEfficiencyCurveFields",
]

GeneratorEfficiencyCurveFields = Literal["ref", "power", "efficiency"]

_GENERATOREFFICIENCYCURVE_PROPERTIES_BY_FIELD = {
    "ref": "ref",
    "power": "power",
    "efficiency": "efficiency",
}


class GeneratorEfficiencyCurve(DomainModel):
    """This represents the reading version of generator efficiency curve.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the generator efficiency curve.
        data_record: The data record of the generator efficiency curve node.
        ref: The reference value
        power: The generator power values
        efficiency: The generator efficiency values
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "power-ops-types", "GeneratorEfficiencyCurve"
    )
    ref: Optional[float] = None
    power: Optional[list[float]] = None
    efficiency: Optional[list[float]] = None

    def as_apply(self) -> GeneratorEfficiencyCurveApply:
        """Convert this read version of generator efficiency curve to the writing version."""
        return GeneratorEfficiencyCurveApply(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            ref=self.ref,
            power=self.power,
            efficiency=self.efficiency,
        )


class GeneratorEfficiencyCurveApply(DomainModelApply):
    """This represents the writing version of generator efficiency curve.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the generator efficiency curve.
        data_record: The data record of the generator efficiency curve node.
        ref: The reference value
        power: The generator power values
        efficiency: The generator efficiency values
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "power-ops-types", "GeneratorEfficiencyCurve"
    )
    ref: Optional[float] = None
    power: list[float]
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
            GeneratorEfficiencyCurve, dm.ViewId("power-ops-assets", "GeneratorEfficiencyCurve", "1")
        )

        properties: dict[str, Any] = {}

        if self.ref is not None or write_none:
            properties["ref"] = self.ref

        if self.power is not None:
            properties["power"] = self.power

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


class GeneratorEfficiencyCurveList(DomainModelList[GeneratorEfficiencyCurve]):
    """List of generator efficiency curves in the read version."""

    _INSTANCE = GeneratorEfficiencyCurve

    def as_apply(self) -> GeneratorEfficiencyCurveApplyList:
        """Convert these read versions of generator efficiency curve to the writing versions."""
        return GeneratorEfficiencyCurveApplyList([node.as_apply() for node in self.data])


class GeneratorEfficiencyCurveApplyList(DomainModelApplyList[GeneratorEfficiencyCurveApply]):
    """List of generator efficiency curves in the writing version."""

    _INSTANCE = GeneratorEfficiencyCurveApply


def _create_generator_efficiency_curve_filter(
    view_id: dm.ViewId,
    min_ref: float | None = None,
    max_ref: float | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if min_ref or max_ref:
        filters.append(dm.filters.Range(view_id.as_property_ref("ref"), gte=min_ref, lte=max_ref))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
