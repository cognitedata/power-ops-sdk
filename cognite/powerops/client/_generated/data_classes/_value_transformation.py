from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = [
    "ValueTransformation",
    "ValueTransformationApply",
    "ValueTransformationList",
    "ValueTransformationApplyList",
    "ValueTransformationFields",
    "ValueTransformationTextFields",
]


ValueTransformationTextFields = Literal["method"]
ValueTransformationFields = Literal["method", "arguments"]

_VALUETRANSFORMATION_PROPERTIES_BY_FIELD = {
    "method": "method",
    "arguments": "arguments",
}


class ValueTransformation(DomainModel):
    space: str = "power-ops"
    method: Optional[str] = None
    arguments: Optional[dict] = None

    def as_apply(self) -> ValueTransformationApply:
        return ValueTransformationApply(
            external_id=self.external_id,
            method=self.method,
            arguments=self.arguments,
        )


class ValueTransformationApply(DomainModelApply):
    space: str = "power-ops"
    method: Optional[str] = None
    arguments: Optional[dict] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.method is not None:
            properties["method"] = self.method
        if self.arguments is not None:
            properties["arguments"] = self.arguments
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "ValueTransformation"),
                properties=properties,
            )
            sources.append(source)
        if sources:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=sources,
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class ValueTransformationList(TypeList[ValueTransformation]):
    _NODE = ValueTransformation

    def as_apply(self) -> ValueTransformationApplyList:
        return ValueTransformationApplyList([node.as_apply() for node in self.data])


class ValueTransformationApplyList(TypeApplyList[ValueTransformationApply]):
    _NODE = ValueTransformationApply
