from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = [
    "DateTransformation",
    "DateTransformationApply",
    "DateTransformationList",
    "DateTransformationApplyList",
    "DateTransformationFields",
    "DateTransformationTextFields",
]


DateTransformationTextFields = Literal["transformation", "args"]
DateTransformationFields = Literal["transformation", "args", "kwargs"]

_DATETRANSFORMATION_PROPERTIES_BY_FIELD = {
    "transformation": "transformation",
    "args": "args",
    "kwargs": "kwargs",
}


class DateTransformation(DomainModel):
    space: str = "power-ops"
    transformation: Optional[str] = None
    args: Optional[list[str]] = None
    kwargs: Optional[dict] = None

    def as_apply(self) -> DateTransformationApply:
        return DateTransformationApply(
            external_id=self.external_id,
            transformation=self.transformation,
            args=self.args,
            kwargs=self.kwargs,
        )


class DateTransformationApply(DomainModelApply):
    space: str = "power-ops"
    transformation: Optional[str] = None
    args: Optional[list[str]] = None
    kwargs: Optional[dict] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.transformation is not None:
            properties["transformation"] = self.transformation
        if self.args is not None:
            properties["args"] = self.args
        if self.kwargs is not None:
            properties["kwargs"] = self.kwargs
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "DateTransformation"),
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


class DateTransformationList(TypeList[DateTransformation]):
    _NODE = DateTransformation

    def as_apply(self) -> DateTransformationApplyList:
        return DateTransformationApplyList([node.as_apply() for node in self.data])


class DateTransformationApplyList(TypeApplyList[DateTransformationApply]):
    _NODE = DateTransformationApply
