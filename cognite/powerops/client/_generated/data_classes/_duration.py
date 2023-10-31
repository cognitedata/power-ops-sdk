from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = ["Duration", "DurationApply", "DurationList", "DurationApplyList", "DurationFields", "DurationTextFields"]


DurationTextFields = Literal["unit"]
DurationFields = Literal["duration", "unit"]

_DURATION_PROPERTIES_BY_FIELD = {
    "duration": "duration",
    "unit": "unit",
}


class Duration(DomainModel):
    space: str = "power-ops"
    duration: Optional[int] = None
    unit: Optional[str] = None

    def as_apply(self) -> DurationApply:
        return DurationApply(
            external_id=self.external_id,
            duration=self.duration,
            unit=self.unit,
        )


class DurationApply(DomainModelApply):
    space: str = "power-ops"
    duration: Optional[int] = None
    unit: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.duration is not None:
            properties["duration"] = self.duration
        if self.unit is not None:
            properties["unit"] = self.unit
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "Duration"),
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


class DurationList(TypeList[Duration]):
    _NODE = Duration

    def as_apply(self) -> DurationApplyList:
        return DurationApplyList([node.as_apply() for node in self.data])


class DurationApplyList(TypeApplyList[DurationApply]):
    _NODE = DurationApply
