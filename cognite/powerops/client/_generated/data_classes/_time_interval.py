from __future__ import annotations

import datetime
from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = ["TimeInterval", "TimeIntervalApply", "TimeIntervalList", "TimeIntervalApplyList", "TimeIntervalFields"]
TimeIntervalFields = Literal["start", "end"]

_TIMEINTERVAL_PROPERTIES_BY_FIELD = {
    "start": "Start",
    "end": "End",
}


class TimeInterval(DomainModel):
    space: str = "power-ops"
    start: Optional[datetime.datetime] = Field(None, alias="Start")
    end: Optional[datetime.datetime] = Field(None, alias="End")

    def as_apply(self) -> TimeIntervalApply:
        return TimeIntervalApply(
            external_id=self.external_id,
            start=self.start,
            end=self.end,
        )


class TimeIntervalApply(DomainModelApply):
    space: str = "power-ops"
    start: Optional[datetime.datetime] = Field(None, alias="Start")
    end: Optional[datetime.datetime] = Field(None, alias="End")

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.start is not None:
            properties["Start"] = self.start.isoformat(timespec="milliseconds")
        if self.end is not None:
            properties["End"] = self.end.isoformat(timespec="milliseconds")
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "TimeInterval"),
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


class TimeIntervalList(TypeList[TimeInterval]):
    _NODE = TimeInterval

    def as_apply(self) -> TimeIntervalApplyList:
        return TimeIntervalApplyList([node.as_apply() for node in self.data])


class TimeIntervalApplyList(TypeApplyList[TimeIntervalApply]):
    _NODE = TimeIntervalApply
