from __future__ import annotations

import datetime
from typing import Literal, Optional

from cognite.client import data_modeling as dm

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = [
    "DateTimeInterval",
    "DateTimeIntervalApply",
    "DateTimeIntervalList",
    "DateTimeIntervalApplyList",
    "DateTimeIntervalFields",
]
DateTimeIntervalFields = Literal["start", "end"]

_DATETIMEINTERVAL_PROPERTIES_BY_FIELD = {
    "start": "start",
    "end": "end",
}


class DateTimeInterval(DomainModel):
    space: str = "power-ops"
    start: Optional[datetime.datetime] = None
    end: Optional[datetime.datetime] = None

    def as_apply(self) -> DateTimeIntervalApply:
        return DateTimeIntervalApply(
            external_id=self.external_id,
            start=self.start,
            end=self.end,
        )


class DateTimeIntervalApply(DomainModelApply):
    space: str = "power-ops"
    start: Optional[datetime.datetime] = None
    end: Optional[datetime.datetime] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.start is not None:
            properties["start"] = self.start.isoformat(timespec="milliseconds")
        if self.end is not None:
            properties["end"] = self.end.isoformat(timespec="milliseconds")
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "DateTimeInterval"),
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


class DateTimeIntervalList(TypeList[DateTimeInterval]):
    _NODE = DateTimeInterval

    def as_apply(self) -> DateTimeIntervalApplyList:
        return DateTimeIntervalApplyList([node.as_apply() for node in self.data])


class DateTimeIntervalApplyList(TypeApplyList[DateTimeIntervalApply]):
    _NODE = DateTimeIntervalApply
