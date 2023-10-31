from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._duration import DurationApply
    from ._point import PointApply

__all__ = ["Series", "SeriesApply", "SeriesList", "SeriesApplyList", "SeriesFields"]
SeriesFields = Literal["time_interval_start", "time_interval_end"]

_SERIES_PROPERTIES_BY_FIELD = {
    "time_interval_start": "timeIntervalStart",
    "time_interval_end": "timeIntervalEnd",
}


class Series(DomainModel):
    space: str = "power-ops"
    time_interval_start: Optional[datetime.datetime] = Field(None, alias="timeIntervalStart")
    time_interval_end: Optional[datetime.datetime] = Field(None, alias="timeIntervalEnd")
    resolution: Optional[str] = None
    points: Optional[list[str]] = None

    def as_apply(self) -> SeriesApply:
        return SeriesApply(
            external_id=self.external_id,
            time_interval_start=self.time_interval_start,
            time_interval_end=self.time_interval_end,
            resolution=self.resolution,
            points=self.points,
        )


class SeriesApply(DomainModelApply):
    space: str = "power-ops"
    time_interval_start: Optional[datetime.datetime] = Field(None, alias="timeIntervalStart")
    time_interval_end: Optional[datetime.datetime] = Field(None, alias="timeIntervalEnd")
    resolution: Union[DurationApply, str, None] = Field(None, repr=False)
    points: Union[list[PointApply], list[str], None] = Field(default=None, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.time_interval_start is not None:
            properties["timeIntervalStart"] = self.time_interval_start.isoformat(timespec="milliseconds")
        if self.time_interval_end is not None:
            properties["timeIntervalEnd"] = self.time_interval_end.isoformat(timespec="milliseconds")
        if self.resolution is not None:
            properties["resolution"] = {
                "space": "power-ops",
                "externalId": self.resolution if isinstance(self.resolution, str) else self.resolution.external_id,
            }
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "Series"),
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

        for point in self.points or []:
            edge = self._create_point_edge(point)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(point, DomainModelApply):
                instances = point._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        if isinstance(self.resolution, DomainModelApply):
            instances = self.resolution._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_point_edge(self, point: Union[str, PointApply]) -> dm.EdgeApply:
        if isinstance(point, str):
            end_node_ext_id = point
        elif isinstance(point, DomainModelApply):
            end_node_ext_id = point.external_id
        else:
            raise TypeError(f"Expected str or PointApply, got {type(point)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "Series.points"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class SeriesList(TypeList[Series]):
    _NODE = Series

    def as_apply(self) -> SeriesApplyList:
        return SeriesApplyList([node.as_apply() for node in self.data])


class SeriesApplyList(TypeApplyList[SeriesApply]):
    _NODE = SeriesApply
