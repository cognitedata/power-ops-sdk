from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._plant import PlantApply
    from ._watercourse import WatercourseApply

__all__ = [
    "PriceArea",
    "PriceAreaApply",
    "PriceAreaList",
    "PriceAreaApplyList",
    "PriceAreaFields",
    "PriceAreaTextFields",
]


PriceAreaTextFields = Literal["name", "description", "dayahead_price_time_series"]
PriceAreaFields = Literal["name", "description", "dayahead_price_time_series"]

_PRICEAREA_PROPERTIES_BY_FIELD = {
    "name": "name",
    "description": "description",
    "dayahead_price_time_series": "dayaheadPriceTimeSeries",
}


class PriceArea(DomainModel):
    space: str = "power-ops"
    name: Optional[str] = None
    description: Optional[str] = None
    dayahead_price_time_series: Optional[str] = Field(None, alias="dayaheadPriceTimeSeries")
    plants: Optional[list[str]] = None
    watercourses: Optional[list[str]] = None

    def as_apply(self) -> PriceAreaApply:
        return PriceAreaApply(
            external_id=self.external_id,
            name=self.name,
            description=self.description,
            dayahead_price_time_series=self.dayahead_price_time_series,
            plants=self.plants,
            watercourses=self.watercourses,
        )


class PriceAreaApply(DomainModelApply):
    space: str = "power-ops"
    name: Optional[str] = None
    description: Optional[str] = None
    dayahead_price_time_series: Optional[str] = Field(None, alias="dayaheadPriceTimeSeries")
    plants: Union[list[PlantApply], list[str], None] = Field(default=None, repr=False)
    watercourses: Union[list[WatercourseApply], list[str], None] = Field(default=None, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.description is not None:
            properties["description"] = self.description
        if self.dayahead_price_time_series is not None:
            properties["dayaheadPriceTimeSeries"] = self.dayahead_price_time_series
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "PriceArea"),
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

        for plant in self.plants or []:
            edge = self._create_plant_edge(plant)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(plant, DomainModelApply):
                instances = plant._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for watercourse in self.watercourses or []:
            edge = self._create_watercourse_edge(watercourse)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(watercourse, DomainModelApply):
                instances = watercourse._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_plant_edge(self, plant: Union[str, PlantApply]) -> dm.EdgeApply:
        if isinstance(plant, str):
            end_node_ext_id = plant
        elif isinstance(plant, DomainModelApply):
            end_node_ext_id = plant.external_id
        else:
            raise TypeError(f"Expected str or PlantApply, got {type(plant)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "PriceArea.plants"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )

    def _create_watercourse_edge(self, watercourse: Union[str, WatercourseApply]) -> dm.EdgeApply:
        if isinstance(watercourse, str):
            end_node_ext_id = watercourse
        elif isinstance(watercourse, DomainModelApply):
            end_node_ext_id = watercourse.external_id
        else:
            raise TypeError(f"Expected str or WatercourseApply, got {type(watercourse)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "PriceArea.watercourses"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class PriceAreaList(TypeList[PriceArea]):
    _NODE = PriceArea

    def as_apply(self) -> PriceAreaApplyList:
        return PriceAreaApplyList([node.as_apply() for node in self.data])


class PriceAreaApplyList(TypeApplyList[PriceAreaApply]):
    _NODE = PriceAreaApply
