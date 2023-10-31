from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._transformation import TransformationApply

__all__ = ["Mapping", "MappingApply", "MappingList", "MappingApplyList", "MappingFields", "MappingTextFields"]


MappingTextFields = Literal["path", "timeseries_external_id", "retrieve", "aggregation"]
MappingFields = Literal["path", "timeseries_external_id", "retrieve", "aggregation"]

_MAPPING_PROPERTIES_BY_FIELD = {
    "path": "path",
    "timeseries_external_id": "timeseriesExternalId",
    "retrieve": "retrieve",
    "aggregation": "aggregation",
}


class Mapping(DomainModel):
    space: str = "cogShop"
    path: Optional[str] = None
    timeseries_external_id: Optional[str] = Field(None, alias="timeseriesExternalId")
    retrieve: Optional[str] = None
    aggregation: Optional[str] = None
    transformations: Optional[list[str]] = None

    def as_apply(self) -> MappingApply:
        return MappingApply(
            external_id=self.external_id,
            path=self.path,
            timeseries_external_id=self.timeseries_external_id,
            retrieve=self.retrieve,
            aggregation=self.aggregation,
            transformations=self.transformations,
        )


class MappingApply(DomainModelApply):
    space: str = "cogShop"
    path: str
    timeseries_external_id: Optional[str] = Field(None, alias="timeseriesExternalId")
    retrieve: Optional[str] = None
    aggregation: Optional[str] = None
    transformations: Union[list[TransformationApply], list[str], None] = Field(default=None, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.path is not None:
            properties["path"] = self.path
        if self.timeseries_external_id is not None:
            properties["timeseriesExternalId"] = self.timeseries_external_id
        if self.retrieve is not None:
            properties["retrieve"] = self.retrieve
        if self.aggregation is not None:
            properties["aggregation"] = self.aggregation
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("cogShop", "Mapping"),
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

        for transformation in self.transformations or []:
            edge = self._create_transformation_edge(transformation)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(transformation, DomainModelApply):
                instances = transformation._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_transformation_edge(self, transformation: Union[str, TransformationApply]) -> dm.EdgeApply:
        if isinstance(transformation, str):
            end_node_ext_id = transformation
        elif isinstance(transformation, DomainModelApply):
            end_node_ext_id = transformation.external_id
        else:
            raise TypeError(f"Expected str or TransformationApply, got {type(transformation)}")

        return dm.EdgeApply(
            space="cogShop",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("cogShop", "Mapping.transformations"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("cogShop", end_node_ext_id),
        )


class MappingList(TypeList[Mapping]):
    _NODE = Mapping

    def as_apply(self) -> MappingApplyList:
        return MappingApplyList([node.as_apply() for node in self.data])


class MappingApplyList(TypeApplyList[MappingApply]):
    _NODE = MappingApply
