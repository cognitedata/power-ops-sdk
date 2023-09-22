from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.client._generated.cogshop1.data_classes._core import DomainModel, DomainModelApply, TypeList

if TYPE_CHECKING:
    from cognite.powerops.client._generated.cogshop1.data_classes._transformations import TransformationApply

__all__ = ["Mapping", "MappingApply", "MappingList"]


class Mapping(DomainModel):
    space: ClassVar[str] = "cogShop"
    aggregation: Optional[str] = None
    path: Optional[str] = None
    retrieve: Optional[str] = None
    timeseries_external_id: Optional[str] = Field(None, alias="timeseriesExternalId")
    transformations: list[str] = []


class MappingApply(DomainModelApply):
    space: ClassVar[str] = "cogShop"
    aggregation: Optional[str] = None
    path: str
    retrieve: Optional[str] = None
    timeseries_external_id: Optional[str] = None
    transformations: list[Union[TransformationApply, str]] = Field(default_factory=list, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.aggregation is not None:
            properties["aggregation"] = self.aggregation
        if self.path is not None:
            properties["path"] = self.path
        if self.retrieve is not None:
            properties["retrieve"] = self.retrieve
        if self.timeseries_external_id is not None:
            properties["timeseriesExternalId"] = self.timeseries_external_id
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

        for transformation in self.transformations:
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
