from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.client.dm_client.data_classes._core import (
    CircularModelApply,
    DomainModel,
    InstancesApply,
    TypeList,
)

if TYPE_CHECKING:
    from cognite.powerops.client.dm_client.data_classes._transformations import TransformationApply

__all__ = ["Mapping", "MappingApply", "MappingList"]


class Mapping(DomainModel):
    space: ClassVar[str] = "cogShop"
    path: Optional[str] = None
    timeseries_external_id: Optional[str] = Field(None, alias="timeseriesExternalId")
    retrieve: Optional[str] = None
    aggregation: Optional[str] = None
    transformations: list[str] = []


class MappingApply(CircularModelApply):
    space: ClassVar[str] = "cogShop"
    path: str
    timeseries_external_id: Optional[str] = None
    retrieve: Optional[str] = None
    aggregation: Optional[str] = None
    transformations: list[Union[str, "TransformationApply"]] = []

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=[
                dm.NodeOrEdgeData(
                    source=dm.ContainerId("cogShop", "Mapping"),
                    properties={
                        "path": self.path,
                        "timeseriesExternalId": self.timeseries_external_id,
                        "retrieve": self.retrieve,
                        "aggregation": self.aggregation,
                    },
                ),
            ],
        )
        nodes = [this_node]
        edges = []

        for transformation in self.transformations:
            edge = self._create_transformation_edge(transformation)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(transformation, CircularModelApply):
                instances = transformation._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return InstancesApply(nodes, edges)

    def _create_transformation_edge(self, transformation: Union[str, "TransformationApply"]) -> dm.EdgeApply:
        if isinstance(transformation, str):
            end_node_ext_id = transformation
        elif isinstance(transformation, CircularModelApply):
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
