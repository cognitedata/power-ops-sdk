from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._output_mapping import OutputMappingApply

__all__ = [
    "OutputContainer",
    "OutputContainerApply",
    "OutputContainerList",
    "OutputContainerApplyList",
    "OutputContainerFields",
    "OutputContainerTextFields",
]


OutputContainerTextFields = Literal["name", "watercourse", "shop_type"]
OutputContainerFields = Literal["name", "watercourse", "shop_type"]

_OUTPUTCONTAINER_PROPERTIES_BY_FIELD = {
    "name": "name",
    "watercourse": "watercourse",
    "shop_type": "shopType",
}


class OutputContainer(DomainModel):
    space: str = "power-ops"
    name: Optional[str] = None
    watercourse: Optional[str] = None
    shop_type: Optional[str] = Field(None, alias="shopType")
    mappings: Optional[list[str]] = None

    def as_apply(self) -> OutputContainerApply:
        return OutputContainerApply(
            external_id=self.external_id,
            name=self.name,
            watercourse=self.watercourse,
            shop_type=self.shop_type,
            mappings=self.mappings,
        )


class OutputContainerApply(DomainModelApply):
    space: str = "power-ops"
    name: Optional[str] = None
    watercourse: Optional[str] = None
    shop_type: Optional[str] = Field(None, alias="shopType")
    mappings: Union[list[OutputMappingApply], list[str], None] = Field(default=None, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.watercourse is not None:
            properties["watercourse"] = self.watercourse
        if self.shop_type is not None:
            properties["shopType"] = self.shop_type
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "OutputContainer"),
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

        for mapping in self.mappings or []:
            edge = self._create_mapping_edge(mapping)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(mapping, DomainModelApply):
                instances = mapping._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_mapping_edge(self, mapping: Union[str, OutputMappingApply]) -> dm.EdgeApply:
        if isinstance(mapping, str):
            end_node_ext_id = mapping
        elif isinstance(mapping, DomainModelApply):
            end_node_ext_id = mapping.external_id
        else:
            raise TypeError(f"Expected str or OutputMappingApply, got {type(mapping)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "OutputContainer.mappings"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class OutputContainerList(TypeList[OutputContainer]):
    _NODE = OutputContainer

    def as_apply(self) -> OutputContainerApplyList:
        return OutputContainerApplyList([node.as_apply() for node in self.data])


class OutputContainerApplyList(TypeApplyList[OutputContainerApply]):
    _NODE = OutputContainerApply
