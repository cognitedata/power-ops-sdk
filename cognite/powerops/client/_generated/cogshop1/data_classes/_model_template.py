from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._file_ref import FileRefApply
    from ._mapping import MappingApply

__all__ = ["ModelTemplate", "ModelTemplateApply", "ModelTemplateList", "ModelTemplateApplyList"]


class ModelTemplate(DomainModel, protected_namespaces=()):
    space: ClassVar[str] = "cogShop"
    version: Optional[str] = None
    shop_version: Optional[str] = Field(None, alias="shopVersion")
    watercourse: Optional[str] = None
    model: Optional[str] = None
    source: Optional[str] = None
    base_mappings: Optional[list[str]] = Field(None, alias="baseMappings")

    def as_apply(self) -> ModelTemplateApply:
        return ModelTemplateApply(
            external_id=self.external_id,
            version=self.version,
            shop_version=self.shop_version,
            watercourse=self.watercourse,
            model=self.model,
            source=self.source,
            base_mappings=self.base_mappings,
        )


class ModelTemplateApply(DomainModelApply, protected_namespaces=()):
    space: ClassVar[str] = "cogShop"
    version: str
    shop_version: str
    watercourse: str
    model: Union[FileRefApply, str, None] = Field(None, repr=False)
    source: Optional[str] = None
    base_mappings: Union[list[MappingApply], list[str], None] = Field(default=None, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.version is not None:
            properties["version"] = self.version
        if self.shop_version is not None:
            properties["shopVersion"] = self.shop_version
        if self.watercourse is not None:
            properties["watercourse"] = self.watercourse
        if self.model is not None:
            properties["model"] = {
                "space": "cogShop",
                "externalId": self.model if isinstance(self.model, str) else self.model.external_id,
            }
        if self.source is not None:
            properties["source"] = self.source
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("cogShop", "ModelTemplate"),
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

        for base_mapping in self.base_mappings or []:
            edge = self._create_base_mapping_edge(base_mapping)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(base_mapping, DomainModelApply):
                instances = base_mapping._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        if isinstance(self.model, DomainModelApply):
            instances = self.model._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_base_mapping_edge(self, base_mapping: Union[str, MappingApply]) -> dm.EdgeApply:
        if isinstance(base_mapping, str):
            end_node_ext_id = base_mapping
        elif isinstance(base_mapping, DomainModelApply):
            end_node_ext_id = base_mapping.external_id
        else:
            raise TypeError(f"Expected str or MappingApply, got {type(base_mapping)}")

        return dm.EdgeApply(
            space="cogShop",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("cogShop", "ModelTemplate.baseMappings"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("cogShop", end_node_ext_id),
        )


class ModelTemplateList(TypeList[ModelTemplate]):
    _NODE = ModelTemplate

    def as_apply(self) -> ModelTemplateApplyList:
        return ModelTemplateApplyList([node.as_apply() for node in self.data])


class ModelTemplateApplyList(TypeApplyList[ModelTemplateApply]):
    _NODE = ModelTemplateApply
