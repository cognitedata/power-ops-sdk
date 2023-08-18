from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.cogshop1.data_classes._core import DomainModel, DomainModelApply, TypeList

if TYPE_CHECKING:
    from cognite.powerops.cogshop1.data_classes._file_refs import FileRefApply
    from cognite.powerops.cogshop1.data_classes._mappings import MappingApply

__all__ = ["ModelTemplate", "ModelTemplateApply", "ModelTemplateList"]


class ModelTemplate(DomainModel):
    space: ClassVar[str] = "cogShop"
    base_mappings: list[str] = Field([], alias="baseMappings")
    model: Optional[str] = None
    shop_version: Optional[str] = Field(None, alias="shopVersion")
    version: Optional[str] = None
    watercourse: Optional[str] = None


class ModelTemplateApply(DomainModelApply):
    space: ClassVar[str] = "cogShop"
    base_mappings: list[Union["MappingApply", str]] = Field(default_factory=list, repr=False)
    model: Optional[Union["FileRefApply", str]] = Field(None, repr=False)
    shop_version: str
    version: str
    watercourse: str

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("cogShop", "ModelTemplate"),
            properties={
                "model": {
                    "space": "cogShop",
                    "externalId": self.model if isinstance(self.model, str) else self.model.external_id,
                },
                "shopVersion": self.shop_version,
                "version": self.version,
                "watercourse": self.watercourse,
            },
        )
        sources.append(source)

        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=sources,
        )
        nodes = [this_node]
        edges = []
        cache.add(self.external_id)

        for base_mapping in self.base_mappings:
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

    def _create_base_mapping_edge(self, base_mapping: Union[str, "MappingApply"]) -> dm.EdgeApply:
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
