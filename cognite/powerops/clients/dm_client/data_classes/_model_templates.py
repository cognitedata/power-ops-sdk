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
    from cognite.powerops.client.dm_client.data_classes._file_refs import FileRefApply
    from cognite.powerops.client.dm_client.data_classes._mappings import MappingApply

__all__ = ["ModelTemplate", "ModelTemplateApply", "ModelTemplateList"]


class ModelTemplate(DomainModel):
    space: ClassVar[str] = "cogShop"
    version: Optional[str] = None
    shop_version: Optional[str] = Field(None, alias="shopVersion")
    watercourse: Optional[str] = None
    model: Optional[str] = None
    base_mappings: list[str] = Field([], alias="baseMappings")


class ModelTemplateApply(CircularModelApply):
    space: ClassVar[str] = "cogShop"
    version: str
    shop_version: str
    watercourse: str
    model: Optional[Union[str, "FileRefApply"]] = None
    base_mappings: list[Union[str, "MappingApply"]] = []

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=[
                dm.NodeOrEdgeData(
                    source=dm.ContainerId("cogShop", "ModelTemplate"),
                    properties={
                        "version": self.version,
                        "shopVersion": self.shop_version,
                        "watercourse": self.watercourse,
                    },
                ),
            ],
        )
        nodes = [this_node]
        edges = []

        if self.model is not None:
            edge = self._create_model_edge(self.model)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(self.model, CircularModelApply):
                instances = self.model._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for base_mapping in self.base_mappings:
            edge = self._create_base_mapping_edge(base_mapping)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(base_mapping, CircularModelApply):
                instances = base_mapping._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return InstancesApply(nodes, edges)

    def _create_model_edge(self, model: Union[str, "CircularModelApply"]) -> dm.EdgeApply:
        if isinstance(model, str):
            end_node_ext_id = model
        elif isinstance(model, CircularModelApply):
            end_node_ext_id = model.external_id
        else:
            raise TypeError(f"Expected str or ModelApply, got {type(model)}")

        return dm.EdgeApply(
            space="cogShop",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("cogShop", "ModelTemplate.model"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("cogShop", end_node_ext_id),
        )

    def _create_base_mapping_edge(self, base_mapping: Union[str, "MappingApply"]) -> dm.EdgeApply:
        if isinstance(base_mapping, str):
            end_node_ext_id = base_mapping
        elif isinstance(base_mapping, CircularModelApply):
            end_node_ext_id = base_mapping.external_id
        else:
            raise TypeError(f"Expected str or BaseMappingApply, got {type(base_mapping)}")

        return dm.EdgeApply(
            space="cogShop",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("cogShop", "ModelTemplate.baseMappings"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("cogShop", end_node_ext_id),
        )


class ModelTemplateList(TypeList[ModelTemplate]):
    _NODE = ModelTemplate
