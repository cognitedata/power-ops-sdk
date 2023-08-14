from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.clients.data_classes._core import DomainModel, DomainModelApply, TypeList

if TYPE_CHECKING:
    from cognite.powerops.clients.data_classes._output_containers import OutputContainerApply
    from cognite.powerops.clients.data_classes._scenario_mappings import ScenarioMappingApply

__all__ = ["ScenarioTemplate", "ScenarioTemplateApply", "ScenarioTemplateList"]


class ScenarioTemplate(DomainModel):
    space: ClassVar[str] = "power-ops"
    base_mapping: Optional[str] = Field(None, alias="baseMapping")
    model: Optional[str] = None
    output_definitions: Optional[str] = Field(None, alias="outputDefinitions")
    shop_files: list[str] = Field([], alias="shopFiles")
    shop_version: Optional[str] = Field(None, alias="shopVersion")
    template_version: Optional[str] = Field(None, alias="templateVersion")
    watercourse: Optional[str] = None


class ScenarioTemplateApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    base_mapping: Optional[Union["ScenarioMappingApply", str]] = Field(None, repr=False)
    model: Optional[str] = None
    output_definitions: Optional[Union["OutputContainerApply", str]] = Field(None, repr=False)
    shop_files: list[str] = []
    shop_version: Optional[str] = None
    template_version: Optional[str] = None
    watercourse: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "ScenarioTemplate"),
            properties={
                "baseMapping": {
                    "space": "power-ops",
                    "externalId": self.base_mapping
                    if isinstance(self.base_mapping, str)
                    else self.base_mapping.external_id,
                },
                "model": self.model,
                "outputDefinitions": {
                    "space": "power-ops",
                    "externalId": self.output_definitions
                    if isinstance(self.output_definitions, str)
                    else self.output_definitions.external_id,
                },
                "shopFiles": self.shop_files,
                "shopVersion": self.shop_version,
                "templateVersion": self.template_version,
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

        if isinstance(self.base_mapping, DomainModelApply):
            instances = self.base_mapping._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.output_definitions, DomainModelApply):
            instances = self.output_definitions._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class ScenarioTemplateList(TypeList[ScenarioTemplate]):
    _NODE = ScenarioTemplate
