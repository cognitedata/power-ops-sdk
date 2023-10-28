from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._output_container import OutputContainerApply
    from ._scenario_mapping import ScenarioMappingApply

__all__ = [
    "ScenarioTemplate",
    "ScenarioTemplateApply",
    "ScenarioTemplateList",
    "ScenarioTemplateApplyList",
    "ScenarioTemplateFields",
    "ScenarioTemplateTextFields",
]


ScenarioTemplateTextFields = Literal["watercourse", "shop_version", "template_version", "model", "shop_files"]
ScenarioTemplateFields = Literal["watercourse", "shop_version", "template_version", "model", "shop_files"]

_SCENARIOTEMPLATE_PROPERTIES_BY_FIELD = {
    "watercourse": "watercourse",
    "shop_version": "shopVersion",
    "template_version": "templateVersion",
    "model": "model",
    "shop_files": "shopFiles",
}


class ScenarioTemplate(DomainModel):
    space: str = "power-ops"
    watercourse: Optional[str] = None
    shop_version: Optional[str] = Field(None, alias="shopVersion")
    template_version: Optional[str] = Field(None, alias="templateVersion")
    model: Optional[str] = None
    shop_files: Optional[list[str]] = Field(None, alias="shopFiles")
    base_mapping: Optional[str] = Field(None, alias="baseMapping")
    output_definitions: Optional[str] = Field(None, alias="outputDefinitions")

    def as_apply(self) -> ScenarioTemplateApply:
        return ScenarioTemplateApply(
            external_id=self.external_id,
            watercourse=self.watercourse,
            shop_version=self.shop_version,
            template_version=self.template_version,
            model=self.model,
            shop_files=self.shop_files,
            base_mapping=self.base_mapping,
            output_definitions=self.output_definitions,
        )


class ScenarioTemplateApply(DomainModelApply):
    space: str = "power-ops"
    watercourse: Optional[str] = None
    shop_version: Optional[str] = Field(None, alias="shopVersion")
    template_version: Optional[str] = Field(None, alias="templateVersion")
    model: Optional[str] = None
    shop_files: Optional[list[str]] = Field(None, alias="shopFiles")
    base_mapping: Union[ScenarioMappingApply, str, None] = Field(None, repr=False, alias="baseMapping")
    output_definitions: Union[OutputContainerApply, str, None] = Field(None, repr=False, alias="outputDefinitions")

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.watercourse is not None:
            properties["watercourse"] = self.watercourse
        if self.shop_version is not None:
            properties["shopVersion"] = self.shop_version
        if self.template_version is not None:
            properties["templateVersion"] = self.template_version
        if self.model is not None:
            properties["model"] = self.model
        if self.shop_files is not None:
            properties["shopFiles"] = self.shop_files
        if self.base_mapping is not None:
            properties["baseMapping"] = {
                "space": "power-ops",
                "externalId": self.base_mapping
                if isinstance(self.base_mapping, str)
                else self.base_mapping.external_id,
            }
        if self.output_definitions is not None:
            properties["outputDefinitions"] = {
                "space": "power-ops",
                "externalId": self.output_definitions
                if isinstance(self.output_definitions, str)
                else self.output_definitions.external_id,
            }
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "ScenarioTemplate"),
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

    def as_apply(self) -> ScenarioTemplateApplyList:
        return ScenarioTemplateApplyList([node.as_apply() for node in self.data])


class ScenarioTemplateApplyList(TypeApplyList[ScenarioTemplateApply]):
    _NODE = ScenarioTemplateApply
