from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.client._generated.cogshop1.data_classes._core import DomainModel, DomainModelApply, TypeList

if TYPE_CHECKING:
    from cognite.powerops.client._generated.cogshop1.data_classes._processing_logs import ProcessingLogApply
    from cognite.powerops.client._generated.cogshop1.data_classes._scenarios import ScenarioApply

__all__ = ["Case", "CaseApply", "CaseList"]


class Case(DomainModel):
    space: ClassVar[str] = "cogShop"
    end_time: Optional[str] = Field(None, alias="endTime")
    processing_log: list[str] = Field([], alias="processingLog")
    scenario: Optional[str] = None
    start_time: Optional[str] = Field(None, alias="startTime")


class CaseApply(DomainModelApply):
    space: ClassVar[str] = "cogShop"
    end_time: str
    processing_log: list[Union[ProcessingLogApply, str]] = Field(default_factory=list, repr=False)
    scenario: Optional[Union[ScenarioApply, str]] = Field(None, repr=False)
    start_time: str

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.end_time is not None:
            properties["endTime"] = self.end_time
        if self.scenario is not None:
            properties["scenario"] = {
                "space": "cogShop",
                "externalId": self.scenario if isinstance(self.scenario, str) else self.scenario.external_id,
            }
        if self.start_time is not None:
            properties["startTime"] = self.start_time
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("cogShop", "Case"),
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

        for processing_log in self.processing_log:
            edge = self._create_processing_log_edge(processing_log)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(processing_log, DomainModelApply):
                instances = processing_log._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        if isinstance(self.scenario, DomainModelApply):
            instances = self.scenario._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_processing_log_edge(self, processing_log: Union[str, ProcessingLogApply]) -> dm.EdgeApply:
        if isinstance(processing_log, str):
            end_node_ext_id = processing_log
        elif isinstance(processing_log, DomainModelApply):
            end_node_ext_id = processing_log.external_id
        else:
            raise TypeError(f"Expected str or ProcessingLogApply, got {type(processing_log)}")

        return dm.EdgeApply(
            space="cogShop",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("cogShop", "Case.processingLog"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("cogShop", end_node_ext_id),
        )


class CaseList(TypeList[Case]):
    _NODE = Case
