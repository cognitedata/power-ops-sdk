from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._processing_log import ProcessingLogApply
    from ._scenario import ScenarioApply

__all__ = ["Case", "CaseApply", "CaseList", "CaseApplyList", "CaseFields", "CaseTextFields"]


CaseTextFields = Literal["start_time", "end_time"]
CaseFields = Literal["start_time", "end_time"]

_CASE_PROPERTIES_BY_FIELD = {
    "start_time": "startTime",
    "end_time": "endTime",
}


class Case(DomainModel):
    space: str = "cogShop"
    scenario: Optional[str] = None
    start_time: Optional[str] = Field(None, alias="startTime")
    end_time: Optional[str] = Field(None, alias="endTime")
    processing_log: Optional[list[str]] = Field(None, alias="processingLog")

    def as_apply(self) -> CaseApply:
        return CaseApply(
            external_id=self.external_id,
            scenario=self.scenario,
            start_time=self.start_time,
            end_time=self.end_time,
            processing_log=self.processing_log,
        )


class CaseApply(DomainModelApply):
    space: str = "cogShop"
    scenario: Union[ScenarioApply, str, None] = Field(None, repr=False)
    start_time: str = Field(alias="startTime")
    end_time: str = Field(alias="endTime")
    processing_log: Union[list[ProcessingLogApply], list[str], None] = Field(
        default=None, repr=False, alias="processingLog"
    )

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.scenario is not None:
            properties["scenario"] = {
                "space": "cogShop",
                "externalId": self.scenario if isinstance(self.scenario, str) else self.scenario.external_id,
            }
        if self.start_time is not None:
            properties["startTime"] = self.start_time
        if self.end_time is not None:
            properties["endTime"] = self.end_time
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

        for processing_log in self.processing_log or []:
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

    def as_apply(self) -> CaseApplyList:
        return CaseApplyList([node.as_apply() for node in self.data])


class CaseApplyList(TypeApplyList[CaseApply]):
    _NODE = CaseApply
