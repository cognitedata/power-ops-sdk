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
    from cognite.powerops.client.dm_client.data_classes._processing_logs import ProcessingLogApply
    from cognite.powerops.client.dm_client.data_classes._scenarios import ScenarioApply

__all__ = ["Case", "CaseApply", "CaseList"]


class Case(DomainModel):
    space: ClassVar[str] = "cogShop"
    scenario: Optional[str] = None
    start_time: Optional[str] = Field(None, alias="startTime")
    end_time: Optional[str] = Field(None, alias="endTime")
    processing_log: list[str] = Field([], alias="processingLog")


class CaseApply(CircularModelApply):
    space: ClassVar[str] = "cogShop"
    scenario: Optional[Union[str, "ScenarioApply"]] = None
    start_time: str
    end_time: str
    processing_log: list[Union[str, "ProcessingLogApply"]] = []

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        this_node = dm.NodeApply(
            space=self.space,
            external_id=self.external_id,
            existing_version=self.existing_version,
            sources=[
                dm.NodeOrEdgeData(
                    source=dm.ContainerId("cogShop", "Case"),
                    properties={
                        "startTime": self.start_time,
                        "endTime": self.end_time,
                    },
                ),
            ],
        )
        nodes = [this_node]
        edges = []

        if self.scenario is not None:
            edge = self._create_scenario_edge(self.scenario)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(self.scenario, CircularModelApply):
                instances = self.scenario._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for processing_log in self.processing_log:
            edge = self._create_processing_log_edge(processing_log)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(processing_log, CircularModelApply):
                instances = processing_log._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return InstancesApply(nodes, edges)

    def _create_scenario_edge(self, scenario: Union[str, "ScenarioApply"]) -> dm.EdgeApply:
        if isinstance(scenario, str):
            end_node_ext_id = scenario
        elif isinstance(scenario, CircularModelApply):
            end_node_ext_id = scenario.external_id
        else:
            raise TypeError(f"Expected str or ScenarioApply, got {type(scenario)}")

        return dm.EdgeApply(
            space="cogShop",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("cogShop", "Case.scenario"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("cogShop", end_node_ext_id),
        )

    def _create_processing_log_edge(self, processing_log: Union[str, "ProcessingLogApply"]) -> dm.EdgeApply:
        if isinstance(processing_log, str):
            end_node_ext_id = processing_log
        elif isinstance(processing_log, CircularModelApply):
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
