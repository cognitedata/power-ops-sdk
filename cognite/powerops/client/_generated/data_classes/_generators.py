from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.client._generated.data_classes._core import DomainModel, DomainModelApply, TypeList

__all__ = ["Generator", "GeneratorApply", "GeneratorList"]


class Generator(DomainModel):
    space: ClassVar[str] = "power-ops"
    generator_efficiency_curve: Optional[str] = Field(None, alias="generatorEfficiencyCurve")
    name: Optional[str] = None
    p_min: Optional[float] = Field(None, alias="pMin")
    penstock: Optional[int] = None
    start_stop_cost: Optional[str] = Field(None, alias="startStopCost")
    startcost: Optional[float] = None
    turbine_efficiency_curve: Optional[str] = Field(None, alias="turbineEfficiencyCurve")


class GeneratorApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    generator_efficiency_curve: Optional[str] = None
    name: Optional[str] = None
    p_min: Optional[float] = None
    penstock: Optional[int] = None
    start_stop_cost: Optional[str] = None
    startcost: Optional[float] = None
    turbine_efficiency_curve: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.generator_efficiency_curve is not None:
            properties["generatorEfficiencyCurve"] = self.generator_efficiency_curve
        if self.name is not None:
            properties["name"] = self.name
        if self.p_min is not None:
            properties["pMin"] = self.p_min
        if self.penstock is not None:
            properties["penstock"] = self.penstock
        if self.start_stop_cost is not None:
            properties["startStopCost"] = self.start_stop_cost
        if self.startcost is not None:
            properties["startcost"] = self.startcost
        if self.turbine_efficiency_curve is not None:
            properties["turbineEfficiencyCurve"] = self.turbine_efficiency_curve
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "Generator"),
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

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class GeneratorList(TypeList[Generator]):
    _NODE = Generator
