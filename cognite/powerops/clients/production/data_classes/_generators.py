from __future__ import annotations

from typing import ClassVar, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, InstancesApply, TypeList

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

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "Generator"),
            properties={
                "generatorEfficiencyCurve": self.generator_efficiency_curve,
                "name": self.name,
                "pMin": self.p_min,
                "penstock": self.penstock,
                "startStopCost": self.start_stop_cost,
                "startcost": self.startcost,
                "turbineEfficiencyCurve": self.turbine_efficiency_curve,
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

        return InstancesApply(nodes, edges)


class GeneratorList(TypeList[Generator]):
    _NODE = Generator
