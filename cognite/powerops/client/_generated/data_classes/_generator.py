from __future__ import annotations

from typing import Literal, Optional

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

__all__ = [
    "Generator",
    "GeneratorApply",
    "GeneratorList",
    "GeneratorApplyList",
    "GeneratorFields",
    "GeneratorTextFields",
]


GeneratorTextFields = Literal[
    "name", "start_stop_cost", "is_available_time_series", "generator_efficiency_curve", "turbine_efficiency_curve"
]
GeneratorFields = Literal[
    "name",
    "p_min",
    "penstock",
    "startcost",
    "start_stop_cost",
    "is_available_time_series",
    "generator_efficiency_curve",
    "turbine_efficiency_curve",
]

_GENERATOR_PROPERTIES_BY_FIELD = {
    "name": "name",
    "p_min": "pMin",
    "penstock": "penstock",
    "startcost": "startcost",
    "start_stop_cost": "startStopCost",
    "is_available_time_series": "isAvailableTimeSeries",
    "generator_efficiency_curve": "generatorEfficiencyCurve",
    "turbine_efficiency_curve": "turbineEfficiencyCurve",
}


class Generator(DomainModel):
    space: str = "power-ops"
    name: Optional[str] = None
    p_min: Optional[float] = Field(None, alias="pMin")
    penstock: Optional[int] = None
    startcost: Optional[float] = None
    start_stop_cost: Optional[str] = Field(None, alias="startStopCost")
    is_available_time_series: Optional[str] = Field(None, alias="isAvailableTimeSeries")
    generator_efficiency_curve: Optional[str] = Field(None, alias="generatorEfficiencyCurve")
    turbine_efficiency_curve: Optional[str] = Field(None, alias="turbineEfficiencyCurve")

    def as_apply(self) -> GeneratorApply:
        return GeneratorApply(
            external_id=self.external_id,
            name=self.name,
            p_min=self.p_min,
            penstock=self.penstock,
            startcost=self.startcost,
            start_stop_cost=self.start_stop_cost,
            is_available_time_series=self.is_available_time_series,
            generator_efficiency_curve=self.generator_efficiency_curve,
            turbine_efficiency_curve=self.turbine_efficiency_curve,
        )


class GeneratorApply(DomainModelApply):
    space: str = "power-ops"
    name: Optional[str] = None
    p_min: Optional[float] = Field(None, alias="pMin")
    penstock: Optional[int] = None
    startcost: Optional[float] = None
    start_stop_cost: Optional[str] = Field(None, alias="startStopCost")
    is_available_time_series: Optional[str] = Field(None, alias="isAvailableTimeSeries")
    generator_efficiency_curve: Optional[str] = Field(None, alias="generatorEfficiencyCurve")
    turbine_efficiency_curve: Optional[str] = Field(None, alias="turbineEfficiencyCurve")

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.p_min is not None:
            properties["pMin"] = self.p_min
        if self.penstock is not None:
            properties["penstock"] = self.penstock
        if self.startcost is not None:
            properties["startcost"] = self.startcost
        if self.start_stop_cost is not None:
            properties["startStopCost"] = self.start_stop_cost
        if self.is_available_time_series is not None:
            properties["isAvailableTimeSeries"] = self.is_available_time_series
        if self.generator_efficiency_curve is not None:
            properties["generatorEfficiencyCurve"] = self.generator_efficiency_curve
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

    def as_apply(self) -> GeneratorApplyList:
        return GeneratorApplyList([node.as_apply() for node in self.data])


class GeneratorApplyList(TypeApplyList[GeneratorApply]):
    _NODE = GeneratorApply
