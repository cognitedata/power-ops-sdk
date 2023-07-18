from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from cognite.powerops.clients.data_classes._core import DomainModel, DomainModelApply, InstancesApply, TypeList

if TYPE_CHECKING:
    from cognite.powerops.clients.data_classes._generators import GeneratorApply
    from cognite.powerops.clients.data_classes._reservoirs import ReservoirApply

__all__ = ["Plant", "PlantApply", "PlantList"]


class Plant(DomainModel):
    space: ClassVar[str] = "power-ops"
    display_name: Optional[str] = Field(None, alias="displayName")
    feeding_fee: Optional[str] = Field(None, alias="feedingFee")
    generators: list[str] = []
    head_direct_time_series: Optional[str] = Field(None, alias="headDirectTimeSeries")
    head_loss_factor: Optional[float] = Field(None, alias="headLossFactor")
    inlet_level: Optional[str] = Field(None, alias="inletLevel")
    inlet_reservoirs: list[str] = Field([], alias="inletReservoirs")
    name: Optional[str] = None
    ordering: Optional[int] = None
    outlet_level: Optional[float] = Field(None, alias="outletLevel")
    outlet_level_time_series: Optional[str] = Field(None, alias="outletLevelTimeSeries")
    p_max: Optional[float] = Field(None, alias="pMax")
    p_max_time_series: Optional[str] = Field(None, alias="pMaxTimeSeries")
    p_min: Optional[float] = Field(None, alias="pMin")
    p_min_time_series: Optional[str] = Field(None, alias="pMinTimeSeries")
    penstock_head_loss_factors: Optional[dict] = Field(None, alias="penstockHeadLossFactors")
    water_value: Optional[str] = Field(None, alias="waterValue")


class PlantApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    display_name: Optional[str] = None
    feeding_fee: Optional[str] = None
    generators: list[Union["GeneratorApply", str]] = Field(default_factory=list, repr=False)
    head_direct_time_series: Optional[str] = None
    head_loss_factor: Optional[float] = None
    inlet_level: Optional[str] = None
    inlet_reservoirs: list[Union["ReservoirApply", str]] = Field(default_factory=list, repr=False)
    name: Optional[str] = None
    ordering: Optional[int] = None
    outlet_level: Optional[float] = None
    outlet_level_time_series: Optional[str] = None
    p_max: Optional[float] = None
    p_max_time_series: Optional[str] = None
    p_min: Optional[float] = None
    p_min_time_series: Optional[str] = None
    penstock_head_loss_factors: Optional[dict] = None
    water_value: Optional[str] = None

    def _to_instances_apply(self, cache: set[str]) -> InstancesApply:
        if self.external_id in cache:
            return InstancesApply([], [])

        sources = []
        source = dm.NodeOrEdgeData(
            source=dm.ContainerId("power-ops", "Plant"),
            properties={
                "displayName": self.display_name,
                "feedingFee": self.feeding_fee,
                "headDirectTimeSeries": self.head_direct_time_series,
                "headLossFactor": self.head_loss_factor,
                "inletLevel": self.inlet_level,
                "name": self.name,
                "ordering": self.ordering,
                "outletLevel": self.outlet_level,
                "outletLevelTimeSeries": self.outlet_level_time_series,
                "pMax": self.p_max,
                "pMaxTimeSeries": self.p_max_time_series,
                "pMin": self.p_min,
                "pMinTimeSeries": self.p_min_time_series,
                "penstockHeadLossFactors": self.penstock_head_loss_factors,
                "waterValue": self.water_value,
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

        for generator in self.generators:
            edge = self._create_generator_edge(generator)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(generator, DomainModelApply):
                instances = generator._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for inlet_reservoir in self.inlet_reservoirs:
            edge = self._create_inlet_reservoir_edge(inlet_reservoir)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(inlet_reservoir, DomainModelApply):
                instances = inlet_reservoir._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        return InstancesApply(nodes, edges)

    def _create_generator_edge(self, generator: Union[str, "GeneratorApply"]) -> dm.EdgeApply:
        if isinstance(generator, str):
            end_node_ext_id = generator
        elif isinstance(generator, DomainModelApply):
            end_node_ext_id = generator.external_id
        else:
            raise TypeError(f"Expected str or GeneratorApply, got {type(generator)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "Plant.generators"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )

    def _create_inlet_reservoir_edge(self, inlet_reservoir: Union[str, "ReservoirApply"]) -> dm.EdgeApply:
        if isinstance(inlet_reservoir, str):
            end_node_ext_id = inlet_reservoir
        elif isinstance(inlet_reservoir, DomainModelApply):
            end_node_ext_id = inlet_reservoir.external_id
        else:
            raise TypeError(f"Expected str or ReservoirApply, got {type(inlet_reservoir)}")

        return dm.EdgeApply(
            space="power-ops",
            external_id=f"{self.external_id}:{end_node_ext_id}",
            type=dm.DirectRelationReference("power-ops", "Plant.inletReservoirs"),
            start_node=dm.DirectRelationReference(self.space, self.external_id),
            end_node=dm.DirectRelationReference("power-ops", end_node_ext_id),
        )


class PlantList(TypeList[Plant]):
    _NODE = Plant
