from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._generator import GeneratorApply
    from ._reservoir import ReservoirApply
    from ._watercourse import WatercourseApply

__all__ = ["Plant", "PlantApply", "PlantList", "PlantApplyList"]


class Plant(DomainModel):
    space: ClassVar[str] = "power-ops"
    name: Optional[str] = None
    display_name: Optional[str] = Field(None, alias="displayName")
    ordering: Optional[int] = None
    head_loss_factor: Optional[float] = Field(None, alias="headLossFactor")
    outlet_level: Optional[float] = Field(None, alias="outletLevel")
    p_max: Optional[float] = Field(None, alias="pMax")
    p_min: Optional[float] = Field(None, alias="pMin")
    penstock_head_loss_factors: Optional[dict] = Field(None, alias="penstockHeadLossFactors")
    watercourse: Optional[str] = None
    p_max_time_series: Optional[str] = Field(None, alias="pMaxTimeSeries")
    p_min_time_series: Optional[str] = Field(None, alias="pMinTimeSeries")
    water_value: Optional[str] = Field(None, alias="waterValue")
    feeding_fee: Optional[str] = Field(None, alias="feedingFee")
    outlet_level_time_series: Optional[str] = Field(None, alias="outletLevelTimeSeries")
    inlet_level: Optional[str] = Field(None, alias="inletLevel")
    head_direct_time_series: Optional[str] = Field(None, alias="headDirectTimeSeries")
    generators: Optional[list[str]] = None
    inlet_reservoirs: Optional[list[str]] = Field(None, alias="inletReservoirs")

    def as_apply(self) -> PlantApply:
        return PlantApply(
            external_id=self.external_id,
            name=self.name,
            display_name=self.display_name,
            ordering=self.ordering,
            head_loss_factor=self.head_loss_factor,
            outlet_level=self.outlet_level,
            p_max=self.p_max,
            p_min=self.p_min,
            penstock_head_loss_factors=self.penstock_head_loss_factors,
            watercourse=self.watercourse,
            p_max_time_series=self.p_max_time_series,
            p_min_time_series=self.p_min_time_series,
            water_value=self.water_value,
            feeding_fee=self.feeding_fee,
            outlet_level_time_series=self.outlet_level_time_series,
            inlet_level=self.inlet_level,
            head_direct_time_series=self.head_direct_time_series,
            generators=self.generators,
            inlet_reservoirs=self.inlet_reservoirs,
        )


class PlantApply(DomainModelApply):
    space: ClassVar[str] = "power-ops"
    name: Optional[str] = None
    display_name: Optional[str] = None
    ordering: Optional[int] = None
    head_loss_factor: Optional[float] = None
    outlet_level: Optional[float] = None
    p_max: Optional[float] = None
    p_min: Optional[float] = None
    penstock_head_loss_factors: Optional[dict] = None
    watercourse: Union[WatercourseApply, str, None] = Field(None, repr=False)
    p_max_time_series: Optional[str] = None
    p_min_time_series: Optional[str] = None
    water_value: Optional[str] = None
    feeding_fee: Optional[str] = None
    outlet_level_time_series: Optional[str] = None
    inlet_level: Optional[str] = None
    head_direct_time_series: Optional[str] = None
    generators: Union[list[GeneratorApply], list[str], None] = Field(default=None, repr=False)
    inlet_reservoirs: Union[list[ReservoirApply], list[str], None] = Field(default=None, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.display_name is not None:
            properties["displayName"] = self.display_name
        if self.ordering is not None:
            properties["ordering"] = self.ordering
        if self.head_loss_factor is not None:
            properties["headLossFactor"] = self.head_loss_factor
        if self.outlet_level is not None:
            properties["outletLevel"] = self.outlet_level
        if self.p_max is not None:
            properties["pMax"] = self.p_max
        if self.p_min is not None:
            properties["pMin"] = self.p_min
        if self.penstock_head_loss_factors is not None:
            properties["penstockHeadLossFactors"] = self.penstock_head_loss_factors
        if self.watercourse is not None:
            properties["watercourse"] = {
                "space": "power-ops",
                "externalId": self.watercourse if isinstance(self.watercourse, str) else self.watercourse.external_id,
            }
        if self.p_max_time_series is not None:
            properties["pMaxTimeSeries"] = self.p_max_time_series
        if self.p_min_time_series is not None:
            properties["pMinTimeSeries"] = self.p_min_time_series
        if self.water_value is not None:
            properties["waterValue"] = self.water_value
        if self.feeding_fee is not None:
            properties["feedingFee"] = self.feeding_fee
        if self.outlet_level_time_series is not None:
            properties["outletLevelTimeSeries"] = self.outlet_level_time_series
        if self.inlet_level is not None:
            properties["inletLevel"] = self.inlet_level
        if self.head_direct_time_series is not None:
            properties["headDirectTimeSeries"] = self.head_direct_time_series
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "Plant"),
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

        for generator in self.generators or []:
            edge = self._create_generator_edge(generator)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(generator, DomainModelApply):
                instances = generator._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        for inlet_reservoir in self.inlet_reservoirs or []:
            edge = self._create_inlet_reservoir_edge(inlet_reservoir)
            if edge.external_id not in cache:
                edges.append(edge)
                cache.add(edge.external_id)

            if isinstance(inlet_reservoir, DomainModelApply):
                instances = inlet_reservoir._to_instances_apply(cache)
                nodes.extend(instances.nodes)
                edges.extend(instances.edges)

        if isinstance(self.watercourse, DomainModelApply):
            instances = self.watercourse._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))

    def _create_generator_edge(self, generator: Union[str, GeneratorApply]) -> dm.EdgeApply:
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

    def _create_inlet_reservoir_edge(self, inlet_reservoir: Union[str, ReservoirApply]) -> dm.EdgeApply:
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

    def as_apply(self) -> PlantApplyList:
        return PlantApplyList([node.as_apply() for node in self.data])


class PlantApplyList(TypeApplyList[PlantApply]):
    _NODE = PlantApply
