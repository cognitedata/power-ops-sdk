from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._generator import GeneratorApply
    from ._reservoir import ReservoirApply
    from ._watercourse import WatercourseApply

__all__ = ["Plant", "PlantApply", "PlantList", "PlantApplyList", "PlantFields", "PlantTextFields"]


PlantTextFields = Literal[
    "name",
    "display_name",
    "p_max_time_series",
    "p_min_time_series",
    "water_value_time_series",
    "feeding_fee_time_series",
    "outlet_level_time_series",
    "inlet_level_time_series",
    "head_direct_time_series",
]
PlantFields = Literal[
    "name",
    "display_name",
    "ordering",
    "head_loss_factor",
    "outlet_level",
    "p_max",
    "p_min",
    "penstock_head_loss_factors",
    "connection_losses",
    "p_max_time_series",
    "p_min_time_series",
    "water_value_time_series",
    "feeding_fee_time_series",
    "outlet_level_time_series",
    "inlet_level_time_series",
    "head_direct_time_series",
]

_PLANT_PROPERTIES_BY_FIELD = {
    "name": "name",
    "display_name": "displayName",
    "ordering": "ordering",
    "head_loss_factor": "headLossFactor",
    "outlet_level": "outletLevel",
    "p_max": "pMax",
    "p_min": "pMin",
    "penstock_head_loss_factors": "penstockHeadLossFactors",
    "connection_losses": "connectionLosses",
    "p_max_time_series": "pMaxTimeSeries",
    "p_min_time_series": "pMinTimeSeries",
    "water_value_time_series": "waterValueTimeSeries",
    "feeding_fee_time_series": "feedingFeeTimeSeries",
    "outlet_level_time_series": "outletLevelTimeSeries",
    "inlet_level_time_series": "inletLevelTimeSeries",
    "head_direct_time_series": "headDirectTimeSeries",
}


class Plant(DomainModel):
    space: str = "power-ops"
    name: Optional[str] = None
    display_name: Optional[str] = Field(None, alias="displayName")
    ordering: Optional[int] = None
    head_loss_factor: Optional[float] = Field(None, alias="headLossFactor")
    outlet_level: Optional[float] = Field(None, alias="outletLevel")
    p_max: Optional[float] = Field(None, alias="pMax")
    p_min: Optional[float] = Field(None, alias="pMin")
    penstock_head_loss_factors: Optional[dict] = Field(None, alias="penstockHeadLossFactors")
    watercourse: Optional[str] = None
    connection_losses: Optional[float] = Field(None, alias="connectionLosses")
    p_max_time_series: Optional[str] = Field(None, alias="pMaxTimeSeries")
    p_min_time_series: Optional[str] = Field(None, alias="pMinTimeSeries")
    water_value_time_series: Optional[str] = Field(None, alias="waterValueTimeSeries")
    feeding_fee_time_series: Optional[str] = Field(None, alias="feedingFeeTimeSeries")
    outlet_level_time_series: Optional[str] = Field(None, alias="outletLevelTimeSeries")
    inlet_level_time_series: Optional[str] = Field(None, alias="inletLevelTimeSeries")
    head_direct_time_series: Optional[str] = Field(None, alias="headDirectTimeSeries")
    inlet_reservoir: Optional[str] = Field(None, alias="inletReservoir")
    generators: Optional[list[str]] = None

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
            connection_losses=self.connection_losses,
            p_max_time_series=self.p_max_time_series,
            p_min_time_series=self.p_min_time_series,
            water_value_time_series=self.water_value_time_series,
            feeding_fee_time_series=self.feeding_fee_time_series,
            outlet_level_time_series=self.outlet_level_time_series,
            inlet_level_time_series=self.inlet_level_time_series,
            head_direct_time_series=self.head_direct_time_series,
            inlet_reservoir=self.inlet_reservoir,
            generators=self.generators,
        )


class PlantApply(DomainModelApply):
    space: str = "power-ops"
    name: Optional[str] = None
    display_name: Optional[str] = Field(None, alias="displayName")
    ordering: Optional[int] = None
    head_loss_factor: Optional[float] = Field(None, alias="headLossFactor")
    outlet_level: Optional[float] = Field(None, alias="outletLevel")
    p_max: Optional[float] = Field(None, alias="pMax")
    p_min: Optional[float] = Field(None, alias="pMin")
    penstock_head_loss_factors: Optional[dict] = Field(None, alias="penstockHeadLossFactors")
    watercourse: Union[WatercourseApply, str, None] = Field(None, repr=False)
    connection_losses: Optional[float] = Field(None, alias="connectionLosses")
    p_max_time_series: Optional[str] = Field(None, alias="pMaxTimeSeries")
    p_min_time_series: Optional[str] = Field(None, alias="pMinTimeSeries")
    water_value_time_series: Optional[str] = Field(None, alias="waterValueTimeSeries")
    feeding_fee_time_series: Optional[str] = Field(None, alias="feedingFeeTimeSeries")
    outlet_level_time_series: Optional[str] = Field(None, alias="outletLevelTimeSeries")
    inlet_level_time_series: Optional[str] = Field(None, alias="inletLevelTimeSeries")
    head_direct_time_series: Optional[str] = Field(None, alias="headDirectTimeSeries")
    inlet_reservoir: Union[ReservoirApply, str, None] = Field(None, repr=False, alias="inletReservoir")
    generators: Union[list[GeneratorApply], list[str], None] = Field(default=None, repr=False)

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
        if self.connection_losses is not None:
            properties["connectionLosses"] = self.connection_losses
        if self.p_max_time_series is not None:
            properties["pMaxTimeSeries"] = self.p_max_time_series
        if self.p_min_time_series is not None:
            properties["pMinTimeSeries"] = self.p_min_time_series
        if self.water_value_time_series is not None:
            properties["waterValueTimeSeries"] = self.water_value_time_series
        if self.feeding_fee_time_series is not None:
            properties["feedingFeeTimeSeries"] = self.feeding_fee_time_series
        if self.outlet_level_time_series is not None:
            properties["outletLevelTimeSeries"] = self.outlet_level_time_series
        if self.inlet_level_time_series is not None:
            properties["inletLevelTimeSeries"] = self.inlet_level_time_series
        if self.head_direct_time_series is not None:
            properties["headDirectTimeSeries"] = self.head_direct_time_series
        if self.inlet_reservoir is not None:
            properties["inletReservoir"] = {
                "space": "power-ops",
                "externalId": self.inlet_reservoir
                if isinstance(self.inlet_reservoir, str)
                else self.inlet_reservoir.external_id,
            }
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

        if isinstance(self.watercourse, DomainModelApply):
            instances = self.watercourse._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.inlet_reservoir, DomainModelApply):
            instances = self.inlet_reservoir._to_instances_apply(cache)
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


class PlantList(TypeList[Plant]):
    _NODE = Plant

    def as_apply(self) -> PlantApplyList:
        return PlantApplyList([node.as_apply() for node in self.data])


class PlantApplyList(TypeApplyList[PlantApply]):
    _NODE = PlantApply
