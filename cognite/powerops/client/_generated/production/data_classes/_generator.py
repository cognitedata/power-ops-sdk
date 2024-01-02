from __future__ import annotations

from typing import Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelCore,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
    TimeSeries,
)


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
    """This represents the reading version of generator.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the generator.
        name: The name field.
        p_min: The p min field.
        penstock: The penstock field.
        startcost: The startcost field.
        start_stop_cost: The start stop cost field.
        is_available_time_series: The is available time series field.
        generator_efficiency_curve: The generator efficiency curve field.
        turbine_efficiency_curve: The turbine efficiency curve field.
        created_time: The created time of the generator node.
        last_updated_time: The last updated time of the generator node.
        deleted_time: If present, the deleted time of the generator node.
        version: The version of the generator node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    name: Optional[str] = None
    p_min: Optional[float] = Field(None, alias="pMin")
    penstock: Optional[int] = None
    startcost: Optional[float] = None
    start_stop_cost: Union[TimeSeries, str, None] = Field(None, alias="startStopCost")
    is_available_time_series: Union[TimeSeries, str, None] = Field(None, alias="isAvailableTimeSeries")
    generator_efficiency_curve: Union[str, None] = Field(None, alias="generatorEfficiencyCurve")
    turbine_efficiency_curve: Union[str, None] = Field(None, alias="turbineEfficiencyCurve")

    def as_apply(self) -> GeneratorApply:
        """Convert this read version of generator to the writing version."""
        return GeneratorApply(
            space=self.space,
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
    """This represents the writing version of generator.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the generator.
        name: The name field.
        p_min: The p min field.
        penstock: The penstock field.
        startcost: The startcost field.
        start_stop_cost: The start stop cost field.
        is_available_time_series: The is available time series field.
        generator_efficiency_curve: The generator efficiency curve field.
        turbine_efficiency_curve: The turbine efficiency curve field.
        existing_version: Fail the ingestion request if the generator version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    name: Optional[str] = None
    p_min: Optional[float] = Field(None, alias="pMin")
    penstock: Optional[int] = None
    startcost: Optional[float] = None
    start_stop_cost: Union[TimeSeries, str, None] = Field(None, alias="startStopCost")
    is_available_time_series: Union[TimeSeries, str, None] = Field(None, alias="isAvailableTimeSeries")
    generator_efficiency_curve: Union[str, None] = Field(None, alias="generatorEfficiencyCurve")
    turbine_efficiency_curve: Union[str, None] = Field(None, alias="turbineEfficiencyCurve")

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(Generator, dm.ViewId("power-ops", "Generator", "9178931bbaac71"))

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
            properties["startStopCost"] = (
                self.start_stop_cost if isinstance(self.start_stop_cost, str) else self.start_stop_cost.external_id
            )

        if self.is_available_time_series is not None:
            properties["isAvailableTimeSeries"] = (
                self.is_available_time_series
                if isinstance(self.is_available_time_series, str)
                else self.is_available_time_series.external_id
            )

        if self.generator_efficiency_curve is not None:
            properties["generatorEfficiencyCurve"] = self.generator_efficiency_curve

        if self.turbine_efficiency_curve is not None:
            properties["turbineEfficiencyCurve"] = self.turbine_efficiency_curve

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        if isinstance(self.start_stop_cost, CogniteTimeSeries):
            resources.time_series.append(self.start_stop_cost)

        if isinstance(self.is_available_time_series, CogniteTimeSeries):
            resources.time_series.append(self.is_available_time_series)

        return resources


class GeneratorList(DomainModelList[Generator]):
    """List of generators in the read version."""

    _INSTANCE = Generator

    def as_apply(self) -> GeneratorApplyList:
        """Convert these read versions of generator to the writing versions."""
        return GeneratorApplyList([node.as_apply() for node in self.data])


class GeneratorApplyList(DomainModelApplyList[GeneratorApply]):
    """List of generators in the writing version."""

    _INSTANCE = GeneratorApply


def _create_generator_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    min_p_min: float | None = None,
    max_p_min: float | None = None,
    min_penstock: int | None = None,
    max_penstock: int | None = None,
    min_startcost: float | None = None,
    max_startcost: float | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if name is not None and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if min_p_min or max_p_min:
        filters.append(dm.filters.Range(view_id.as_property_ref("pMin"), gte=min_p_min, lte=max_p_min))
    if min_penstock or max_penstock:
        filters.append(dm.filters.Range(view_id.as_property_ref("penstock"), gte=min_penstock, lte=max_penstock))
    if min_startcost or max_startcost:
        filters.append(dm.filters.Range(view_id.as_property_ref("startcost"), gte=min_startcost, lte=max_startcost))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space is not None and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
