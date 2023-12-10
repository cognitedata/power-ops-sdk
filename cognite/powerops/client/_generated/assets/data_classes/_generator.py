from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DomainModel,
    DomainModelApply,
    DomainModelApplyList,
    DomainModelList,
    DomainRelationApply,
    ResourcesApply,
    TimeSeries,
)

if TYPE_CHECKING:
    from ._generator_efficiency_curve import GeneratorEfficiencyCurve, GeneratorEfficiencyCurveApply
    from ._turbine_efficiency_curve import TurbineEfficiencyCurve, TurbineEfficiencyCurveApply


__all__ = [
    "Generator",
    "GeneratorApply",
    "GeneratorList",
    "GeneratorApplyList",
    "GeneratorFields",
    "GeneratorTextFields",
]


GeneratorTextFields = Literal["name", "display_name", "start_stop_cost", "is_available_time_series"]
GeneratorFields = Literal[
    "name", "display_name", "p_min", "penstock", "start_cost", "start_stop_cost", "is_available_time_series"
]

_GENERATOR_PROPERTIES_BY_FIELD = {
    "name": "name",
    "display_name": "displayName",
    "p_min": "pMin",
    "penstock": "penstock",
    "start_cost": "startCost",
    "start_stop_cost": "startStopCost",
    "is_available_time_series": "isAvailableTimeSeries",
}


class Generator(DomainModel):
    """This represents the reading version of generator.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the generator.
        name: Name for the Generator.
        display_name: Display name for the Generator.
        p_min: The p min field.
        penstock: The penstock field.
        start_cost: The start cost field.
        start_stop_cost: The start stop cost field.
        is_available_time_series: The is available time series field.
        efficiency_curve: The efficiency curve field.
        turbine_curves: The watercourses that are connected to the PriceArea.
        created_time: The created time of the generator node.
        last_updated_time: The last updated time of the generator node.
        deleted_time: If present, the deleted time of the generator node.
        version: The version of the generator node.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    name: Optional[str] = None
    display_name: Optional[str] = Field(None, alias="displayName")
    p_min: Optional[float] = Field(None, alias="pMin")
    penstock: Optional[int] = None
    start_cost: Optional[float] = Field(None, alias="startCost")
    start_stop_cost: Union[TimeSeries, str, None] = Field(None, alias="startStopCost")
    is_available_time_series: Union[TimeSeries, str, None] = Field(None, alias="isAvailableTimeSeries")
    efficiency_curve: Union[GeneratorEfficiencyCurve, str, dm.NodeId, None] = Field(
        None, repr=False, alias="efficiencyCurve"
    )
    turbine_curves: Union[list[TurbineEfficiencyCurve], list[str], None] = Field(
        default=None, repr=False, alias="turbineCurves"
    )

    def as_apply(self) -> GeneratorApply:
        """Convert this read version of generator to the writing version."""
        return GeneratorApply(
            space=self.space,
            external_id=self.external_id,
            name=self.name,
            display_name=self.display_name,
            p_min=self.p_min,
            penstock=self.penstock,
            start_cost=self.start_cost,
            start_stop_cost=self.start_stop_cost,
            is_available_time_series=self.is_available_time_series,
            efficiency_curve=self.efficiency_curve.as_apply()
            if isinstance(self.efficiency_curve, DomainModel)
            else self.efficiency_curve,
            turbine_curves=[
                turbine_curve.as_apply() if isinstance(turbine_curve, DomainModel) else turbine_curve
                for turbine_curve in self.turbine_curves or []
            ],
        )


class GeneratorApply(DomainModelApply):
    """This represents the writing version of generator.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the generator.
        name: Name for the Generator.
        display_name: Display name for the Generator.
        p_min: The p min field.
        penstock: The penstock field.
        start_cost: The start cost field.
        start_stop_cost: The start stop cost field.
        is_available_time_series: The is available time series field.
        efficiency_curve: The efficiency curve field.
        turbine_curves: The watercourses that are connected to the PriceArea.
        existing_version: Fail the ingestion request if the generator version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the edge (for the specified container or instance).
            If existingVersion is set to 0, the upsert will behave as an insert, so it will fail the bulk if the item already exists.
            If skipOnVersionConflict is set on the ingestion request, then the item will be skipped instead of failing the ingestion request.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    name: str
    display_name: Optional[str] = Field(None, alias="displayName")
    p_min: Optional[float] = Field(None, alias="pMin")
    penstock: Optional[int] = None
    start_cost: Optional[float] = Field(None, alias="startCost")
    start_stop_cost: Union[TimeSeries, str, None] = Field(None, alias="startStopCost")
    is_available_time_series: Union[TimeSeries, str, None] = Field(None, alias="isAvailableTimeSeries")
    efficiency_curve: Union[GeneratorEfficiencyCurveApply, str, dm.NodeId, None] = Field(
        None, repr=False, alias="efficiencyCurve"
    )
    turbine_curves: Union[list[TurbineEfficiencyCurveApply], list[str], None] = Field(
        default=None, repr=False, alias="turbineCurves"
    )

    def _to_instances_apply(
        self,
        cache: set[tuple[str, str]],
        view_by_write_class: dict[type[DomainModelApply | DomainRelationApply], dm.ViewId] | None,
    ) -> ResourcesApply:
        resources = ResourcesApply()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_write_class and view_by_write_class.get(type(self))) or dm.ViewId(
            "power-ops-assets", "Generator", "1"
        )

        properties = {}
        if self.name is not None:
            properties["name"] = self.name
        if self.display_name is not None:
            properties["displayName"] = self.display_name
        if self.p_min is not None:
            properties["pMin"] = self.p_min
        if self.penstock is not None:
            properties["penstock"] = self.penstock
        if self.start_cost is not None:
            properties["startCost"] = self.start_cost
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
        if self.efficiency_curve is not None:
            properties["efficiencyCurve"] = {
                "space": self.space if isinstance(self.efficiency_curve, str) else self.efficiency_curve.space,
                "externalId": self.efficiency_curve
                if isinstance(self.efficiency_curve, str)
                else self.efficiency_curve.external_id,
            }

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        edge_type = dm.DirectRelationReference("power-ops-types", "isSubAssetOf")
        for turbine_curve in self.turbine_curves or []:
            other_resources = DomainRelationApply.from_edge_to_resources(
                cache, self, turbine_curve, edge_type, view_by_write_class
            )
            resources.extend(other_resources)

        if isinstance(self.efficiency_curve, DomainModelApply):
            other_resources = self.efficiency_curve._to_instances_apply(cache, view_by_write_class)
            resources.extend(other_resources)

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
    display_name: str | list[str] | None = None,
    display_name_prefix: str | None = None,
    min_p_min: float | None = None,
    max_p_min: float | None = None,
    min_penstock: int | None = None,
    max_penstock: int | None = None,
    min_start_cost: float | None = None,
    max_start_cost: float | None = None,
    efficiency_curve: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if name and isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if display_name and isinstance(display_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("displayName"), value=display_name))
    if display_name and isinstance(display_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("displayName"), values=display_name))
    if display_name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("displayName"), value=display_name_prefix))
    if min_p_min or max_p_min:
        filters.append(dm.filters.Range(view_id.as_property_ref("pMin"), gte=min_p_min, lte=max_p_min))
    if min_penstock or max_penstock:
        filters.append(dm.filters.Range(view_id.as_property_ref("penstock"), gte=min_penstock, lte=max_penstock))
    if min_start_cost or max_start_cost:
        filters.append(dm.filters.Range(view_id.as_property_ref("startCost"), gte=min_start_cost, lte=max_start_cost))
    if efficiency_curve and isinstance(efficiency_curve, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("efficiencyCurve"),
                value={"space": "power-ops-assets", "externalId": efficiency_curve},
            )
        )
    if efficiency_curve and isinstance(efficiency_curve, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("efficiencyCurve"),
                value={"space": efficiency_curve[0], "externalId": efficiency_curve[1]},
            )
        )
    if efficiency_curve and isinstance(efficiency_curve, list) and isinstance(efficiency_curve[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("efficiencyCurve"),
                values=[{"space": "power-ops-assets", "externalId": item} for item in efficiency_curve],
            )
        )
    if efficiency_curve and isinstance(efficiency_curve, list) and isinstance(efficiency_curve[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("efficiencyCurve"),
                values=[{"space": item[0], "externalId": item[1]} for item in efficiency_curve],
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if space and isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
