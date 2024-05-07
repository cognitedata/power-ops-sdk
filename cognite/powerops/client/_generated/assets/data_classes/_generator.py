from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
from pydantic import Field
from pydantic import field_validator, model_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    TimeSeries,
)

if TYPE_CHECKING:
    from ._generator_efficiency_curve import (
        GeneratorEfficiencyCurve,
        GeneratorEfficiencyCurveGraphQL,
        GeneratorEfficiencyCurveWrite,
    )
    from ._turbine_efficiency_curve import (
        TurbineEfficiencyCurve,
        TurbineEfficiencyCurveGraphQL,
        TurbineEfficiencyCurveWrite,
    )


__all__ = [
    "Generator",
    "GeneratorWrite",
    "GeneratorApply",
    "GeneratorList",
    "GeneratorWriteList",
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


class GeneratorGraphQL(GraphQLCore):
    """This represents the reading version of generator, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the generator.
        data_record: The data record of the generator node.
        name: Name for the Generator.
        display_name: Display name for the Generator.
        p_min: The p min field.
        penstock: The penstock field.
        start_cost: The start cost field.
        start_stop_cost: The start stop cost field.
        is_available_time_series: The is available time series field.
        efficiency_curve: The efficiency curve field.
        turbine_curves: The watercourses that are connected to the PriceArea.
    """

    view_id = dm.ViewId("power-ops-assets", "Generator", "1")
    name: Optional[str] = None
    display_name: Optional[str] = Field(None, alias="displayName")
    p_min: Optional[float] = Field(None, alias="pMin")
    penstock: Optional[int] = None
    start_cost: Optional[float] = Field(None, alias="startCost")
    start_stop_cost: Union[TimeSeries, dict, None] = Field(None, alias="startStopCost")
    is_available_time_series: Union[TimeSeries, dict, None] = Field(None, alias="isAvailableTimeSeries")
    efficiency_curve: Optional[GeneratorEfficiencyCurveGraphQL] = Field(None, repr=False, alias="efficiencyCurve")
    turbine_curves: Optional[list[TurbineEfficiencyCurveGraphQL]] = Field(
        default=None, repr=False, alias="turbineCurves"
    )

    @model_validator(mode="before")
    def parse_data_record(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        if "lastUpdatedTime" in values or "createdTime" in values:
            values["dataRecord"] = DataRecordGraphQL(
                created_time=values.pop("createdTime", None),
                last_updated_time=values.pop("lastUpdatedTime", None),
            )
        return values

    @field_validator("efficiency_curve", "turbine_curves", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> Generator:
        """Convert this GraphQL format of generator to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return Generator(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            display_name=self.display_name,
            p_min=self.p_min,
            penstock=self.penstock,
            start_cost=self.start_cost,
            start_stop_cost=self.start_stop_cost,
            is_available_time_series=self.is_available_time_series,
            efficiency_curve=(
                self.efficiency_curve.as_read()
                if isinstance(self.efficiency_curve, GraphQLCore)
                else self.efficiency_curve
            ),
            turbine_curves=[
                turbine_curve.as_read() if isinstance(turbine_curve, GraphQLCore) else turbine_curve
                for turbine_curve in self.turbine_curves or []
            ],
        )

    def as_write(self) -> GeneratorWrite:
        """Convert this GraphQL format of generator to the writing format."""
        return GeneratorWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            display_name=self.display_name,
            p_min=self.p_min,
            penstock=self.penstock,
            start_cost=self.start_cost,
            start_stop_cost=self.start_stop_cost,
            is_available_time_series=self.is_available_time_series,
            efficiency_curve=(
                self.efficiency_curve.as_write()
                if isinstance(self.efficiency_curve, DomainModel)
                else self.efficiency_curve
            ),
            turbine_curves=[
                turbine_curve.as_write() if isinstance(turbine_curve, DomainModel) else turbine_curve
                for turbine_curve in self.turbine_curves or []
            ],
        )


class Generator(DomainModel):
    """This represents the reading version of generator.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the generator.
        data_record: The data record of the generator node.
        name: Name for the Generator.
        display_name: Display name for the Generator.
        p_min: The p min field.
        penstock: The penstock field.
        start_cost: The start cost field.
        start_stop_cost: The start stop cost field.
        is_available_time_series: The is available time series field.
        efficiency_curve: The efficiency curve field.
        turbine_curves: The watercourses that are connected to the PriceArea.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power-ops-types", "Generator")
    name: str
    display_name: Optional[str] = Field(None, alias="displayName")
    p_min: Optional[float] = Field(None, alias="pMin")
    penstock: Optional[int] = None
    start_cost: Optional[float] = Field(None, alias="startCost")
    start_stop_cost: Union[TimeSeries, str, None] = Field(None, alias="startStopCost")
    is_available_time_series: Union[TimeSeries, str, None] = Field(None, alias="isAvailableTimeSeries")
    efficiency_curve: Union[GeneratorEfficiencyCurve, str, dm.NodeId, None] = Field(
        None, repr=False, alias="efficiencyCurve"
    )
    turbine_curves: Union[list[TurbineEfficiencyCurve], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="turbineCurves"
    )

    def as_write(self) -> GeneratorWrite:
        """Convert this read version of generator to the writing version."""
        return GeneratorWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            display_name=self.display_name,
            p_min=self.p_min,
            penstock=self.penstock,
            start_cost=self.start_cost,
            start_stop_cost=self.start_stop_cost,
            is_available_time_series=self.is_available_time_series,
            efficiency_curve=(
                self.efficiency_curve.as_write()
                if isinstance(self.efficiency_curve, DomainModel)
                else self.efficiency_curve
            ),
            turbine_curves=[
                turbine_curve.as_write() if isinstance(turbine_curve, DomainModel) else turbine_curve
                for turbine_curve in self.turbine_curves or []
            ],
        )

    def as_apply(self) -> GeneratorWrite:
        """Convert this read version of generator to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class GeneratorWrite(DomainModelWrite):
    """This represents the writing version of generator.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the generator.
        data_record: The data record of the generator node.
        name: Name for the Generator.
        display_name: Display name for the Generator.
        p_min: The p min field.
        penstock: The penstock field.
        start_cost: The start cost field.
        start_stop_cost: The start stop cost field.
        is_available_time_series: The is available time series field.
        efficiency_curve: The efficiency curve field.
        turbine_curves: The watercourses that are connected to the PriceArea.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power-ops-types", "Generator")
    name: str
    display_name: Optional[str] = Field(None, alias="displayName")
    p_min: Optional[float] = Field(None, alias="pMin")
    penstock: Optional[int] = None
    start_cost: Optional[float] = Field(None, alias="startCost")
    start_stop_cost: Union[TimeSeries, str, None] = Field(None, alias="startStopCost")
    is_available_time_series: Union[TimeSeries, str, None] = Field(None, alias="isAvailableTimeSeries")
    efficiency_curve: Union[GeneratorEfficiencyCurveWrite, str, dm.NodeId, None] = Field(
        None, repr=False, alias="efficiencyCurve"
    )
    turbine_curves: Union[list[TurbineEfficiencyCurveWrite], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="turbineCurves"
    )

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(Generator, dm.ViewId("power-ops-assets", "Generator", "1"))

        properties: dict[str, Any] = {}

        if self.name is not None:
            properties["name"] = self.name

        if self.display_name is not None or write_none:
            properties["displayName"] = self.display_name

        if self.p_min is not None or write_none:
            properties["pMin"] = self.p_min

        if self.penstock is not None or write_none:
            properties["penstock"] = self.penstock

        if self.start_cost is not None or write_none:
            properties["startCost"] = self.start_cost

        if self.start_stop_cost is not None or write_none:
            if isinstance(self.start_stop_cost, str) or self.start_stop_cost is None:
                properties["startStopCost"] = self.start_stop_cost
            else:
                properties["startStopCost"] = self.start_stop_cost.external_id

        if self.is_available_time_series is not None or write_none:
            if isinstance(self.is_available_time_series, str) or self.is_available_time_series is None:
                properties["isAvailableTimeSeries"] = self.is_available_time_series
            else:
                properties["isAvailableTimeSeries"] = self.is_available_time_series.external_id

        if self.efficiency_curve is not None:
            properties["efficiencyCurve"] = {
                "space": self.space if isinstance(self.efficiency_curve, str) else self.efficiency_curve.space,
                "externalId": (
                    self.efficiency_curve
                    if isinstance(self.efficiency_curve, str)
                    else self.efficiency_curve.external_id
                ),
            }

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
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

        edge_type = dm.DirectRelationReference("power-ops-types", "isSubAssetOf")
        for turbine_curve in self.turbine_curves or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=turbine_curve,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.efficiency_curve, DomainModelWrite):
            other_resources = self.efficiency_curve._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.start_stop_cost, CogniteTimeSeries):
            resources.time_series.append(self.start_stop_cost)

        if isinstance(self.is_available_time_series, CogniteTimeSeries):
            resources.time_series.append(self.is_available_time_series)

        return resources


class GeneratorApply(GeneratorWrite):
    def __new__(cls, *args, **kwargs) -> GeneratorApply:
        warnings.warn(
            "GeneratorApply is deprecated and will be removed in v1.0. Use GeneratorWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Generator.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class GeneratorList(DomainModelList[Generator]):
    """List of generators in the read version."""

    _INSTANCE = Generator

    def as_write(self) -> GeneratorWriteList:
        """Convert these read versions of generator to the writing versions."""
        return GeneratorWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> GeneratorWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class GeneratorWriteList(DomainModelWriteList[GeneratorWrite]):
    """List of generators in the writing version."""

    _INSTANCE = GeneratorWrite


class GeneratorApplyList(GeneratorWriteList): ...


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
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if isinstance(display_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("displayName"), value=display_name))
    if display_name and isinstance(display_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("displayName"), values=display_name))
    if display_name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("displayName"), value=display_name_prefix))
    if min_p_min is not None or max_p_min is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("pMin"), gte=min_p_min, lte=max_p_min))
    if min_penstock is not None or max_penstock is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("penstock"), gte=min_penstock, lte=max_penstock))
    if min_start_cost is not None or max_start_cost is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("startCost"), gte=min_start_cost, lte=max_start_cost))
    if efficiency_curve and isinstance(efficiency_curve, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("efficiencyCurve"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": efficiency_curve},
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
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in efficiency_curve],
            )
        )
    if efficiency_curve and isinstance(efficiency_curve, list) and isinstance(efficiency_curve[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("efficiencyCurve"),
                values=[{"space": item[0], "externalId": item[1]} for item in efficiency_curve],
            )
        )
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
