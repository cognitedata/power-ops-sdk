from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, ClassVar, Literal,  no_type_check, Optional, Union

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
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    TimeSeries,
)
from ._power_asset import PowerAsset, PowerAssetWrite

if TYPE_CHECKING:
    from ._generator_efficiency_curve import GeneratorEfficiencyCurve, GeneratorEfficiencyCurveGraphQL, GeneratorEfficiencyCurveWrite
    from ._turbine_efficiency_curve import TurbineEfficiencyCurve, TurbineEfficiencyCurveGraphQL, TurbineEfficiencyCurveWrite


__all__ = [
    "Generator",
    "GeneratorWrite",
    "GeneratorApply",
    "GeneratorList",
    "GeneratorWriteList",
    "GeneratorApplyList",
    "GeneratorFields",
    "GeneratorTextFields",
    "GeneratorGraphQL",
]


GeneratorTextFields = Literal["name", "display_name", "asset_type", "start_stop_cost_time_series", "availability_time_series"]
GeneratorFields = Literal["name", "display_name", "ordering", "asset_type", "production_min", "penstock_number", "start_stop_cost", "start_stop_cost_time_series", "availability_time_series"]

_GENERATOR_PROPERTIES_BY_FIELD = {
    "name": "name",
    "display_name": "displayName",
    "ordering": "ordering",
    "asset_type": "assetType",
    "production_min": "productionMin",
    "penstock_number": "penstockNumber",
    "start_stop_cost": "startStopCost",
    "start_stop_cost_time_series": "startStopCostTimeSeries",
    "availability_time_series": "availabilityTimeSeries",
}

class GeneratorGraphQL(GraphQLCore):
    """This represents the reading version of generator, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the generator.
        data_record: The data record of the generator node.
        name: Name for the PowerAsset
        display_name: Display name for the PowerAsset.
        ordering: The ordering of the asset
        asset_type: The type of the asset
        production_min: The production min field.
        penstock_number: The penstock number field.
        start_stop_cost: The start stop cost field.
        start_stop_cost_time_series: The start stop cost time series field.
        availability_time_series: The availability time series field.
        generator_efficiency_curve: The generator efficiency curve field.
        turbine_efficiency_curves: TODO description
    """
    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "Generator", "1")
    name: Optional[str] = None
    display_name: Optional[str] = Field(None, alias="displayName")
    ordering: Optional[int] = None
    asset_type: Optional[str] = Field(None, alias="assetType")
    production_min: Optional[float] = Field(None, alias="productionMin")
    penstock_number: Optional[int] = Field(None, alias="penstockNumber")
    start_stop_cost: Optional[float] = Field(None, alias="startStopCost")
    start_stop_cost_time_series: Union[TimeSeries, dict, None] = Field(None, alias="startStopCostTimeSeries")
    availability_time_series: Union[TimeSeries, dict, None] = Field(None, alias="availabilityTimeSeries")
    generator_efficiency_curve: Optional[GeneratorEfficiencyCurveGraphQL] = Field(default=None, repr=False, alias="generatorEfficiencyCurve")
    turbine_efficiency_curves: Optional[list[TurbineEfficiencyCurveGraphQL]] = Field(default=None, repr=False, alias="turbineEfficiencyCurves")

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
    @field_validator("generator_efficiency_curve", "turbine_efficiency_curves", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
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
            ordering=self.ordering,
            asset_type=self.asset_type,
            production_min=self.production_min,
            penstock_number=self.penstock_number,
            start_stop_cost=self.start_stop_cost,
            start_stop_cost_time_series=self.start_stop_cost_time_series,
            availability_time_series=self.availability_time_series,
            generator_efficiency_curve=self.generator_efficiency_curve.as_read() if isinstance(self.generator_efficiency_curve, GraphQLCore) else self.generator_efficiency_curve,
            turbine_efficiency_curves=[turbine_efficiency_curve.as_read() for turbine_efficiency_curve in self.turbine_efficiency_curves or []],
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> GeneratorWrite:
        """Convert this GraphQL format of generator to the writing format."""
        return GeneratorWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            display_name=self.display_name,
            ordering=self.ordering,
            asset_type=self.asset_type,
            production_min=self.production_min,
            penstock_number=self.penstock_number,
            start_stop_cost=self.start_stop_cost,
            start_stop_cost_time_series=self.start_stop_cost_time_series,
            availability_time_series=self.availability_time_series,
            generator_efficiency_curve=self.generator_efficiency_curve.as_write() if isinstance(self.generator_efficiency_curve, GraphQLCore) else self.generator_efficiency_curve,
            turbine_efficiency_curves=[turbine_efficiency_curve.as_write() for turbine_efficiency_curve in self.turbine_efficiency_curves or []],
        )


class Generator(PowerAsset):
    """This represents the reading version of generator.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the generator.
        data_record: The data record of the generator node.
        name: Name for the PowerAsset
        display_name: Display name for the PowerAsset.
        ordering: The ordering of the asset
        asset_type: The type of the asset
        production_min: The production min field.
        penstock_number: The penstock number field.
        start_stop_cost: The start stop cost field.
        start_stop_cost_time_series: The start stop cost time series field.
        availability_time_series: The availability time series field.
        generator_efficiency_curve: The generator efficiency curve field.
        turbine_efficiency_curves: TODO description
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "Generator", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "Generator")
    production_min: float = Field(alias="productionMin")
    penstock_number: int = Field(alias="penstockNumber")
    start_stop_cost: float = Field(alias="startStopCost")
    start_stop_cost_time_series: Union[TimeSeries, str, None] = Field(None, alias="startStopCostTimeSeries")
    availability_time_series: Union[TimeSeries, str, None] = Field(None, alias="availabilityTimeSeries")
    generator_efficiency_curve: Union[GeneratorEfficiencyCurve, str, dm.NodeId, None] = Field(default=None, repr=False, alias="generatorEfficiencyCurve")
    turbine_efficiency_curves: Optional[list[Union[TurbineEfficiencyCurve, str, dm.NodeId]]] = Field(default=None, repr=False, alias="turbineEfficiencyCurves")

    def as_write(self) -> GeneratorWrite:
        """Convert this read version of generator to the writing version."""
        return GeneratorWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            display_name=self.display_name,
            ordering=self.ordering,
            asset_type=self.asset_type,
            production_min=self.production_min,
            penstock_number=self.penstock_number,
            start_stop_cost=self.start_stop_cost,
            start_stop_cost_time_series=self.start_stop_cost_time_series,
            availability_time_series=self.availability_time_series,
            generator_efficiency_curve=self.generator_efficiency_curve.as_write() if isinstance(self.generator_efficiency_curve, DomainModel) else self.generator_efficiency_curve,
            turbine_efficiency_curves=[turbine_efficiency_curve.as_write() if isinstance(turbine_efficiency_curve, DomainModel) else turbine_efficiency_curve for turbine_efficiency_curve in self.turbine_efficiency_curves or []],
        )

    def as_apply(self) -> GeneratorWrite:
        """Convert this read version of generator to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class GeneratorWrite(PowerAssetWrite):
    """This represents the writing version of generator.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the generator.
        data_record: The data record of the generator node.
        name: Name for the PowerAsset
        display_name: Display name for the PowerAsset.
        ordering: The ordering of the asset
        asset_type: The type of the asset
        production_min: The production min field.
        penstock_number: The penstock number field.
        start_stop_cost: The start stop cost field.
        start_stop_cost_time_series: The start stop cost time series field.
        availability_time_series: The availability time series field.
        generator_efficiency_curve: The generator efficiency curve field.
        turbine_efficiency_curves: TODO description
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "Generator", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "Generator")
    production_min: float = Field(alias="productionMin")
    penstock_number: int = Field(alias="penstockNumber")
    start_stop_cost: float = Field(alias="startStopCost")
    start_stop_cost_time_series: Union[TimeSeries, str, None] = Field(None, alias="startStopCostTimeSeries")
    availability_time_series: Union[TimeSeries, str, None] = Field(None, alias="availabilityTimeSeries")
    generator_efficiency_curve: Union[GeneratorEfficiencyCurveWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="generatorEfficiencyCurve")
    turbine_efficiency_curves: Optional[list[Union[TurbineEfficiencyCurveWrite, str, dm.NodeId]]] = Field(default=None, repr=False, alias="turbineEfficiencyCurves")

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        properties: dict[str, Any] = {}

        if self.name is not None:
            properties["name"] = self.name

        if self.display_name is not None or write_none:
            properties["displayName"] = self.display_name

        if self.ordering is not None or write_none:
            properties["ordering"] = self.ordering

        if self.asset_type is not None or write_none:
            properties["assetType"] = self.asset_type

        if self.production_min is not None:
            properties["productionMin"] = self.production_min

        if self.penstock_number is not None:
            properties["penstockNumber"] = self.penstock_number

        if self.start_stop_cost is not None:
            properties["startStopCost"] = self.start_stop_cost

        if self.start_stop_cost_time_series is not None or write_none:
            properties["startStopCostTimeSeries"] = self.start_stop_cost_time_series if isinstance(self.start_stop_cost_time_series, str) or self.start_stop_cost_time_series is None else self.start_stop_cost_time_series.external_id

        if self.availability_time_series is not None or write_none:
            properties["availabilityTimeSeries"] = self.availability_time_series if isinstance(self.availability_time_series, str) or self.availability_time_series is None else self.availability_time_series.external_id

        if self.generator_efficiency_curve is not None:
            properties["generatorEfficiencyCurve"] = {
                "space":  self.space if isinstance(self.generator_efficiency_curve, str) else self.generator_efficiency_curve.space,
                "externalId": self.generator_efficiency_curve if isinstance(self.generator_efficiency_curve, str) else self.generator_efficiency_curve.external_id,
            }


        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                )],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())



        edge_type = dm.DirectRelationReference("power_ops_types", "isSubAssetOf")
        for turbine_efficiency_curve in self.turbine_efficiency_curves or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=turbine_efficiency_curve,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.generator_efficiency_curve, DomainModelWrite):
            other_resources = self.generator_efficiency_curve._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.start_stop_cost_time_series, CogniteTimeSeries):
            resources.time_series.append(self.start_stop_cost_time_series)

        if isinstance(self.availability_time_series, CogniteTimeSeries):
            resources.time_series.append(self.availability_time_series)

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
    min_ordering: int | None = None,
    max_ordering: int | None = None,
    asset_type: str | list[str] | None = None,
    asset_type_prefix: str | None = None,
    min_production_min: float | None = None,
    max_production_min: float | None = None,
    min_penstock_number: int | None = None,
    max_penstock_number: int | None = None,
    min_start_stop_cost: float | None = None,
    max_start_stop_cost: float | None = None,
    generator_efficiency_curve: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
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
    if min_ordering is not None or max_ordering is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("ordering"), gte=min_ordering, lte=max_ordering))
    if isinstance(asset_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("assetType"), value=asset_type))
    if asset_type and isinstance(asset_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("assetType"), values=asset_type))
    if asset_type_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("assetType"), value=asset_type_prefix))
    if min_production_min is not None or max_production_min is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("productionMin"), gte=min_production_min, lte=max_production_min))
    if min_penstock_number is not None or max_penstock_number is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("penstockNumber"), gte=min_penstock_number, lte=max_penstock_number))
    if min_start_stop_cost is not None or max_start_stop_cost is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("startStopCost"), gte=min_start_stop_cost, lte=max_start_stop_cost))
    if generator_efficiency_curve and isinstance(generator_efficiency_curve, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("generatorEfficiencyCurve"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": generator_efficiency_curve}))
    if generator_efficiency_curve and isinstance(generator_efficiency_curve, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("generatorEfficiencyCurve"), value={"space": generator_efficiency_curve[0], "externalId": generator_efficiency_curve[1]}))
    if generator_efficiency_curve and isinstance(generator_efficiency_curve, list) and isinstance(generator_efficiency_curve[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("generatorEfficiencyCurve"), values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in generator_efficiency_curve]))
    if generator_efficiency_curve and isinstance(generator_efficiency_curve, list) and isinstance(generator_efficiency_curve[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("generatorEfficiencyCurve"), values=[{"space": item[0], "externalId": item[1]} for item in generator_efficiency_curve]))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
