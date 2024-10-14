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
from ._plant import Plant, PlantWrite

if TYPE_CHECKING:
    from ._generator import Generator, GeneratorGraphQL, GeneratorWrite


__all__ = [
    "PlantWaterValueBased",
    "PlantWaterValueBasedWrite",
    "PlantWaterValueBasedApply",
    "PlantWaterValueBasedList",
    "PlantWaterValueBasedWriteList",
    "PlantWaterValueBasedApplyList",
    "PlantWaterValueBasedFields",
    "PlantWaterValueBasedTextFields",
    "PlantWaterValueBasedGraphQL",
]


PlantWaterValueBasedTextFields = Literal["name", "display_name", "asset_type", "production_max_time_series", "production_min_time_series", "water_value_time_series", "feeding_fee_time_series", "outlet_level_time_series", "inlet_level_time_series", "head_direct_time_series"]
PlantWaterValueBasedFields = Literal["name", "display_name", "ordering", "asset_type", "head_loss_factor", "outlet_level", "production_max", "production_min", "penstock_head_loss_factors", "connection_losses", "production_max_time_series", "production_min_time_series", "water_value_time_series", "feeding_fee_time_series", "outlet_level_time_series", "inlet_level_time_series", "head_direct_time_series"]

_PLANTWATERVALUEBASED_PROPERTIES_BY_FIELD = {
    "name": "name",
    "display_name": "displayName",
    "ordering": "ordering",
    "asset_type": "assetType",
    "head_loss_factor": "headLossFactor",
    "outlet_level": "outletLevel",
    "production_max": "productionMax",
    "production_min": "productionMin",
    "penstock_head_loss_factors": "penstockHeadLossFactors",
    "connection_losses": "connectionLosses",
    "production_max_time_series": "productionMaxTimeSeries",
    "production_min_time_series": "productionMinTimeSeries",
    "water_value_time_series": "waterValueTimeSeries",
    "feeding_fee_time_series": "feedingFeeTimeSeries",
    "outlet_level_time_series": "outletLevelTimeSeries",
    "inlet_level_time_series": "inletLevelTimeSeries",
    "head_direct_time_series": "headDirectTimeSeries",
}

class PlantWaterValueBasedGraphQL(GraphQLCore):
    """This represents the reading version of plant water value based, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the plant water value based.
        data_record: The data record of the plant water value based node.
        name: Name for the PowerAsset
        display_name: Display name for the PowerAsset.
        ordering: The ordering of the asset
        asset_type: The type of the asset
        head_loss_factor: The head loss factor field.
        outlet_level: The outlet level field.
        production_max: The production max field.
        production_min: The production min field.
        penstock_head_loss_factors: The penstock head loss factor field.
        connection_losses: The connection loss field.
        production_max_time_series: The production max time series field.
        production_min_time_series: The production min time series field.
        water_value_time_series: The water value time series field.
        feeding_fee_time_series: The feeding fee time series field.
        outlet_level_time_series: The outlet level time series field.
        inlet_level_time_series: The inlet level time series field.
        head_direct_time_series: The head direct time series field.
        generators: The generator field.
    """
    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PlantWaterValueBased", "1")
    name: Optional[str] = None
    display_name: Optional[str] = Field(None, alias="displayName")
    ordering: Optional[int] = None
    asset_type: Optional[str] = Field(None, alias="assetType")
    head_loss_factor: Optional[float] = Field(None, alias="headLossFactor")
    outlet_level: Optional[float] = Field(None, alias="outletLevel")
    production_max: Optional[float] = Field(None, alias="productionMax")
    production_min: Optional[float] = Field(None, alias="productionMin")
    penstock_head_loss_factors: Optional[list[float]] = Field(None, alias="penstockHeadLossFactors")
    connection_losses: Optional[float] = Field(None, alias="connectionLosses")
    production_max_time_series: Union[TimeSeries, dict, None] = Field(None, alias="productionMaxTimeSeries")
    production_min_time_series: Union[TimeSeries, dict, None] = Field(None, alias="productionMinTimeSeries")
    water_value_time_series: Union[TimeSeries, dict, None] = Field(None, alias="waterValueTimeSeries")
    feeding_fee_time_series: Union[TimeSeries, dict, None] = Field(None, alias="feedingFeeTimeSeries")
    outlet_level_time_series: Union[TimeSeries, dict, None] = Field(None, alias="outletLevelTimeSeries")
    inlet_level_time_series: Union[TimeSeries, dict, None] = Field(None, alias="inletLevelTimeSeries")
    head_direct_time_series: Union[TimeSeries, dict, None] = Field(None, alias="headDirectTimeSeries")
    generators: Optional[list[GeneratorGraphQL]] = Field(default=None, repr=False)

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
    @field_validator("generators", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> PlantWaterValueBased:
        """Convert this GraphQL format of plant water value based to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return PlantWaterValueBased(
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
            head_loss_factor=self.head_loss_factor,
            outlet_level=self.outlet_level,
            production_max=self.production_max,
            production_min=self.production_min,
            penstock_head_loss_factors=self.penstock_head_loss_factors,
            connection_losses=self.connection_losses,
            production_max_time_series=self.production_max_time_series,
            production_min_time_series=self.production_min_time_series,
            water_value_time_series=self.water_value_time_series,
            feeding_fee_time_series=self.feeding_fee_time_series,
            outlet_level_time_series=self.outlet_level_time_series,
            inlet_level_time_series=self.inlet_level_time_series,
            head_direct_time_series=self.head_direct_time_series,
            generators=[generator.as_read() for generator in self.generators or []],
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> PlantWaterValueBasedWrite:
        """Convert this GraphQL format of plant water value based to the writing format."""
        return PlantWaterValueBasedWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            display_name=self.display_name,
            ordering=self.ordering,
            asset_type=self.asset_type,
            head_loss_factor=self.head_loss_factor,
            outlet_level=self.outlet_level,
            production_max=self.production_max,
            production_min=self.production_min,
            penstock_head_loss_factors=self.penstock_head_loss_factors,
            connection_losses=self.connection_losses,
            production_max_time_series=self.production_max_time_series,
            production_min_time_series=self.production_min_time_series,
            water_value_time_series=self.water_value_time_series,
            feeding_fee_time_series=self.feeding_fee_time_series,
            outlet_level_time_series=self.outlet_level_time_series,
            inlet_level_time_series=self.inlet_level_time_series,
            head_direct_time_series=self.head_direct_time_series,
            generators=[generator.as_write() for generator in self.generators or []],
        )


class PlantWaterValueBased(Plant):
    """This represents the reading version of plant water value based.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the plant water value based.
        data_record: The data record of the plant water value based node.
        name: Name for the PowerAsset
        display_name: Display name for the PowerAsset.
        ordering: The ordering of the asset
        asset_type: The type of the asset
        head_loss_factor: The head loss factor field.
        outlet_level: The outlet level field.
        production_max: The production max field.
        production_min: The production min field.
        penstock_head_loss_factors: The penstock head loss factor field.
        connection_losses: The connection loss field.
        production_max_time_series: The production max time series field.
        production_min_time_series: The production min time series field.
        water_value_time_series: The water value time series field.
        feeding_fee_time_series: The feeding fee time series field.
        outlet_level_time_series: The outlet level time series field.
        inlet_level_time_series: The inlet level time series field.
        head_direct_time_series: The head direct time series field.
        generators: The generator field.
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PlantWaterValueBased", "1")

    node_type: Union[dm.DirectRelationReference, None] = None
    head_loss_factor: Optional[float] = Field(None, alias="headLossFactor")
    outlet_level: Optional[float] = Field(None, alias="outletLevel")
    production_max: Optional[float] = Field(None, alias="productionMax")
    production_min: Optional[float] = Field(None, alias="productionMin")
    penstock_head_loss_factors: Optional[list[float]] = Field(None, alias="penstockHeadLossFactors")
    connection_losses: Optional[float] = Field(None, alias="connectionLosses")
    production_max_time_series: Union[TimeSeries, str, None] = Field(None, alias="productionMaxTimeSeries")
    production_min_time_series: Union[TimeSeries, str, None] = Field(None, alias="productionMinTimeSeries")
    water_value_time_series: Union[TimeSeries, str, None] = Field(None, alias="waterValueTimeSeries")
    feeding_fee_time_series: Union[TimeSeries, str, None] = Field(None, alias="feedingFeeTimeSeries")
    outlet_level_time_series: Union[TimeSeries, str, None] = Field(None, alias="outletLevelTimeSeries")
    inlet_level_time_series: Union[TimeSeries, str, None] = Field(None, alias="inletLevelTimeSeries")
    head_direct_time_series: Union[TimeSeries, str, None] = Field(None, alias="headDirectTimeSeries")
    generators: Optional[list[Union[Generator, str, dm.NodeId]]] = Field(default=None, repr=False)

    def as_write(self) -> PlantWaterValueBasedWrite:
        """Convert this read version of plant water value based to the writing version."""
        return PlantWaterValueBasedWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            display_name=self.display_name,
            ordering=self.ordering,
            asset_type=self.asset_type,
            head_loss_factor=self.head_loss_factor,
            outlet_level=self.outlet_level,
            production_max=self.production_max,
            production_min=self.production_min,
            penstock_head_loss_factors=self.penstock_head_loss_factors,
            connection_losses=self.connection_losses,
            production_max_time_series=self.production_max_time_series,
            production_min_time_series=self.production_min_time_series,
            water_value_time_series=self.water_value_time_series,
            feeding_fee_time_series=self.feeding_fee_time_series,
            outlet_level_time_series=self.outlet_level_time_series,
            inlet_level_time_series=self.inlet_level_time_series,
            head_direct_time_series=self.head_direct_time_series,
            generators=[generator.as_write() if isinstance(generator, DomainModel) else generator for generator in self.generators or []],
        )

    def as_apply(self) -> PlantWaterValueBasedWrite:
        """Convert this read version of plant water value based to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PlantWaterValueBasedWrite(PlantWrite):
    """This represents the writing version of plant water value based.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the plant water value based.
        data_record: The data record of the plant water value based node.
        name: Name for the PowerAsset
        display_name: Display name for the PowerAsset.
        ordering: The ordering of the asset
        asset_type: The type of the asset
        head_loss_factor: The head loss factor field.
        outlet_level: The outlet level field.
        production_max: The production max field.
        production_min: The production min field.
        penstock_head_loss_factors: The penstock head loss factor field.
        connection_losses: The connection loss field.
        production_max_time_series: The production max time series field.
        production_min_time_series: The production min time series field.
        water_value_time_series: The water value time series field.
        feeding_fee_time_series: The feeding fee time series field.
        outlet_level_time_series: The outlet level time series field.
        inlet_level_time_series: The inlet level time series field.
        head_direct_time_series: The head direct time series field.
        generators: The generator field.
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PlantWaterValueBased", "1")

    node_type: Union[dm.DirectRelationReference, None] = None
    head_loss_factor: Optional[float] = Field(None, alias="headLossFactor")
    outlet_level: Optional[float] = Field(None, alias="outletLevel")
    production_max: Optional[float] = Field(None, alias="productionMax")
    production_min: Optional[float] = Field(None, alias="productionMin")
    penstock_head_loss_factors: Optional[list[float]] = Field(None, alias="penstockHeadLossFactors")
    connection_losses: Optional[float] = Field(None, alias="connectionLosses")
    production_max_time_series: Union[TimeSeries, str, None] = Field(None, alias="productionMaxTimeSeries")
    production_min_time_series: Union[TimeSeries, str, None] = Field(None, alias="productionMinTimeSeries")
    water_value_time_series: Union[TimeSeries, str, None] = Field(None, alias="waterValueTimeSeries")
    feeding_fee_time_series: Union[TimeSeries, str, None] = Field(None, alias="feedingFeeTimeSeries")
    outlet_level_time_series: Union[TimeSeries, str, None] = Field(None, alias="outletLevelTimeSeries")
    inlet_level_time_series: Union[TimeSeries, str, None] = Field(None, alias="inletLevelTimeSeries")
    head_direct_time_series: Union[TimeSeries, str, None] = Field(None, alias="headDirectTimeSeries")
    generators: Optional[list[Union[GeneratorWrite, str, dm.NodeId]]] = Field(default=None, repr=False)

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

        if self.head_loss_factor is not None or write_none:
            properties["headLossFactor"] = self.head_loss_factor

        if self.outlet_level is not None or write_none:
            properties["outletLevel"] = self.outlet_level

        if self.production_max is not None or write_none:
            properties["productionMax"] = self.production_max

        if self.production_min is not None or write_none:
            properties["productionMin"] = self.production_min

        if self.penstock_head_loss_factors is not None or write_none:
            properties["penstockHeadLossFactors"] = self.penstock_head_loss_factors

        if self.connection_losses is not None or write_none:
            properties["connectionLosses"] = self.connection_losses

        if self.production_max_time_series is not None or write_none:
            properties["productionMaxTimeSeries"] = self.production_max_time_series if isinstance(self.production_max_time_series, str) or self.production_max_time_series is None else self.production_max_time_series.external_id

        if self.production_min_time_series is not None or write_none:
            properties["productionMinTimeSeries"] = self.production_min_time_series if isinstance(self.production_min_time_series, str) or self.production_min_time_series is None else self.production_min_time_series.external_id

        if self.water_value_time_series is not None or write_none:
            properties["waterValueTimeSeries"] = self.water_value_time_series if isinstance(self.water_value_time_series, str) or self.water_value_time_series is None else self.water_value_time_series.external_id

        if self.feeding_fee_time_series is not None or write_none:
            properties["feedingFeeTimeSeries"] = self.feeding_fee_time_series if isinstance(self.feeding_fee_time_series, str) or self.feeding_fee_time_series is None else self.feeding_fee_time_series.external_id

        if self.outlet_level_time_series is not None or write_none:
            properties["outletLevelTimeSeries"] = self.outlet_level_time_series if isinstance(self.outlet_level_time_series, str) or self.outlet_level_time_series is None else self.outlet_level_time_series.external_id

        if self.inlet_level_time_series is not None or write_none:
            properties["inletLevelTimeSeries"] = self.inlet_level_time_series if isinstance(self.inlet_level_time_series, str) or self.inlet_level_time_series is None else self.inlet_level_time_series.external_id

        if self.head_direct_time_series is not None or write_none:
            properties["headDirectTimeSeries"] = self.head_direct_time_series if isinstance(self.head_direct_time_series, str) or self.head_direct_time_series is None else self.head_direct_time_series.external_id


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
        for generator in self.generators or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=generator,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.production_max_time_series, CogniteTimeSeries):
            resources.time_series.append(self.production_max_time_series)

        if isinstance(self.production_min_time_series, CogniteTimeSeries):
            resources.time_series.append(self.production_min_time_series)

        if isinstance(self.water_value_time_series, CogniteTimeSeries):
            resources.time_series.append(self.water_value_time_series)

        if isinstance(self.feeding_fee_time_series, CogniteTimeSeries):
            resources.time_series.append(self.feeding_fee_time_series)

        if isinstance(self.outlet_level_time_series, CogniteTimeSeries):
            resources.time_series.append(self.outlet_level_time_series)

        if isinstance(self.inlet_level_time_series, CogniteTimeSeries):
            resources.time_series.append(self.inlet_level_time_series)

        if isinstance(self.head_direct_time_series, CogniteTimeSeries):
            resources.time_series.append(self.head_direct_time_series)

        return resources


class PlantWaterValueBasedApply(PlantWaterValueBasedWrite):
    def __new__(cls, *args, **kwargs) -> PlantWaterValueBasedApply:
        warnings.warn(
            "PlantWaterValueBasedApply is deprecated and will be removed in v1.0. Use PlantWaterValueBasedWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "PlantWaterValueBased.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class PlantWaterValueBasedList(DomainModelList[PlantWaterValueBased]):
    """List of plant water value baseds in the read version."""

    _INSTANCE = PlantWaterValueBased

    def as_write(self) -> PlantWaterValueBasedWriteList:
        """Convert these read versions of plant water value based to the writing versions."""
        return PlantWaterValueBasedWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> PlantWaterValueBasedWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PlantWaterValueBasedWriteList(DomainModelWriteList[PlantWaterValueBasedWrite]):
    """List of plant water value baseds in the writing version."""

    _INSTANCE = PlantWaterValueBasedWrite

class PlantWaterValueBasedApplyList(PlantWaterValueBasedWriteList): ...



def _create_plant_water_value_based_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    display_name: str | list[str] | None = None,
    display_name_prefix: str | None = None,
    min_ordering: int | None = None,
    max_ordering: int | None = None,
    asset_type: str | list[str] | None = None,
    asset_type_prefix: str | None = None,
    min_head_loss_factor: float | None = None,
    max_head_loss_factor: float | None = None,
    min_outlet_level: float | None = None,
    max_outlet_level: float | None = None,
    min_production_max: float | None = None,
    max_production_max: float | None = None,
    min_production_min: float | None = None,
    max_production_min: float | None = None,
    min_connection_losses: float | None = None,
    max_connection_losses: float | None = None,
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
    if min_head_loss_factor is not None or max_head_loss_factor is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("headLossFactor"), gte=min_head_loss_factor, lte=max_head_loss_factor))
    if min_outlet_level is not None or max_outlet_level is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("outletLevel"), gte=min_outlet_level, lte=max_outlet_level))
    if min_production_max is not None or max_production_max is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("productionMax"), gte=min_production_max, lte=max_production_max))
    if min_production_min is not None or max_production_min is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("productionMin"), gte=min_production_min, lte=max_production_min))
    if min_connection_losses is not None or max_connection_losses is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("connectionLosses"), gte=min_connection_losses, lte=max_connection_losses))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
