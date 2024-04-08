from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from cognite.client.data_classes import TimeSeries as CogniteTimeSeries
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    ResourcesWrite,
    TimeSeries,
)
from ._power_asset import PowerAsset, PowerAssetWrite

if TYPE_CHECKING:
    from ._generator import Generator, GeneratorWrite


__all__ = [
    "Plant",
    "PlantWrite",
    "PlantApply",
    "PlantList",
    "PlantWriteList",
    "PlantApplyList",
    "PlantFields",
    "PlantTextFields",
]


PlantTextFields = Literal[
    "name",
    "display_name",
    "asset_type",
    "production_max_time_series",
    "production_min_time_series",
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
    "asset_type",
    "head_loss_factor",
    "outlet_level",
    "production_max",
    "production_min",
    "penstock_head_loss_factors",
    "connection_losses",
    "production_max_time_series",
    "production_min_time_series",
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


class Plant(PowerAsset):
    """This represents the reading version of plant.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the plant.
        data_record: The data record of the plant node.
        name: Name for the Asset
        display_name: Display name for the Asset.
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

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("sp_powerops_types_temp", "Plant")
    head_loss_factor: Optional[float] = Field(None, alias="headLossFactor")
    outlet_level: Optional[float] = Field(None, alias="outletLevel")
    production_max: Optional[float] = Field(None, alias="productionMax")
    production_min: Optional[float] = Field(None, alias="productionMin")
    penstock_head_loss_factors: Optional[dict] = Field(None, alias="penstockHeadLossFactors")
    connection_losses: Optional[float] = Field(None, alias="connectionLosses")
    production_max_time_series: Union[TimeSeries, str, None] = Field(None, alias="productionMaxTimeSeries")
    production_min_time_series: Union[TimeSeries, str, None] = Field(None, alias="productionMinTimeSeries")
    water_value_time_series: Union[TimeSeries, str, None] = Field(None, alias="waterValueTimeSeries")
    feeding_fee_time_series: Union[TimeSeries, str, None] = Field(None, alias="feedingFeeTimeSeries")
    outlet_level_time_series: Union[TimeSeries, str, None] = Field(None, alias="outletLevelTimeSeries")
    inlet_level_time_series: Union[TimeSeries, str, None] = Field(None, alias="inletLevelTimeSeries")
    head_direct_time_series: Union[TimeSeries, str, None] = Field(None, alias="headDirectTimeSeries")
    generators: Union[list[Generator], list[str], None] = Field(default=None, repr=False)

    def as_write(self) -> PlantWrite:
        """Convert this read version of plant to the writing version."""
        return PlantWrite(
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
            generators=[
                generator.as_write() if isinstance(generator, DomainModel) else generator
                for generator in self.generators or []
            ],
        )

    def as_apply(self) -> PlantWrite:
        """Convert this read version of plant to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PlantWrite(PowerAssetWrite):
    """This represents the writing version of plant.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the plant.
        data_record: The data record of the plant node.
        name: Name for the Asset
        display_name: Display name for the Asset.
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

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("sp_powerops_types_temp", "Plant")
    head_loss_factor: Optional[float] = Field(None, alias="headLossFactor")
    outlet_level: Optional[float] = Field(None, alias="outletLevel")
    production_max: Optional[float] = Field(None, alias="productionMax")
    production_min: Optional[float] = Field(None, alias="productionMin")
    penstock_head_loss_factors: Optional[dict] = Field(None, alias="penstockHeadLossFactors")
    connection_losses: Optional[float] = Field(None, alias="connectionLosses")
    production_max_time_series: Union[TimeSeries, str, None] = Field(None, alias="productionMaxTimeSeries")
    production_min_time_series: Union[TimeSeries, str, None] = Field(None, alias="productionMinTimeSeries")
    water_value_time_series: Union[TimeSeries, str, None] = Field(None, alias="waterValueTimeSeries")
    feeding_fee_time_series: Union[TimeSeries, str, None] = Field(None, alias="feedingFeeTimeSeries")
    outlet_level_time_series: Union[TimeSeries, str, None] = Field(None, alias="outletLevelTimeSeries")
    inlet_level_time_series: Union[TimeSeries, str, None] = Field(None, alias="inletLevelTimeSeries")
    head_direct_time_series: Union[TimeSeries, str, None] = Field(None, alias="headDirectTimeSeries")
    generators: Union[list[GeneratorWrite], list[str], None] = Field(default=None, repr=False)

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(Plant, dm.ViewId("sp_powerops_models_temp", "Plant", "1"))

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
            if isinstance(self.production_max_time_series, str) or self.production_max_time_series is None:
                properties["productionMaxTimeSeries"] = self.production_max_time_series
            else:
                properties["productionMaxTimeSeries"] = self.production_max_time_series.external_id

        if self.production_min_time_series is not None or write_none:
            if isinstance(self.production_min_time_series, str) or self.production_min_time_series is None:
                properties["productionMinTimeSeries"] = self.production_min_time_series
            else:
                properties["productionMinTimeSeries"] = self.production_min_time_series.external_id

        if self.water_value_time_series is not None or write_none:
            if isinstance(self.water_value_time_series, str) or self.water_value_time_series is None:
                properties["waterValueTimeSeries"] = self.water_value_time_series
            else:
                properties["waterValueTimeSeries"] = self.water_value_time_series.external_id

        if self.feeding_fee_time_series is not None or write_none:
            if isinstance(self.feeding_fee_time_series, str) or self.feeding_fee_time_series is None:
                properties["feedingFeeTimeSeries"] = self.feeding_fee_time_series
            else:
                properties["feedingFeeTimeSeries"] = self.feeding_fee_time_series.external_id

        if self.outlet_level_time_series is not None or write_none:
            if isinstance(self.outlet_level_time_series, str) or self.outlet_level_time_series is None:
                properties["outletLevelTimeSeries"] = self.outlet_level_time_series
            else:
                properties["outletLevelTimeSeries"] = self.outlet_level_time_series.external_id

        if self.inlet_level_time_series is not None or write_none:
            if isinstance(self.inlet_level_time_series, str) or self.inlet_level_time_series is None:
                properties["inletLevelTimeSeries"] = self.inlet_level_time_series
            else:
                properties["inletLevelTimeSeries"] = self.inlet_level_time_series.external_id

        if self.head_direct_time_series is not None or write_none:
            if isinstance(self.head_direct_time_series, str) or self.head_direct_time_series is None:
                properties["headDirectTimeSeries"] = self.head_direct_time_series
            else:
                properties["headDirectTimeSeries"] = self.head_direct_time_series.external_id

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.data_record.existing_version,
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

        edge_type = dm.DirectRelationReference("sp_powerops_types_temp", "isSubAssetOf")
        for generator in self.generators or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache, start_node=self, end_node=generator, edge_type=edge_type, view_by_read_class=view_by_read_class
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


class PlantApply(PlantWrite):
    def __new__(cls, *args, **kwargs) -> PlantApply:
        warnings.warn(
            "PlantApply is deprecated and will be removed in v1.0. Use PlantWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Plant.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class PlantList(DomainModelList[Plant]):
    """List of plants in the read version."""

    _INSTANCE = Plant

    def as_write(self) -> PlantWriteList:
        """Convert these read versions of plant to the writing versions."""
        return PlantWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> PlantWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PlantWriteList(DomainModelWriteList[PlantWrite]):
    """List of plants in the writing version."""

    _INSTANCE = PlantWrite


class PlantApplyList(PlantWriteList): ...


def _create_plant_filter(
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
    if min_ordering is not None or max_ordering is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("ordering"), gte=min_ordering, lte=max_ordering))
    if isinstance(asset_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("assetType"), value=asset_type))
    if asset_type and isinstance(asset_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("assetType"), values=asset_type))
    if asset_type_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("assetType"), value=asset_type_prefix))
    if min_head_loss_factor is not None or max_head_loss_factor is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("headLossFactor"), gte=min_head_loss_factor, lte=max_head_loss_factor
            )
        )
    if min_outlet_level is not None or max_outlet_level is not None:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("outletLevel"), gte=min_outlet_level, lte=max_outlet_level)
        )
    if min_production_max is not None or max_production_max is not None:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("productionMax"), gte=min_production_max, lte=max_production_max)
        )
    if min_production_min is not None or max_production_min is not None:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("productionMin"), gte=min_production_min, lte=max_production_min)
        )
    if min_connection_losses is not None or max_connection_losses is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("connectionLosses"), gte=min_connection_losses, lte=max_connection_losses
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
