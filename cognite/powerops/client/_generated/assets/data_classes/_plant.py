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
    from ._generator import Generator, GeneratorGraphQL, GeneratorWrite
    from ._reservoir import Reservoir, ReservoirGraphQL, ReservoirWrite
    from ._watercourse import Watercourse, WatercourseGraphQL, WatercourseWrite


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


class PlantGraphQL(GraphQLCore):
    """This represents the reading version of plant, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the plant.
        data_record: The data record of the plant node.
        name: Name for the Plant.
        display_name: The display name field.
        ordering: The order of this plant
        head_loss_factor: The head loss factor field.
        outlet_level: The outlet level field.
        p_max: The p max field.
        p_min: The p min field.
        penstock_head_loss_factors: The penstock head loss factor field.
        watercourse: The watercourse field.
        connection_losses: The connection loss field.
        p_max_time_series: The p max time series field.
        p_min_time_series: The p min time series field.
        water_value_time_series: The water value time series field.
        feeding_fee_time_series: The feeding fee time series field.
        outlet_level_time_series: The outlet level time series field.
        inlet_level_time_series: The inlet level time series field.
        head_direct_time_series: The head direct time series field.
        inlet_reservoir: The inlet reservoir field.
        generators: The generator field.
    """

    view_id = dm.ViewId("power-ops-assets", "Plant", "1")
    name: Optional[str] = None
    display_name: Optional[str] = Field(None, alias="displayName")
    ordering: Optional[int] = None
    head_loss_factor: Optional[float] = Field(None, alias="headLossFactor")
    outlet_level: Optional[float] = Field(None, alias="outletLevel")
    p_max: Optional[float] = Field(None, alias="pMax")
    p_min: Optional[float] = Field(None, alias="pMin")
    penstock_head_loss_factors: Optional[dict] = Field(None, alias="penstockHeadLossFactors")
    watercourse: Optional[WatercourseGraphQL] = Field(None, repr=False)
    connection_losses: Optional[float] = Field(None, alias="connectionLosses")
    p_max_time_series: Union[TimeSeries, dict, None] = Field(None, alias="pMaxTimeSeries")
    p_min_time_series: Union[TimeSeries, dict, None] = Field(None, alias="pMinTimeSeries")
    water_value_time_series: Union[TimeSeries, dict, None] = Field(None, alias="waterValueTimeSeries")
    feeding_fee_time_series: Union[TimeSeries, dict, None] = Field(None, alias="feedingFeeTimeSeries")
    outlet_level_time_series: Union[TimeSeries, dict, None] = Field(None, alias="outletLevelTimeSeries")
    inlet_level_time_series: Union[TimeSeries, dict, None] = Field(None, alias="inletLevelTimeSeries")
    head_direct_time_series: Union[TimeSeries, dict, None] = Field(None, alias="headDirectTimeSeries")
    inlet_reservoir: Optional[ReservoirGraphQL] = Field(None, repr=False, alias="inletReservoir")
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

    @field_validator("watercourse", "inlet_reservoir", "generators", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> Plant:
        """Convert this GraphQL format of plant to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return Plant(
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
            head_loss_factor=self.head_loss_factor,
            outlet_level=self.outlet_level,
            p_max=self.p_max,
            p_min=self.p_min,
            penstock_head_loss_factors=self.penstock_head_loss_factors,
            watercourse=self.watercourse.as_read() if isinstance(self.watercourse, GraphQLCore) else self.watercourse,
            connection_losses=self.connection_losses,
            p_max_time_series=self.p_max_time_series,
            p_min_time_series=self.p_min_time_series,
            water_value_time_series=self.water_value_time_series,
            feeding_fee_time_series=self.feeding_fee_time_series,
            outlet_level_time_series=self.outlet_level_time_series,
            inlet_level_time_series=self.inlet_level_time_series,
            head_direct_time_series=self.head_direct_time_series,
            inlet_reservoir=(
                self.inlet_reservoir.as_read()
                if isinstance(self.inlet_reservoir, GraphQLCore)
                else self.inlet_reservoir
            ),
            generators=[
                generator.as_read() if isinstance(generator, GraphQLCore) else generator
                for generator in self.generators or []
            ],
        )

    def as_write(self) -> PlantWrite:
        """Convert this GraphQL format of plant to the writing format."""
        return PlantWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            display_name=self.display_name,
            ordering=self.ordering,
            head_loss_factor=self.head_loss_factor,
            outlet_level=self.outlet_level,
            p_max=self.p_max,
            p_min=self.p_min,
            penstock_head_loss_factors=self.penstock_head_loss_factors,
            watercourse=self.watercourse.as_write() if isinstance(self.watercourse, DomainModel) else self.watercourse,
            connection_losses=self.connection_losses,
            p_max_time_series=self.p_max_time_series,
            p_min_time_series=self.p_min_time_series,
            water_value_time_series=self.water_value_time_series,
            feeding_fee_time_series=self.feeding_fee_time_series,
            outlet_level_time_series=self.outlet_level_time_series,
            inlet_level_time_series=self.inlet_level_time_series,
            head_direct_time_series=self.head_direct_time_series,
            inlet_reservoir=(
                self.inlet_reservoir.as_write()
                if isinstance(self.inlet_reservoir, DomainModel)
                else self.inlet_reservoir
            ),
            generators=[
                generator.as_write() if isinstance(generator, DomainModel) else generator
                for generator in self.generators or []
            ],
        )


class Plant(DomainModel):
    """This represents the reading version of plant.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the plant.
        data_record: The data record of the plant node.
        name: Name for the Plant.
        display_name: The display name field.
        ordering: The order of this plant
        head_loss_factor: The head loss factor field.
        outlet_level: The outlet level field.
        p_max: The p max field.
        p_min: The p min field.
        penstock_head_loss_factors: The penstock head loss factor field.
        watercourse: The watercourse field.
        connection_losses: The connection loss field.
        p_max_time_series: The p max time series field.
        p_min_time_series: The p min time series field.
        water_value_time_series: The water value time series field.
        feeding_fee_time_series: The feeding fee time series field.
        outlet_level_time_series: The outlet level time series field.
        inlet_level_time_series: The inlet level time series field.
        head_direct_time_series: The head direct time series field.
        inlet_reservoir: The inlet reservoir field.
        generators: The generator field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power-ops-types", "Plant")
    name: str
    display_name: Optional[str] = Field(None, alias="displayName")
    ordering: Optional[int] = None
    head_loss_factor: Optional[float] = Field(None, alias="headLossFactor")
    outlet_level: Optional[float] = Field(None, alias="outletLevel")
    p_max: Optional[float] = Field(None, alias="pMax")
    p_min: Optional[float] = Field(None, alias="pMin")
    penstock_head_loss_factors: Optional[dict] = Field(None, alias="penstockHeadLossFactors")
    watercourse: Union[Watercourse, str, dm.NodeId, None] = Field(None, repr=False)
    connection_losses: Optional[float] = Field(None, alias="connectionLosses")
    p_max_time_series: Union[TimeSeries, str, None] = Field(None, alias="pMaxTimeSeries")
    p_min_time_series: Union[TimeSeries, str, None] = Field(None, alias="pMinTimeSeries")
    water_value_time_series: Union[TimeSeries, str, None] = Field(None, alias="waterValueTimeSeries")
    feeding_fee_time_series: Union[TimeSeries, str, None] = Field(None, alias="feedingFeeTimeSeries")
    outlet_level_time_series: Union[TimeSeries, str, None] = Field(None, alias="outletLevelTimeSeries")
    inlet_level_time_series: Union[TimeSeries, str, None] = Field(None, alias="inletLevelTimeSeries")
    head_direct_time_series: Union[TimeSeries, str, None] = Field(None, alias="headDirectTimeSeries")
    inlet_reservoir: Union[Reservoir, str, dm.NodeId, None] = Field(None, repr=False, alias="inletReservoir")
    generators: Union[list[Generator], list[str], list[dm.NodeId], None] = Field(default=None, repr=False)

    def as_write(self) -> PlantWrite:
        """Convert this read version of plant to the writing version."""
        return PlantWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            display_name=self.display_name,
            ordering=self.ordering,
            head_loss_factor=self.head_loss_factor,
            outlet_level=self.outlet_level,
            p_max=self.p_max,
            p_min=self.p_min,
            penstock_head_loss_factors=self.penstock_head_loss_factors,
            watercourse=self.watercourse.as_write() if isinstance(self.watercourse, DomainModel) else self.watercourse,
            connection_losses=self.connection_losses,
            p_max_time_series=self.p_max_time_series,
            p_min_time_series=self.p_min_time_series,
            water_value_time_series=self.water_value_time_series,
            feeding_fee_time_series=self.feeding_fee_time_series,
            outlet_level_time_series=self.outlet_level_time_series,
            inlet_level_time_series=self.inlet_level_time_series,
            head_direct_time_series=self.head_direct_time_series,
            inlet_reservoir=(
                self.inlet_reservoir.as_write()
                if isinstance(self.inlet_reservoir, DomainModel)
                else self.inlet_reservoir
            ),
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


class PlantWrite(DomainModelWrite):
    """This represents the writing version of plant.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the plant.
        data_record: The data record of the plant node.
        name: Name for the Plant.
        display_name: The display name field.
        ordering: The order of this plant
        head_loss_factor: The head loss factor field.
        outlet_level: The outlet level field.
        p_max: The p max field.
        p_min: The p min field.
        penstock_head_loss_factors: The penstock head loss factor field.
        watercourse: The watercourse field.
        connection_losses: The connection loss field.
        p_max_time_series: The p max time series field.
        p_min_time_series: The p min time series field.
        water_value_time_series: The water value time series field.
        feeding_fee_time_series: The feeding fee time series field.
        outlet_level_time_series: The outlet level time series field.
        inlet_level_time_series: The inlet level time series field.
        head_direct_time_series: The head direct time series field.
        inlet_reservoir: The inlet reservoir field.
        generators: The generator field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power-ops-types", "Plant")
    name: str
    display_name: Optional[str] = Field(None, alias="displayName")
    ordering: Optional[int] = None
    head_loss_factor: Optional[float] = Field(None, alias="headLossFactor")
    outlet_level: Optional[float] = Field(None, alias="outletLevel")
    p_max: Optional[float] = Field(None, alias="pMax")
    p_min: Optional[float] = Field(None, alias="pMin")
    penstock_head_loss_factors: Optional[dict] = Field(None, alias="penstockHeadLossFactors")
    watercourse: Union[WatercourseWrite, str, dm.NodeId, None] = Field(None, repr=False)
    connection_losses: Optional[float] = Field(None, alias="connectionLosses")
    p_max_time_series: Union[TimeSeries, str, None] = Field(None, alias="pMaxTimeSeries")
    p_min_time_series: Union[TimeSeries, str, None] = Field(None, alias="pMinTimeSeries")
    water_value_time_series: Union[TimeSeries, str, None] = Field(None, alias="waterValueTimeSeries")
    feeding_fee_time_series: Union[TimeSeries, str, None] = Field(None, alias="feedingFeeTimeSeries")
    outlet_level_time_series: Union[TimeSeries, str, None] = Field(None, alias="outletLevelTimeSeries")
    inlet_level_time_series: Union[TimeSeries, str, None] = Field(None, alias="inletLevelTimeSeries")
    head_direct_time_series: Union[TimeSeries, str, None] = Field(None, alias="headDirectTimeSeries")
    inlet_reservoir: Union[ReservoirWrite, str, dm.NodeId, None] = Field(None, repr=False, alias="inletReservoir")
    generators: Union[list[GeneratorWrite], list[str], list[dm.NodeId], None] = Field(default=None, repr=False)

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

        write_view = (view_by_read_class or {}).get(Plant, dm.ViewId("power-ops-assets", "Plant", "1"))

        properties: dict[str, Any] = {}

        if self.name is not None:
            properties["name"] = self.name

        if self.display_name is not None or write_none:
            properties["displayName"] = self.display_name

        if self.ordering is not None or write_none:
            properties["ordering"] = self.ordering

        if self.head_loss_factor is not None or write_none:
            properties["headLossFactor"] = self.head_loss_factor

        if self.outlet_level is not None or write_none:
            properties["outletLevel"] = self.outlet_level

        if self.p_max is not None or write_none:
            properties["pMax"] = self.p_max

        if self.p_min is not None or write_none:
            properties["pMin"] = self.p_min

        if self.penstock_head_loss_factors is not None or write_none:
            properties["penstockHeadLossFactors"] = self.penstock_head_loss_factors

        if self.watercourse is not None:
            properties["watercourse"] = {
                "space": self.space if isinstance(self.watercourse, str) else self.watercourse.space,
                "externalId": self.watercourse if isinstance(self.watercourse, str) else self.watercourse.external_id,
            }

        if self.connection_losses is not None or write_none:
            properties["connectionLosses"] = self.connection_losses

        if self.p_max_time_series is not None or write_none:
            if isinstance(self.p_max_time_series, str) or self.p_max_time_series is None:
                properties["pMaxTimeSeries"] = self.p_max_time_series
            else:
                properties["pMaxTimeSeries"] = self.p_max_time_series.external_id

        if self.p_min_time_series is not None or write_none:
            if isinstance(self.p_min_time_series, str) or self.p_min_time_series is None:
                properties["pMinTimeSeries"] = self.p_min_time_series
            else:
                properties["pMinTimeSeries"] = self.p_min_time_series.external_id

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

        if self.inlet_reservoir is not None:
            properties["inletReservoir"] = {
                "space": self.space if isinstance(self.inlet_reservoir, str) else self.inlet_reservoir.space,
                "externalId": (
                    self.inlet_reservoir if isinstance(self.inlet_reservoir, str) else self.inlet_reservoir.external_id
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
        for generator in self.generators or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=generator,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.watercourse, DomainModelWrite):
            other_resources = self.watercourse._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.inlet_reservoir, DomainModelWrite):
            other_resources = self.inlet_reservoir._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.p_max_time_series, CogniteTimeSeries):
            resources.time_series.append(self.p_max_time_series)

        if isinstance(self.p_min_time_series, CogniteTimeSeries):
            resources.time_series.append(self.p_min_time_series)

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
    min_head_loss_factor: float | None = None,
    max_head_loss_factor: float | None = None,
    min_outlet_level: float | None = None,
    max_outlet_level: float | None = None,
    min_p_max: float | None = None,
    max_p_max: float | None = None,
    min_p_min: float | None = None,
    max_p_min: float | None = None,
    watercourse: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    min_connection_losses: float | None = None,
    max_connection_losses: float | None = None,
    inlet_reservoir: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
    if min_p_max is not None or max_p_max is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("pMax"), gte=min_p_max, lte=max_p_max))
    if min_p_min is not None or max_p_min is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("pMin"), gte=min_p_min, lte=max_p_min))
    if watercourse and isinstance(watercourse, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("watercourse"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": watercourse},
            )
        )
    if watercourse and isinstance(watercourse, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("watercourse"), value={"space": watercourse[0], "externalId": watercourse[1]}
            )
        )
    if watercourse and isinstance(watercourse, list) and isinstance(watercourse[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("watercourse"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in watercourse],
            )
        )
    if watercourse and isinstance(watercourse, list) and isinstance(watercourse[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("watercourse"),
                values=[{"space": item[0], "externalId": item[1]} for item in watercourse],
            )
        )
    if min_connection_losses is not None or max_connection_losses is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("connectionLosses"), gte=min_connection_losses, lte=max_connection_losses
            )
        )
    if inlet_reservoir and isinstance(inlet_reservoir, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("inletReservoir"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": inlet_reservoir},
            )
        )
    if inlet_reservoir and isinstance(inlet_reservoir, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("inletReservoir"),
                value={"space": inlet_reservoir[0], "externalId": inlet_reservoir[1]},
            )
        )
    if inlet_reservoir and isinstance(inlet_reservoir, list) and isinstance(inlet_reservoir[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("inletReservoir"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in inlet_reservoir],
            )
        )
    if inlet_reservoir and isinstance(inlet_reservoir, list) and isinstance(inlet_reservoir[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("inletReservoir"),
                values=[{"space": item[0], "externalId": item[1]} for item in inlet_reservoir],
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
