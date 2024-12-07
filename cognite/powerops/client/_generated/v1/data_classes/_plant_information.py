from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal,  no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from cognite.client.data_classes import (
    TimeSeries as CogniteTimeSeries,
    TimeSeriesWrite as CogniteTimeSeriesWrite,
)
from pydantic import Field
from pydantic import field_validator, model_validator

from cognite.powerops.client._generated.v1.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelation,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
    FileMetadata,
    FileMetadataWrite,
    FileMetadataGraphQL,
    TimeSeries,
    TimeSeriesWrite,
    TimeSeriesGraphQL,
    TimeSeriesReferenceAPI,
    T_DomainModelList,
    as_direct_relation_reference,
    as_instance_dict_id,
    as_node_id,
    as_pygen_node_id,
    are_nodes_equal,
    is_tuple_id,
    select_best_node,
    QueryCore,
    NodeQueryCore,
    StringFilter,
    FloatFilter,
    IntFilter,
)
from cognite.powerops.client._generated.v1.data_classes._plant_water_value_based import PlantWaterValueBased, PlantWaterValueBasedWrite
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._generator import Generator, GeneratorList, GeneratorGraphQL, GeneratorWrite, GeneratorWriteList


__all__ = [
    "PlantInformation",
    "PlantInformationWrite",
    "PlantInformationApply",
    "PlantInformationList",
    "PlantInformationWriteList",
    "PlantInformationApplyList",
    "PlantInformationFields",
    "PlantInformationTextFields",
    "PlantInformationGraphQL",
]


PlantInformationTextFields = Literal["external_id", "name", "display_name", "asset_type", "production_max_time_series", "production_min_time_series", "water_value_time_series", "feeding_fee_time_series", "outlet_level_time_series", "inlet_level_time_series", "head_direct_time_series"]
PlantInformationFields = Literal["external_id", "name", "display_name", "ordering", "asset_type", "head_loss_factor", "outlet_level", "production_max", "production_min", "penstock_head_loss_factors", "connection_losses", "production_max_time_series", "production_min_time_series", "water_value_time_series", "feeding_fee_time_series", "outlet_level_time_series", "inlet_level_time_series", "head_direct_time_series"]

_PLANTINFORMATION_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
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


class PlantInformationGraphQL(GraphQLCore):
    """This represents the reading version of plant information, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the plant information.
        data_record: The data record of the plant information node.
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

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PlantInformation", "1")
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
    production_max_time_series: Optional[TimeSeriesGraphQL] = Field(None, alias="productionMaxTimeSeries")
    production_min_time_series: Optional[TimeSeriesGraphQL] = Field(None, alias="productionMinTimeSeries")
    water_value_time_series: Optional[TimeSeriesGraphQL] = Field(None, alias="waterValueTimeSeries")
    feeding_fee_time_series: Optional[TimeSeriesGraphQL] = Field(None, alias="feedingFeeTimeSeries")
    outlet_level_time_series: Optional[TimeSeriesGraphQL] = Field(None, alias="outletLevelTimeSeries")
    inlet_level_time_series: Optional[TimeSeriesGraphQL] = Field(None, alias="inletLevelTimeSeries")
    head_direct_time_series: Optional[TimeSeriesGraphQL] = Field(None, alias="headDirectTimeSeries")
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
    def as_read(self) -> PlantInformation:
        """Convert this GraphQL format of plant information to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return PlantInformation(
            space=self.space,
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
            production_max_time_series=self.production_max_time_series.as_read() if self.production_max_time_series else None,
            production_min_time_series=self.production_min_time_series.as_read() if self.production_min_time_series else None,
            water_value_time_series=self.water_value_time_series.as_read() if self.water_value_time_series else None,
            feeding_fee_time_series=self.feeding_fee_time_series.as_read() if self.feeding_fee_time_series else None,
            outlet_level_time_series=self.outlet_level_time_series.as_read() if self.outlet_level_time_series else None,
            inlet_level_time_series=self.inlet_level_time_series.as_read() if self.inlet_level_time_series else None,
            head_direct_time_series=self.head_direct_time_series.as_read() if self.head_direct_time_series else None,
            generators=[generator.as_read() for generator in self.generators] if self.generators is not None else None,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> PlantInformationWrite:
        """Convert this GraphQL format of plant information to the writing format."""
        return PlantInformationWrite(
            space=self.space,
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
            production_max_time_series=self.production_max_time_series.as_write() if self.production_max_time_series else None,
            production_min_time_series=self.production_min_time_series.as_write() if self.production_min_time_series else None,
            water_value_time_series=self.water_value_time_series.as_write() if self.water_value_time_series else None,
            feeding_fee_time_series=self.feeding_fee_time_series.as_write() if self.feeding_fee_time_series else None,
            outlet_level_time_series=self.outlet_level_time_series.as_write() if self.outlet_level_time_series else None,
            inlet_level_time_series=self.inlet_level_time_series.as_write() if self.inlet_level_time_series else None,
            head_direct_time_series=self.head_direct_time_series.as_write() if self.head_direct_time_series else None,
            generators=[generator.as_write() for generator in self.generators] if self.generators is not None else None,
        )


class PlantInformation(PlantWaterValueBased):
    """This represents the reading version of plant information.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the plant information.
        data_record: The data record of the plant information node.
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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PlantInformation", "1")

    node_type: Union[dm.DirectRelationReference, None] = None

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> PlantInformationWrite:
        """Convert this read version of plant information to the writing version."""
        return PlantInformationWrite(
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
            production_max_time_series=self.production_max_time_series.as_write() if isinstance(self.production_max_time_series, CogniteTimeSeries) else self.production_max_time_series,
            production_min_time_series=self.production_min_time_series.as_write() if isinstance(self.production_min_time_series, CogniteTimeSeries) else self.production_min_time_series,
            water_value_time_series=self.water_value_time_series.as_write() if isinstance(self.water_value_time_series, CogniteTimeSeries) else self.water_value_time_series,
            feeding_fee_time_series=self.feeding_fee_time_series.as_write() if isinstance(self.feeding_fee_time_series, CogniteTimeSeries) else self.feeding_fee_time_series,
            outlet_level_time_series=self.outlet_level_time_series.as_write() if isinstance(self.outlet_level_time_series, CogniteTimeSeries) else self.outlet_level_time_series,
            inlet_level_time_series=self.inlet_level_time_series.as_write() if isinstance(self.inlet_level_time_series, CogniteTimeSeries) else self.inlet_level_time_series,
            head_direct_time_series=self.head_direct_time_series.as_write() if isinstance(self.head_direct_time_series, CogniteTimeSeries) else self.head_direct_time_series,
            generators=[generator.as_write() if isinstance(generator, DomainModel) else generator for generator in self.generators] if self.generators is not None else None,
        )

    def as_apply(self) -> PlantInformationWrite:
        """Convert this read version of plant information to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()
    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, PlantInformation],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._generator import Generator
        for instance in instances.values():
            if edges := edges_by_source_node.get(instance.as_id()):
                generators: list[Generator | str | dm.NodeId] = []
                for edge in edges:
                    value: DomainModel | DomainRelation | str | dm.NodeId
                    if isinstance(edge, DomainRelation):
                        value = edge
                    else:
                        other_end: dm.DirectRelationReference = (
                            edge.end_node
                            if edge.start_node.space == instance.space
                            and edge.start_node.external_id == instance.external_id
                            else edge.start_node
                        )
                        destination: dm.NodeId | str = (
                            as_node_id(other_end)
                            if other_end.space != DEFAULT_INSTANCE_SPACE
                            else other_end.external_id
                        )
                        if destination in nodes_by_id:
                            value = nodes_by_id[destination]
                        else:
                            value = destination
                    edge_type = edge.edge_type if isinstance(edge, DomainRelation) else edge.type

                    if edge_type == dm.DirectRelationReference("power_ops_types", "isSubAssetOf") and isinstance(
                        value, (Generator, str, dm.NodeId)
                    ):
                        generators.append(value)

                instance.generators = generators or None



class PlantInformationWrite(PlantWaterValueBasedWrite):
    """This represents the writing version of plant information.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the plant information.
        data_record: The data record of the plant information node.
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

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PlantInformation", "1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None


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
                type=as_direct_relation_reference(self.node_type),
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

        if isinstance(self.production_max_time_series, CogniteTimeSeriesWrite):
            resources.time_series.append(self.production_max_time_series)

        if isinstance(self.production_min_time_series, CogniteTimeSeriesWrite):
            resources.time_series.append(self.production_min_time_series)

        if isinstance(self.water_value_time_series, CogniteTimeSeriesWrite):
            resources.time_series.append(self.water_value_time_series)

        if isinstance(self.feeding_fee_time_series, CogniteTimeSeriesWrite):
            resources.time_series.append(self.feeding_fee_time_series)

        if isinstance(self.outlet_level_time_series, CogniteTimeSeriesWrite):
            resources.time_series.append(self.outlet_level_time_series)

        if isinstance(self.inlet_level_time_series, CogniteTimeSeriesWrite):
            resources.time_series.append(self.inlet_level_time_series)

        if isinstance(self.head_direct_time_series, CogniteTimeSeriesWrite):
            resources.time_series.append(self.head_direct_time_series)

        return resources


class PlantInformationApply(PlantInformationWrite):
    def __new__(cls, *args, **kwargs) -> PlantInformationApply:
        warnings.warn(
            "PlantInformationApply is deprecated and will be removed in v1.0. Use PlantInformationWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "PlantInformation.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)

class PlantInformationList(DomainModelList[PlantInformation]):
    """List of plant information in the read version."""

    _INSTANCE = PlantInformation
    def as_write(self) -> PlantInformationWriteList:
        """Convert these read versions of plant information to the writing versions."""
        return PlantInformationWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> PlantInformationWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

    @property
    def generators(self) -> GeneratorList:
        from ._generator import Generator, GeneratorList
        return GeneratorList([item for items in self.data for item in items.generators or [] if isinstance(item, Generator)])


class PlantInformationWriteList(DomainModelWriteList[PlantInformationWrite]):
    """List of plant information in the writing version."""

    _INSTANCE = PlantInformationWrite
    @property
    def generators(self) -> GeneratorWriteList:
        from ._generator import GeneratorWrite, GeneratorWriteList
        return GeneratorWriteList([item for items in self.data for item in items.generators or [] if isinstance(item, GeneratorWrite)])


class PlantInformationApplyList(PlantInformationWriteList): ...


def _create_plant_information_filter(
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


class _PlantInformationQuery(NodeQueryCore[T_DomainModelList, PlantInformationList]):
    _view_id = PlantInformation._view_id
    _result_cls = PlantInformation
    _result_list_cls_end = PlantInformationList

    def __init__(
        self,
        created_types: set[type],
        creation_path: list[QueryCore],
        client: CogniteClient,
        result_list_cls: type[T_DomainModelList],
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
    ):
        from ._generator import _GeneratorQuery

        super().__init__(
            created_types,
            creation_path,
            client,
            result_list_cls,
            expression,
            dm.filters.HasData(views=[self._view_id]),
            connection_name,
            connection_type,
            reverse_expression,
        )

        if _GeneratorQuery not in created_types:
            self.generators = _GeneratorQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="generators",
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.display_name = StringFilter(self, self._view_id.as_property_ref("displayName"))
        self.ordering = IntFilter(self, self._view_id.as_property_ref("ordering"))
        self.asset_type = StringFilter(self, self._view_id.as_property_ref("assetType"))
        self.head_loss_factor = FloatFilter(self, self._view_id.as_property_ref("headLossFactor"))
        self.outlet_level = FloatFilter(self, self._view_id.as_property_ref("outletLevel"))
        self.production_max = FloatFilter(self, self._view_id.as_property_ref("productionMax"))
        self.production_min = FloatFilter(self, self._view_id.as_property_ref("productionMin"))
        self.connection_losses = FloatFilter(self, self._view_id.as_property_ref("connectionLosses"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
            self.display_name,
            self.ordering,
            self.asset_type,
            self.head_loss_factor,
            self.outlet_level,
            self.production_max,
            self.production_min,
            self.connection_losses,
        ])
        self.production_max_time_series = TimeSeriesReferenceAPI(client,  lambda limit: [
            item.production_max_time_series if isinstance(item.production_max_time_series, str) else item.production_max_time_series.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.production_max_time_series is not None and
               (isinstance(item.production_max_time_series, str) or item.production_max_time_series.external_id is not None)
        ])
        self.production_min_time_series = TimeSeriesReferenceAPI(client,  lambda limit: [
            item.production_min_time_series if isinstance(item.production_min_time_series, str) else item.production_min_time_series.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.production_min_time_series is not None and
               (isinstance(item.production_min_time_series, str) or item.production_min_time_series.external_id is not None)
        ])
        self.water_value_time_series = TimeSeriesReferenceAPI(client,  lambda limit: [
            item.water_value_time_series if isinstance(item.water_value_time_series, str) else item.water_value_time_series.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.water_value_time_series is not None and
               (isinstance(item.water_value_time_series, str) or item.water_value_time_series.external_id is not None)
        ])
        self.feeding_fee_time_series = TimeSeriesReferenceAPI(client,  lambda limit: [
            item.feeding_fee_time_series if isinstance(item.feeding_fee_time_series, str) else item.feeding_fee_time_series.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.feeding_fee_time_series is not None and
               (isinstance(item.feeding_fee_time_series, str) or item.feeding_fee_time_series.external_id is not None)
        ])
        self.outlet_level_time_series = TimeSeriesReferenceAPI(client,  lambda limit: [
            item.outlet_level_time_series if isinstance(item.outlet_level_time_series, str) else item.outlet_level_time_series.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.outlet_level_time_series is not None and
               (isinstance(item.outlet_level_time_series, str) or item.outlet_level_time_series.external_id is not None)
        ])
        self.inlet_level_time_series = TimeSeriesReferenceAPI(client,  lambda limit: [
            item.inlet_level_time_series if isinstance(item.inlet_level_time_series, str) else item.inlet_level_time_series.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.inlet_level_time_series is not None and
               (isinstance(item.inlet_level_time_series, str) or item.inlet_level_time_series.external_id is not None)
        ])
        self.head_direct_time_series = TimeSeriesReferenceAPI(client,  lambda limit: [
            item.head_direct_time_series if isinstance(item.head_direct_time_series, str) else item.head_direct_time_series.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.head_direct_time_series is not None and
               (isinstance(item.head_direct_time_series, str) or item.head_direct_time_series.external_id is not None)
        ])

    def list_plant_information(self, limit: int = DEFAULT_QUERY_LIMIT) -> PlantInformationList:
        return self._list(limit=limit)


class PlantInformationQuery(_PlantInformationQuery[PlantInformationList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, PlantInformationList)
