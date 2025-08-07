from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
from cognite.client.data_classes import (
    TimeSeries as CogniteTimeSeries,
    TimeSeriesWrite as CogniteTimeSeriesWrite,
)
from pydantic import Field
from pydantic import field_validator, model_validator, ValidationInfo

from cognite.powerops.client._generated.v1.config import global_config
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
    as_node_id,
    as_read_args,
    as_write_args,
    is_tuple_id,
    as_instance_dict_id,
    parse_single_connection,
    QueryCore,
    NodeQueryCore,
    StringFilter,
    ViewPropertyId,
    FloatFilter,
    IntFilter,
)
from cognite.powerops.client._generated.v1.data_classes._plant_water_value_based import PlantWaterValueBased, PlantWaterValueBasedWrite
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._generator import Generator, GeneratorList, GeneratorGraphQL, GeneratorWrite, GeneratorWriteList


__all__ = [
    "PlantInformation",
    "PlantInformationWrite",
    "PlantInformationList",
    "PlantInformationWriteList",
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

    def as_read(self) -> PlantInformation:
        """Convert this GraphQL format of plant information to the reading format."""
        return PlantInformation.model_validate(as_read_args(self))

    def as_write(self) -> PlantInformationWrite:
        """Convert this GraphQL format of plant information to the writing format."""
        return PlantInformationWrite.model_validate(as_write_args(self))


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

    @field_validator("generators", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> PlantInformationWrite:
        """Convert this read version of plant information to the writing version."""
        return PlantInformationWrite.model_validate(as_write_args(self))



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
    _container_fields: ClassVar[tuple[str, ...]] = ("asset_type", "connection_losses", "display_name", "feeding_fee_time_series", "head_direct_time_series", "head_loss_factor", "inlet_level_time_series", "name", "ordering", "outlet_level", "outlet_level_time_series", "penstock_head_loss_factors", "production_max", "production_max_time_series", "production_min", "production_min_time_series", "water_value_time_series",)
    _outwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (("generators", dm.DirectRelationReference("power_ops_types", "isSubAssetOf")),)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "PlantInformation", "1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None



class PlantInformationList(DomainModelList[PlantInformation]):
    """List of plant information in the read version."""

    _INSTANCE = PlantInformation
    def as_write(self) -> PlantInformationWriteList:
        """Convert these read versions of plant information to the writing versions."""
        return PlantInformationWriteList([node.as_write() for node in self.data])


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
        expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
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
            connection_property,
            connection_type,
            reverse_expression,
        )

        if _GeneratorQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "generators"),
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
