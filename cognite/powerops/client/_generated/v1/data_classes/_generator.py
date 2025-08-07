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
    DirectRelationFilter,
    FloatFilter,
    IntFilter,
)
from cognite.powerops.client._generated.v1.data_classes._power_asset import PowerAsset, PowerAssetWrite
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._generator_efficiency_curve import GeneratorEfficiencyCurve, GeneratorEfficiencyCurveList, GeneratorEfficiencyCurveGraphQL, GeneratorEfficiencyCurveWrite, GeneratorEfficiencyCurveWriteList
    from cognite.powerops.client._generated.v1.data_classes._turbine_efficiency_curve import TurbineEfficiencyCurve, TurbineEfficiencyCurveList, TurbineEfficiencyCurveGraphQL, TurbineEfficiencyCurveWrite, TurbineEfficiencyCurveWriteList


__all__ = [
    "Generator",
    "GeneratorWrite",
    "GeneratorList",
    "GeneratorWriteList",
    "GeneratorFields",
    "GeneratorTextFields",
    "GeneratorGraphQL",
]


GeneratorTextFields = Literal["external_id", "name", "display_name", "asset_type", "start_stop_cost_time_series", "availability_time_series"]
GeneratorFields = Literal["external_id", "name", "display_name", "ordering", "asset_type", "production_min", "production_max", "penstock_number", "start_stop_cost", "start_stop_cost_time_series", "availability_time_series"]

_GENERATOR_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "name": "name",
    "display_name": "displayName",
    "ordering": "ordering",
    "asset_type": "assetType",
    "production_min": "productionMin",
    "production_max": "productionMax",
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
        production_max: The production max field.
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
    production_max: Optional[float] = Field(None, alias="productionMax")
    penstock_number: Optional[int] = Field(None, alias="penstockNumber")
    start_stop_cost: Optional[float] = Field(None, alias="startStopCost")
    start_stop_cost_time_series: Optional[TimeSeriesGraphQL] = Field(None, alias="startStopCostTimeSeries")
    availability_time_series: Optional[TimeSeriesGraphQL] = Field(None, alias="availabilityTimeSeries")
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

    def as_read(self) -> Generator:
        """Convert this GraphQL format of generator to the reading format."""
        return Generator.model_validate(as_read_args(self))

    def as_write(self) -> GeneratorWrite:
        """Convert this GraphQL format of generator to the writing format."""
        return GeneratorWrite.model_validate(as_write_args(self))


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
        production_max: The production max field.
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
    production_max: Optional[float] = Field(None, alias="productionMax")
    penstock_number: int = Field(alias="penstockNumber")
    start_stop_cost: float = Field(alias="startStopCost")
    start_stop_cost_time_series: Union[TimeSeries, str, None] = Field(None, alias="startStopCostTimeSeries")
    availability_time_series: Union[TimeSeries, str, None] = Field(None, alias="availabilityTimeSeries")
    generator_efficiency_curve: Union[GeneratorEfficiencyCurve, str, dm.NodeId, None] = Field(default=None, repr=False, alias="generatorEfficiencyCurve")
    turbine_efficiency_curves: Optional[list[Union[TurbineEfficiencyCurve, str, dm.NodeId]]] = Field(default=None, repr=False, alias="turbineEfficiencyCurves")
    @field_validator("generator_efficiency_curve", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    @field_validator("turbine_efficiency_curves", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> GeneratorWrite:
        """Convert this read version of generator to the writing version."""
        return GeneratorWrite.model_validate(as_write_args(self))



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
        production_max: The production max field.
        penstock_number: The penstock number field.
        start_stop_cost: The start stop cost field.
        start_stop_cost_time_series: The start stop cost time series field.
        availability_time_series: The availability time series field.
        generator_efficiency_curve: The generator efficiency curve field.
        turbine_efficiency_curves: TODO description
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("asset_type", "availability_time_series", "display_name", "generator_efficiency_curve", "name", "ordering", "penstock_number", "production_max", "production_min", "start_stop_cost", "start_stop_cost_time_series",)
    _outwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (("turbine_efficiency_curves", dm.DirectRelationReference("power_ops_types", "isSubAssetOf")),)
    _direct_relations: ClassVar[tuple[str, ...]] = ("generator_efficiency_curve",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "Generator", "1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "Generator")
    production_min: float = Field(alias="productionMin")
    production_max: Optional[float] = Field(None, alias="productionMax")
    penstock_number: int = Field(alias="penstockNumber")
    start_stop_cost: float = Field(alias="startStopCost")
    start_stop_cost_time_series: Union[TimeSeriesWrite, str, None] = Field(None, alias="startStopCostTimeSeries")
    availability_time_series: Union[TimeSeriesWrite, str, None] = Field(None, alias="availabilityTimeSeries")
    generator_efficiency_curve: Union[GeneratorEfficiencyCurveWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="generatorEfficiencyCurve")
    turbine_efficiency_curves: Optional[list[Union[TurbineEfficiencyCurveWrite, str, dm.NodeId]]] = Field(default=None, repr=False, alias="turbineEfficiencyCurves")

    @field_validator("generator_efficiency_curve", "turbine_efficiency_curves", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class GeneratorList(DomainModelList[Generator]):
    """List of generators in the read version."""

    _INSTANCE = Generator
    def as_write(self) -> GeneratorWriteList:
        """Convert these read versions of generator to the writing versions."""
        return GeneratorWriteList([node.as_write() for node in self.data])


    @property
    def generator_efficiency_curve(self) -> GeneratorEfficiencyCurveList:
        from ._generator_efficiency_curve import GeneratorEfficiencyCurve, GeneratorEfficiencyCurveList
        return GeneratorEfficiencyCurveList([item.generator_efficiency_curve for item in self.data if isinstance(item.generator_efficiency_curve, GeneratorEfficiencyCurve)])
    @property
    def turbine_efficiency_curves(self) -> TurbineEfficiencyCurveList:
        from ._turbine_efficiency_curve import TurbineEfficiencyCurve, TurbineEfficiencyCurveList
        return TurbineEfficiencyCurveList([item for items in self.data for item in items.turbine_efficiency_curves or [] if isinstance(item, TurbineEfficiencyCurve)])


class GeneratorWriteList(DomainModelWriteList[GeneratorWrite]):
    """List of generators in the writing version."""

    _INSTANCE = GeneratorWrite
    @property
    def generator_efficiency_curve(self) -> GeneratorEfficiencyCurveWriteList:
        from ._generator_efficiency_curve import GeneratorEfficiencyCurveWrite, GeneratorEfficiencyCurveWriteList
        return GeneratorEfficiencyCurveWriteList([item.generator_efficiency_curve for item in self.data if isinstance(item.generator_efficiency_curve, GeneratorEfficiencyCurveWrite)])
    @property
    def turbine_efficiency_curves(self) -> TurbineEfficiencyCurveWriteList:
        from ._turbine_efficiency_curve import TurbineEfficiencyCurveWrite, TurbineEfficiencyCurveWriteList
        return TurbineEfficiencyCurveWriteList([item for items in self.data for item in items.turbine_efficiency_curves or [] if isinstance(item, TurbineEfficiencyCurveWrite)])



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
    min_production_max: float | None = None,
    max_production_max: float | None = None,
    min_penstock_number: int | None = None,
    max_penstock_number: int | None = None,
    min_start_stop_cost: float | None = None,
    max_start_stop_cost: float | None = None,
    generator_efficiency_curve: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
    if min_production_max is not None or max_production_max is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("productionMax"), gte=min_production_max, lte=max_production_max))
    if min_penstock_number is not None or max_penstock_number is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("penstockNumber"), gte=min_penstock_number, lte=max_penstock_number))
    if min_start_stop_cost is not None or max_start_stop_cost is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("startStopCost"), gte=min_start_stop_cost, lte=max_start_stop_cost))
    if isinstance(generator_efficiency_curve, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(generator_efficiency_curve):
        filters.append(dm.filters.Equals(view_id.as_property_ref("generatorEfficiencyCurve"), value=as_instance_dict_id(generator_efficiency_curve)))
    if generator_efficiency_curve and isinstance(generator_efficiency_curve, Sequence) and not isinstance(generator_efficiency_curve, str) and not is_tuple_id(generator_efficiency_curve):
        filters.append(dm.filters.In(view_id.as_property_ref("generatorEfficiencyCurve"), values=[as_instance_dict_id(item) for item in generator_efficiency_curve]))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _GeneratorQuery(NodeQueryCore[T_DomainModelList, GeneratorList]):
    _view_id = Generator._view_id
    _result_cls = Generator
    _result_list_cls_end = GeneratorList

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
        from ._generator_efficiency_curve import _GeneratorEfficiencyCurveQuery
        from ._turbine_efficiency_curve import _TurbineEfficiencyCurveQuery

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

        if _GeneratorEfficiencyCurveQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.generator_efficiency_curve = _GeneratorEfficiencyCurveQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("generatorEfficiencyCurve"),
                    direction="outwards",
                ),
                connection_name="generator_efficiency_curve",
                connection_property=ViewPropertyId(self._view_id, "generatorEfficiencyCurve"),
            )

        if _TurbineEfficiencyCurveQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.turbine_efficiency_curves = _TurbineEfficiencyCurveQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="turbine_efficiency_curves",
                connection_property=ViewPropertyId(self._view_id, "turbineEfficiencyCurves"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.display_name = StringFilter(self, self._view_id.as_property_ref("displayName"))
        self.ordering = IntFilter(self, self._view_id.as_property_ref("ordering"))
        self.asset_type = StringFilter(self, self._view_id.as_property_ref("assetType"))
        self.production_min = FloatFilter(self, self._view_id.as_property_ref("productionMin"))
        self.production_max = FloatFilter(self, self._view_id.as_property_ref("productionMax"))
        self.penstock_number = IntFilter(self, self._view_id.as_property_ref("penstockNumber"))
        self.start_stop_cost = FloatFilter(self, self._view_id.as_property_ref("startStopCost"))
        self.generator_efficiency_curve_filter = DirectRelationFilter(self, self._view_id.as_property_ref("generatorEfficiencyCurve"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
            self.display_name,
            self.ordering,
            self.asset_type,
            self.production_min,
            self.production_max,
            self.penstock_number,
            self.start_stop_cost,
            self.generator_efficiency_curve_filter,
        ])
        self.start_stop_cost_time_series = TimeSeriesReferenceAPI(client,  lambda limit: [
            item.start_stop_cost_time_series if isinstance(item.start_stop_cost_time_series, str) else item.start_stop_cost_time_series.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.start_stop_cost_time_series is not None and
               (isinstance(item.start_stop_cost_time_series, str) or item.start_stop_cost_time_series.external_id is not None)
        ])
        self.availability_time_series = TimeSeriesReferenceAPI(client,  lambda limit: [
            item.availability_time_series if isinstance(item.availability_time_series, str) else item.availability_time_series.external_id #type: ignore[misc]
            for item in self._list(limit=limit)
            if item.availability_time_series is not None and
               (isinstance(item.availability_time_series, str) or item.availability_time_series.external_id is not None)
        ])

    def list_generator(self, limit: int = DEFAULT_QUERY_LIMIT) -> GeneratorList:
        return self._list(limit=limit)


class GeneratorQuery(_GeneratorQuery[GeneratorList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, GeneratorList)
