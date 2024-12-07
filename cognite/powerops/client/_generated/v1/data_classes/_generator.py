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
from cognite.powerops.client._generated.v1.data_classes._power_asset import PowerAsset, PowerAssetWrite
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._generator_efficiency_curve import GeneratorEfficiencyCurve, GeneratorEfficiencyCurveList, GeneratorEfficiencyCurveGraphQL, GeneratorEfficiencyCurveWrite, GeneratorEfficiencyCurveWriteList
    from cognite.powerops.client._generated.v1.data_classes._turbine_efficiency_curve import TurbineEfficiencyCurve, TurbineEfficiencyCurveList, TurbineEfficiencyCurveGraphQL, TurbineEfficiencyCurveWrite, TurbineEfficiencyCurveWriteList


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


GeneratorTextFields = Literal["external_id", "name", "display_name", "asset_type", "start_stop_cost_time_series", "availability_time_series"]
GeneratorFields = Literal["external_id", "name", "display_name", "ordering", "asset_type", "production_min", "penstock_number", "start_stop_cost", "start_stop_cost_time_series", "availability_time_series", "production_max"]

_GENERATOR_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "name": "name",
    "display_name": "displayName",
    "ordering": "ordering",
    "asset_type": "assetType",
    "production_min": "productionMin",
    "penstock_number": "penstockNumber",
    "start_stop_cost": "startStopCost",
    "start_stop_cost_time_series": "startStopCostTimeSeries",
    "availability_time_series": "availabilityTimeSeries",
    "production_max": "productionMax",
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
        production_max: The production max field.
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
    start_stop_cost_time_series: Optional[TimeSeriesGraphQL] = Field(None, alias="startStopCostTimeSeries")
    availability_time_series: Optional[TimeSeriesGraphQL] = Field(None, alias="availabilityTimeSeries")
    generator_efficiency_curve: Optional[GeneratorEfficiencyCurveGraphQL] = Field(default=None, repr=False, alias="generatorEfficiencyCurve")
    production_max: Optional[float] = Field(None, alias="productionMax")
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
            production_min=self.production_min,
            penstock_number=self.penstock_number,
            start_stop_cost=self.start_stop_cost,
            start_stop_cost_time_series=self.start_stop_cost_time_series.as_read() if self.start_stop_cost_time_series else None,
            availability_time_series=self.availability_time_series.as_read() if self.availability_time_series else None,
            generator_efficiency_curve=self.generator_efficiency_curve.as_read()
if isinstance(self.generator_efficiency_curve, GraphQLCore)
else self.generator_efficiency_curve,
            production_max=self.production_max,
            turbine_efficiency_curves=[turbine_efficiency_curve.as_read() for turbine_efficiency_curve in self.turbine_efficiency_curves] if self.turbine_efficiency_curves is not None else None,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> GeneratorWrite:
        """Convert this GraphQL format of generator to the writing format."""
        return GeneratorWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            display_name=self.display_name,
            ordering=self.ordering,
            asset_type=self.asset_type,
            production_min=self.production_min,
            penstock_number=self.penstock_number,
            start_stop_cost=self.start_stop_cost,
            start_stop_cost_time_series=self.start_stop_cost_time_series.as_write() if self.start_stop_cost_time_series else None,
            availability_time_series=self.availability_time_series.as_write() if self.availability_time_series else None,
            generator_efficiency_curve=self.generator_efficiency_curve.as_write()
if isinstance(self.generator_efficiency_curve, GraphQLCore)
else self.generator_efficiency_curve,
            production_max=self.production_max,
            turbine_efficiency_curves=[turbine_efficiency_curve.as_write() for turbine_efficiency_curve in self.turbine_efficiency_curves] if self.turbine_efficiency_curves is not None else None,
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
        production_max: The production max field.
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
    production_max: Optional[float] = Field(None, alias="productionMax")
    turbine_efficiency_curves: Optional[list[Union[TurbineEfficiencyCurve, str, dm.NodeId]]] = Field(default=None, repr=False, alias="turbineEfficiencyCurves")

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
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
            start_stop_cost_time_series=self.start_stop_cost_time_series.as_write() if isinstance(self.start_stop_cost_time_series, CogniteTimeSeries) else self.start_stop_cost_time_series,
            availability_time_series=self.availability_time_series.as_write() if isinstance(self.availability_time_series, CogniteTimeSeries) else self.availability_time_series,
            generator_efficiency_curve=self.generator_efficiency_curve.as_write()
if isinstance(self.generator_efficiency_curve, DomainModel)
else self.generator_efficiency_curve,
            production_max=self.production_max,
            turbine_efficiency_curves=[turbine_efficiency_curve.as_write() if isinstance(turbine_efficiency_curve, DomainModel) else turbine_efficiency_curve for turbine_efficiency_curve in self.turbine_efficiency_curves] if self.turbine_efficiency_curves is not None else None,
        )

    def as_apply(self) -> GeneratorWrite:
        """Convert this read version of generator to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()
    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, Generator],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._generator_efficiency_curve import GeneratorEfficiencyCurve
        from ._turbine_efficiency_curve import TurbineEfficiencyCurve
        for instance in instances.values():
            if isinstance(instance.generator_efficiency_curve, (dm.NodeId, str)) and (generator_efficiency_curve := nodes_by_id.get(instance.generator_efficiency_curve)) and isinstance(
                    generator_efficiency_curve, GeneratorEfficiencyCurve
            ):
                instance.generator_efficiency_curve = generator_efficiency_curve
            if edges := edges_by_source_node.get(instance.as_id()):
                turbine_efficiency_curves: list[TurbineEfficiencyCurve | str | dm.NodeId] = []
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
                        value, (TurbineEfficiencyCurve, str, dm.NodeId)
                    ):
                        turbine_efficiency_curves.append(value)

                instance.turbine_efficiency_curves = turbine_efficiency_curves or None



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
        production_max: The production max field.
        turbine_efficiency_curves: TODO description
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "Generator", "1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "Generator")
    production_min: float = Field(alias="productionMin")
    penstock_number: int = Field(alias="penstockNumber")
    start_stop_cost: float = Field(alias="startStopCost")
    start_stop_cost_time_series: Union[TimeSeriesWrite, str, None] = Field(None, alias="startStopCostTimeSeries")
    availability_time_series: Union[TimeSeriesWrite, str, None] = Field(None, alias="availabilityTimeSeries")
    generator_efficiency_curve: Union[GeneratorEfficiencyCurveWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="generatorEfficiencyCurve")
    production_max: Optional[float] = Field(None, alias="productionMax")
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

        if self.production_max is not None or write_none:
            properties["productionMax"] = self.production_max

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

        if isinstance(self.start_stop_cost_time_series, CogniteTimeSeriesWrite):
            resources.time_series.append(self.start_stop_cost_time_series)

        if isinstance(self.availability_time_series, CogniteTimeSeriesWrite):
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
    generator_efficiency_curve: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    min_production_max: float | None = None,
    max_production_max: float | None = None,
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
    if isinstance(generator_efficiency_curve, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(generator_efficiency_curve):
        filters.append(dm.filters.Equals(view_id.as_property_ref("generatorEfficiencyCurve"), value=as_instance_dict_id(generator_efficiency_curve)))
    if generator_efficiency_curve and isinstance(generator_efficiency_curve, Sequence) and not isinstance(generator_efficiency_curve, str) and not is_tuple_id(generator_efficiency_curve):
        filters.append(dm.filters.In(view_id.as_property_ref("generatorEfficiencyCurve"), values=[as_instance_dict_id(item) for item in generator_efficiency_curve]))
    if min_production_max is not None or max_production_max is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("productionMax"), gte=min_production_max, lte=max_production_max))
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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
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
            connection_type,
            reverse_expression,
        )

        if _GeneratorEfficiencyCurveQuery not in created_types:
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
            )

        if _TurbineEfficiencyCurveQuery not in created_types:
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
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.display_name = StringFilter(self, self._view_id.as_property_ref("displayName"))
        self.ordering = IntFilter(self, self._view_id.as_property_ref("ordering"))
        self.asset_type = StringFilter(self, self._view_id.as_property_ref("assetType"))
        self.production_min = FloatFilter(self, self._view_id.as_property_ref("productionMin"))
        self.penstock_number = IntFilter(self, self._view_id.as_property_ref("penstockNumber"))
        self.start_stop_cost = FloatFilter(self, self._view_id.as_property_ref("startStopCost"))
        self.production_max = FloatFilter(self, self._view_id.as_property_ref("productionMax"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
            self.display_name,
            self.ordering,
            self.asset_type,
            self.production_min,
            self.penstock_number,
            self.start_stop_cost,
            self.production_max,
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
