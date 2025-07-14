from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
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
    BooleanFilter,
    DirectRelationFilter,
)
from cognite.powerops.client._generated.v1.data_classes._partial_bid_configuration import PartialBidConfiguration, PartialBidConfigurationWrite
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._plant_water_value_based import PlantWaterValueBased, PlantWaterValueBasedList, PlantWaterValueBasedGraphQL, PlantWaterValueBasedWrite, PlantWaterValueBasedWriteList


__all__ = [
    "WaterValueBasedPartialBidConfiguration",
    "WaterValueBasedPartialBidConfigurationWrite",
    "WaterValueBasedPartialBidConfigurationList",
    "WaterValueBasedPartialBidConfigurationWriteList",
    "WaterValueBasedPartialBidConfigurationFields",
    "WaterValueBasedPartialBidConfigurationTextFields",
    "WaterValueBasedPartialBidConfigurationGraphQL",
]


WaterValueBasedPartialBidConfigurationTextFields = Literal["external_id", "name", "method"]
WaterValueBasedPartialBidConfigurationFields = Literal["external_id", "name", "method", "add_steps"]

_WATERVALUEBASEDPARTIALBIDCONFIGURATION_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "name": "name",
    "method": "method",
    "add_steps": "addSteps",
}


class WaterValueBasedPartialBidConfigurationGraphQL(GraphQLCore):
    """This represents the reading version of water value based partial bid configuration, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the water value based partial bid configuration.
        data_record: The data record of the water value based partial bid configuration node.
        name: Name for the PartialBidConfiguration
        method: Name of the method used for the bid calculation
        power_asset: TODO description (has to be a Plant)
        add_steps: TODO definition
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "WaterValueBasedPartialBidConfiguration", "1")
    name: Optional[str] = None
    method: Optional[str] = None
    power_asset: Optional[PlantWaterValueBasedGraphQL] = Field(default=None, repr=False, alias="powerAsset")
    add_steps: Optional[bool] = Field(None, alias="addSteps")

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


    @field_validator("power_asset", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> WaterValueBasedPartialBidConfiguration:
        """Convert this GraphQL format of water value based partial bid configuration to the reading format."""
        return WaterValueBasedPartialBidConfiguration.model_validate(as_read_args(self))

    def as_write(self) -> WaterValueBasedPartialBidConfigurationWrite:
        """Convert this GraphQL format of water value based partial bid configuration to the writing format."""
        return WaterValueBasedPartialBidConfigurationWrite.model_validate(as_write_args(self))


class WaterValueBasedPartialBidConfiguration(PartialBidConfiguration):
    """This represents the reading version of water value based partial bid configuration.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the water value based partial bid configuration.
        data_record: The data record of the water value based partial bid configuration node.
        name: Name for the PartialBidConfiguration
        method: Name of the method used for the bid calculation
        power_asset: TODO description (has to be a Plant)
        add_steps: TODO definition
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "WaterValueBasedPartialBidConfiguration", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "WaterValueBasedPartialBidConfiguration")
    power_asset: Union[PlantWaterValueBased, str, dm.NodeId, None] = Field(default=None, repr=False, alias="powerAsset")
    @field_validator("power_asset", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)


    def as_write(self) -> WaterValueBasedPartialBidConfigurationWrite:
        """Convert this read version of water value based partial bid configuration to the writing version."""
        return WaterValueBasedPartialBidConfigurationWrite.model_validate(as_write_args(self))



class WaterValueBasedPartialBidConfigurationWrite(PartialBidConfigurationWrite):
    """This represents the writing version of water value based partial bid configuration.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the water value based partial bid configuration.
        data_record: The data record of the water value based partial bid configuration node.
        name: Name for the PartialBidConfiguration
        method: Name of the method used for the bid calculation
        power_asset: TODO description (has to be a Plant)
        add_steps: TODO definition
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("add_steps", "method", "name", "power_asset",)
    _direct_relations: ClassVar[tuple[str, ...]] = ("power_asset",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "WaterValueBasedPartialBidConfiguration", "1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "WaterValueBasedPartialBidConfiguration")
    power_asset: Union[PlantWaterValueBasedWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="powerAsset")

    @field_validator("power_asset", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class WaterValueBasedPartialBidConfigurationList(DomainModelList[WaterValueBasedPartialBidConfiguration]):
    """List of water value based partial bid configurations in the read version."""

    _INSTANCE = WaterValueBasedPartialBidConfiguration
    def as_write(self) -> WaterValueBasedPartialBidConfigurationWriteList:
        """Convert these read versions of water value based partial bid configuration to the writing versions."""
        return WaterValueBasedPartialBidConfigurationWriteList([node.as_write() for node in self.data])


    @property
    def power_asset(self) -> PlantWaterValueBasedList:
        from ._plant_water_value_based import PlantWaterValueBased, PlantWaterValueBasedList
        return PlantWaterValueBasedList([item.power_asset for item in self.data if isinstance(item.power_asset, PlantWaterValueBased)])

class WaterValueBasedPartialBidConfigurationWriteList(DomainModelWriteList[WaterValueBasedPartialBidConfigurationWrite]):
    """List of water value based partial bid configurations in the writing version."""

    _INSTANCE = WaterValueBasedPartialBidConfigurationWrite
    @property
    def power_asset(self) -> PlantWaterValueBasedWriteList:
        from ._plant_water_value_based import PlantWaterValueBasedWrite, PlantWaterValueBasedWriteList
        return PlantWaterValueBasedWriteList([item.power_asset for item in self.data if isinstance(item.power_asset, PlantWaterValueBasedWrite)])


def _create_water_value_based_partial_bid_configuration_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    method: str | list[str] | None = None,
    method_prefix: str | None = None,
    power_asset: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    add_steps: bool | None = None,
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
    if isinstance(method, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("method"), value=method))
    if method and isinstance(method, list):
        filters.append(dm.filters.In(view_id.as_property_ref("method"), values=method))
    if method_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("method"), value=method_prefix))
    if isinstance(power_asset, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(power_asset):
        filters.append(dm.filters.Equals(view_id.as_property_ref("powerAsset"), value=as_instance_dict_id(power_asset)))
    if power_asset and isinstance(power_asset, Sequence) and not isinstance(power_asset, str) and not is_tuple_id(power_asset):
        filters.append(dm.filters.In(view_id.as_property_ref("powerAsset"), values=[as_instance_dict_id(item) for item in power_asset]))
    if isinstance(add_steps, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("addSteps"), value=add_steps))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _WaterValueBasedPartialBidConfigurationQuery(NodeQueryCore[T_DomainModelList, WaterValueBasedPartialBidConfigurationList]):
    _view_id = WaterValueBasedPartialBidConfiguration._view_id
    _result_cls = WaterValueBasedPartialBidConfiguration
    _result_list_cls_end = WaterValueBasedPartialBidConfigurationList

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
        from ._plant_water_value_based import _PlantWaterValueBasedQuery

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

        if _PlantWaterValueBasedQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.power_asset = _PlantWaterValueBasedQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("powerAsset"),
                    direction="outwards",
                ),
                connection_name="power_asset",
                connection_property=ViewPropertyId(self._view_id, "powerAsset"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.method = StringFilter(self, self._view_id.as_property_ref("method"))
        self.power_asset_filter = DirectRelationFilter(self, self._view_id.as_property_ref("powerAsset"))
        self.add_steps = BooleanFilter(self, self._view_id.as_property_ref("addSteps"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
            self.method,
            self.power_asset_filter,
            self.add_steps,
        ])

    def list_water_value_based_partial_bid_configuration(self, limit: int = DEFAULT_QUERY_LIMIT) -> WaterValueBasedPartialBidConfigurationList:
        return self._list(limit=limit)


class WaterValueBasedPartialBidConfigurationQuery(_WaterValueBasedPartialBidConfigurationQuery[WaterValueBasedPartialBidConfigurationList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, WaterValueBasedPartialBidConfigurationList)
