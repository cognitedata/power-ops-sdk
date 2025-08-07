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
    from cognite.powerops.client._generated.v1.data_classes._power_asset import PowerAsset, PowerAssetList, PowerAssetGraphQL, PowerAssetWrite, PowerAssetWriteList
    from cognite.powerops.client._generated.v1.data_classes._shop_scenario_set import ShopScenarioSet, ShopScenarioSetList, ShopScenarioSetGraphQL, ShopScenarioSetWrite, ShopScenarioSetWriteList


__all__ = [
    "ShopBasedPartialBidConfiguration",
    "ShopBasedPartialBidConfigurationWrite",
    "ShopBasedPartialBidConfigurationList",
    "ShopBasedPartialBidConfigurationWriteList",
    "ShopBasedPartialBidConfigurationFields",
    "ShopBasedPartialBidConfigurationTextFields",
    "ShopBasedPartialBidConfigurationGraphQL",
]


ShopBasedPartialBidConfigurationTextFields = Literal["external_id", "name", "method"]
ShopBasedPartialBidConfigurationFields = Literal["external_id", "name", "method", "add_steps"]

_SHOPBASEDPARTIALBIDCONFIGURATION_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "name": "name",
    "method": "method",
    "add_steps": "addSteps",
}


class ShopBasedPartialBidConfigurationGraphQL(GraphQLCore):
    """This represents the reading version of shop based partial bid configuration, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop based partial bid configuration.
        data_record: The data record of the shop based partial bid configuration node.
        name: Name for the PartialBidConfiguration
        method: Name of the method used for the bid calculation
        power_asset: TODO description
        add_steps: TODO definition
        scenario_set: The scenario set field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopBasedPartialBidConfiguration", "1")
    name: Optional[str] = None
    method: Optional[str] = None
    power_asset: Optional[PowerAssetGraphQL] = Field(default=None, repr=False, alias="powerAsset")
    add_steps: Optional[bool] = Field(None, alias="addSteps")
    scenario_set: Optional[ShopScenarioSetGraphQL] = Field(default=None, repr=False, alias="scenarioSet")

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


    @field_validator("power_asset", "scenario_set", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> ShopBasedPartialBidConfiguration:
        """Convert this GraphQL format of shop based partial bid configuration to the reading format."""
        return ShopBasedPartialBidConfiguration.model_validate(as_read_args(self))

    def as_write(self) -> ShopBasedPartialBidConfigurationWrite:
        """Convert this GraphQL format of shop based partial bid configuration to the writing format."""
        return ShopBasedPartialBidConfigurationWrite.model_validate(as_write_args(self))


class ShopBasedPartialBidConfiguration(PartialBidConfiguration):
    """This represents the reading version of shop based partial bid configuration.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop based partial bid configuration.
        data_record: The data record of the shop based partial bid configuration node.
        name: Name for the PartialBidConfiguration
        method: Name of the method used for the bid calculation
        power_asset: TODO description
        add_steps: TODO definition
        scenario_set: The scenario set field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopBasedPartialBidConfiguration", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "ShopBasedPartialBidConfiguration")
    scenario_set: Union[ShopScenarioSet, str, dm.NodeId, None] = Field(default=None, repr=False, alias="scenarioSet")
    @field_validator("power_asset", "scenario_set", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)


    def as_write(self) -> ShopBasedPartialBidConfigurationWrite:
        """Convert this read version of shop based partial bid configuration to the writing version."""
        return ShopBasedPartialBidConfigurationWrite.model_validate(as_write_args(self))



class ShopBasedPartialBidConfigurationWrite(PartialBidConfigurationWrite):
    """This represents the writing version of shop based partial bid configuration.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop based partial bid configuration.
        data_record: The data record of the shop based partial bid configuration node.
        name: Name for the PartialBidConfiguration
        method: Name of the method used for the bid calculation
        power_asset: TODO description
        add_steps: TODO definition
        scenario_set: The scenario set field.
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("add_steps", "method", "name", "power_asset", "scenario_set",)
    _direct_relations: ClassVar[tuple[str, ...]] = ("power_asset", "scenario_set",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopBasedPartialBidConfiguration", "1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "ShopBasedPartialBidConfiguration")
    scenario_set: Union[ShopScenarioSetWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="scenarioSet")

    @field_validator("scenario_set", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class ShopBasedPartialBidConfigurationList(DomainModelList[ShopBasedPartialBidConfiguration]):
    """List of shop based partial bid configurations in the read version."""

    _INSTANCE = ShopBasedPartialBidConfiguration
    def as_write(self) -> ShopBasedPartialBidConfigurationWriteList:
        """Convert these read versions of shop based partial bid configuration to the writing versions."""
        return ShopBasedPartialBidConfigurationWriteList([node.as_write() for node in self.data])


    @property
    def power_asset(self) -> PowerAssetList:
        from ._power_asset import PowerAsset, PowerAssetList
        return PowerAssetList([item.power_asset for item in self.data if isinstance(item.power_asset, PowerAsset)])
    @property
    def scenario_set(self) -> ShopScenarioSetList:
        from ._shop_scenario_set import ShopScenarioSet, ShopScenarioSetList
        return ShopScenarioSetList([item.scenario_set for item in self.data if isinstance(item.scenario_set, ShopScenarioSet)])

class ShopBasedPartialBidConfigurationWriteList(DomainModelWriteList[ShopBasedPartialBidConfigurationWrite]):
    """List of shop based partial bid configurations in the writing version."""

    _INSTANCE = ShopBasedPartialBidConfigurationWrite
    @property
    def power_asset(self) -> PowerAssetWriteList:
        from ._power_asset import PowerAssetWrite, PowerAssetWriteList
        return PowerAssetWriteList([item.power_asset for item in self.data if isinstance(item.power_asset, PowerAssetWrite)])
    @property
    def scenario_set(self) -> ShopScenarioSetWriteList:
        from ._shop_scenario_set import ShopScenarioSetWrite, ShopScenarioSetWriteList
        return ShopScenarioSetWriteList([item.scenario_set for item in self.data if isinstance(item.scenario_set, ShopScenarioSetWrite)])


def _create_shop_based_partial_bid_configuration_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    method: str | list[str] | None = None,
    method_prefix: str | None = None,
    power_asset: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    add_steps: bool | None = None,
    scenario_set: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
    if isinstance(scenario_set, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(scenario_set):
        filters.append(dm.filters.Equals(view_id.as_property_ref("scenarioSet"), value=as_instance_dict_id(scenario_set)))
    if scenario_set and isinstance(scenario_set, Sequence) and not isinstance(scenario_set, str) and not is_tuple_id(scenario_set):
        filters.append(dm.filters.In(view_id.as_property_ref("scenarioSet"), values=[as_instance_dict_id(item) for item in scenario_set]))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _ShopBasedPartialBidConfigurationQuery(NodeQueryCore[T_DomainModelList, ShopBasedPartialBidConfigurationList]):
    _view_id = ShopBasedPartialBidConfiguration._view_id
    _result_cls = ShopBasedPartialBidConfiguration
    _result_list_cls_end = ShopBasedPartialBidConfigurationList

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
        from ._power_asset import _PowerAssetQuery
        from ._shop_scenario_set import _ShopScenarioSetQuery

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

        if _PowerAssetQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.power_asset = _PowerAssetQuery(
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

        if _ShopScenarioSetQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.scenario_set = _ShopScenarioSetQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("scenarioSet"),
                    direction="outwards",
                ),
                connection_name="scenario_set",
                connection_property=ViewPropertyId(self._view_id, "scenarioSet"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.method = StringFilter(self, self._view_id.as_property_ref("method"))
        self.power_asset_filter = DirectRelationFilter(self, self._view_id.as_property_ref("powerAsset"))
        self.add_steps = BooleanFilter(self, self._view_id.as_property_ref("addSteps"))
        self.scenario_set_filter = DirectRelationFilter(self, self._view_id.as_property_ref("scenarioSet"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
            self.method,
            self.power_asset_filter,
            self.add_steps,
            self.scenario_set_filter,
        ])

    def list_shop_based_partial_bid_configuration(self, limit: int = DEFAULT_QUERY_LIMIT) -> ShopBasedPartialBidConfigurationList:
        return self._list(limit=limit)


class ShopBasedPartialBidConfigurationQuery(_ShopBasedPartialBidConfigurationQuery[ShopBasedPartialBidConfigurationList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ShopBasedPartialBidConfigurationList)
