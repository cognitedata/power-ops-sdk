from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal,  no_type_check, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
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
    BooleanFilter,
)
from cognite.powerops.client._generated.v1.data_classes._partial_bid_configuration import PartialBidConfiguration, PartialBidConfigurationWrite
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._power_asset import PowerAsset, PowerAssetList, PowerAssetGraphQL, PowerAssetWrite, PowerAssetWriteList
    from cognite.powerops.client._generated.v1.data_classes._shop_scenario_set import ShopScenarioSet, ShopScenarioSetList, ShopScenarioSetGraphQL, ShopScenarioSetWrite, ShopScenarioSetWriteList


__all__ = [
    "ShopBasedPartialBidConfiguration",
    "ShopBasedPartialBidConfigurationWrite",
    "ShopBasedPartialBidConfigurationApply",
    "ShopBasedPartialBidConfigurationList",
    "ShopBasedPartialBidConfigurationWriteList",
    "ShopBasedPartialBidConfigurationApplyList",
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> ShopBasedPartialBidConfiguration:
        """Convert this GraphQL format of shop based partial bid configuration to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ShopBasedPartialBidConfiguration(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            method=self.method,
            power_asset=self.power_asset.as_read()
if isinstance(self.power_asset, GraphQLCore)
else self.power_asset,
            add_steps=self.add_steps,
            scenario_set=self.scenario_set.as_read()
if isinstance(self.scenario_set, GraphQLCore)
else self.scenario_set,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopBasedPartialBidConfigurationWrite:
        """Convert this GraphQL format of shop based partial bid configuration to the writing format."""
        return ShopBasedPartialBidConfigurationWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            method=self.method,
            power_asset=self.power_asset.as_write()
if isinstance(self.power_asset, GraphQLCore)
else self.power_asset,
            add_steps=self.add_steps,
            scenario_set=self.scenario_set.as_write()
if isinstance(self.scenario_set, GraphQLCore)
else self.scenario_set,
        )


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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopBasedPartialBidConfigurationWrite:
        """Convert this read version of shop based partial bid configuration to the writing version."""
        return ShopBasedPartialBidConfigurationWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            method=self.method,
            power_asset=self.power_asset.as_write()
if isinstance(self.power_asset, DomainModel)
else self.power_asset,
            add_steps=self.add_steps,
            scenario_set=self.scenario_set.as_write()
if isinstance(self.scenario_set, DomainModel)
else self.scenario_set,
        )

    def as_apply(self) -> ShopBasedPartialBidConfigurationWrite:
        """Convert this read version of shop based partial bid configuration to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()
    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, ShopBasedPartialBidConfiguration],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._power_asset import PowerAsset
        from ._shop_scenario_set import ShopScenarioSet
        for instance in instances.values():
            if isinstance(instance.power_asset, (dm.NodeId, str)) and (power_asset := nodes_by_id.get(instance.power_asset)) and isinstance(
                    power_asset, PowerAsset
            ):
                instance.power_asset = power_asset
            if isinstance(instance.scenario_set, (dm.NodeId, str)) and (scenario_set := nodes_by_id.get(instance.scenario_set)) and isinstance(
                    scenario_set, ShopScenarioSet
            ):
                instance.scenario_set = scenario_set


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

        if self.method is not None or write_none:
            properties["method"] = self.method

        if self.power_asset is not None:
            properties["powerAsset"] = {
                "space":  self.space if isinstance(self.power_asset, str) else self.power_asset.space,
                "externalId": self.power_asset if isinstance(self.power_asset, str) else self.power_asset.external_id,
            }

        if self.add_steps is not None:
            properties["addSteps"] = self.add_steps

        if self.scenario_set is not None:
            properties["scenarioSet"] = {
                "space":  self.space if isinstance(self.scenario_set, str) else self.scenario_set.space,
                "externalId": self.scenario_set if isinstance(self.scenario_set, str) else self.scenario_set.external_id,
            }

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

        if isinstance(self.power_asset, DomainModelWrite):
            other_resources = self.power_asset._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.scenario_set, DomainModelWrite):
            other_resources = self.scenario_set._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class ShopBasedPartialBidConfigurationApply(ShopBasedPartialBidConfigurationWrite):
    def __new__(cls, *args, **kwargs) -> ShopBasedPartialBidConfigurationApply:
        warnings.warn(
            "ShopBasedPartialBidConfigurationApply is deprecated and will be removed in v1.0. Use ShopBasedPartialBidConfigurationWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ShopBasedPartialBidConfiguration.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)

class ShopBasedPartialBidConfigurationList(DomainModelList[ShopBasedPartialBidConfiguration]):
    """List of shop based partial bid configurations in the read version."""

    _INSTANCE = ShopBasedPartialBidConfiguration
    def as_write(self) -> ShopBasedPartialBidConfigurationWriteList:
        """Convert these read versions of shop based partial bid configuration to the writing versions."""
        return ShopBasedPartialBidConfigurationWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ShopBasedPartialBidConfigurationWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

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

class ShopBasedPartialBidConfigurationApplyList(ShopBasedPartialBidConfigurationWriteList): ...


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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
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
            connection_type,
            reverse_expression,
        )

        if _PowerAssetQuery not in created_types:
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
            )

        if _ShopScenarioSetQuery not in created_types:
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
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.method = StringFilter(self, self._view_id.as_property_ref("method"))
        self.add_steps = BooleanFilter(self, self._view_id.as_property_ref("addSteps"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
            self.method,
            self.add_steps,
        ])

    def list_shop_based_partial_bid_configuration(self, limit: int = DEFAULT_QUERY_LIMIT) -> ShopBasedPartialBidConfigurationList:
        return self._list(limit=limit)


class ShopBasedPartialBidConfigurationQuery(_ShopBasedPartialBidConfigurationQuery[ShopBasedPartialBidConfigurationList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ShopBasedPartialBidConfigurationList)
