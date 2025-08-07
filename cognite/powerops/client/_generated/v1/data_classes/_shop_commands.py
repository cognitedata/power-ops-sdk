from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Optional, Union

from cognite.client import data_modeling as dm, CogniteClient
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

)


__all__ = [
    "ShopCommands",
    "ShopCommandsWrite",
    "ShopCommandsList",
    "ShopCommandsWriteList",
    "ShopCommandsFields",
    "ShopCommandsTextFields",
    "ShopCommandsGraphQL",
]


ShopCommandsTextFields = Literal["external_id", "name", "commands"]
ShopCommandsFields = Literal["external_id", "name", "commands"]

_SHOPCOMMANDS_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "name": "name",
    "commands": "commands",
}


class ShopCommandsGraphQL(GraphQLCore):
    """This represents the reading version of shop command, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop command.
        data_record: The data record of the shop command node.
        name: Name for the ShopCommands
        commands: The commands used in the shop model file
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopCommands", "1")
    name: Optional[str] = None
    commands: Optional[list[str]] = None

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



    def as_read(self) -> ShopCommands:
        """Convert this GraphQL format of shop command to the reading format."""
        return ShopCommands.model_validate(as_read_args(self))

    def as_write(self) -> ShopCommandsWrite:
        """Convert this GraphQL format of shop command to the writing format."""
        return ShopCommandsWrite.model_validate(as_write_args(self))


class ShopCommands(DomainModel):
    """This represents the reading version of shop command.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop command.
        data_record: The data record of the shop command node.
        name: Name for the ShopCommands
        commands: The commands used in the shop model file
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopCommands", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "ShopCommands")
    name: str
    commands: list[str]


    def as_write(self) -> ShopCommandsWrite:
        """Convert this read version of shop command to the writing version."""
        return ShopCommandsWrite.model_validate(as_write_args(self))



class ShopCommandsWrite(DomainModelWrite):
    """This represents the writing version of shop command.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop command.
        data_record: The data record of the shop command node.
        name: Name for the ShopCommands
        commands: The commands used in the shop model file
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("commands", "name",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopCommands", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "ShopCommands")
    name: str
    commands: list[str]



class ShopCommandsList(DomainModelList[ShopCommands]):
    """List of shop commands in the read version."""

    _INSTANCE = ShopCommands
    def as_write(self) -> ShopCommandsWriteList:
        """Convert these read versions of shop command to the writing versions."""
        return ShopCommandsWriteList([node.as_write() for node in self.data])



class ShopCommandsWriteList(DomainModelWriteList[ShopCommandsWrite]):
    """List of shop commands in the writing version."""

    _INSTANCE = ShopCommandsWrite


def _create_shop_command_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
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
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _ShopCommandsQuery(NodeQueryCore[T_DomainModelList, ShopCommandsList]):
    _view_id = ShopCommands._view_id
    _result_cls = ShopCommands
    _result_list_cls_end = ShopCommandsList

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

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
        ])

    def list_shop_command(self, limit: int = DEFAULT_QUERY_LIMIT) -> ShopCommandsList:
        return self._list(limit=limit)


class ShopCommandsQuery(_ShopCommandsQuery[ShopCommandsList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ShopCommandsList)
