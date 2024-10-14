from __future__ import annotations

import warnings
from typing import Any, ClassVar, Literal, no_type_check, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import field_validator, model_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
)


__all__ = [
    "ShopCommands",
    "ShopCommandsWrite",
    "ShopCommandsApply",
    "ShopCommandsList",
    "ShopCommandsWriteList",
    "ShopCommandsApplyList",
    "ShopCommandsFields",
    "ShopCommandsTextFields",
    "ShopCommandsGraphQL",
]


ShopCommandsTextFields = Literal["name", "commands"]
ShopCommandsFields = Literal["name", "commands"]

_SHOPCOMMANDS_PROPERTIES_BY_FIELD = {
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> ShopCommands:
        """Convert this GraphQL format of shop command to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ShopCommands(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            commands=self.commands,
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopCommandsWrite:
        """Convert this GraphQL format of shop command to the writing format."""
        return ShopCommandsWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            commands=self.commands,
        )


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
        return ShopCommandsWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            commands=self.commands,
        )

    def as_apply(self) -> ShopCommandsWrite:
        """Convert this read version of shop command to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


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
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopCommands", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "ShopCommands")
    name: str
    commands: list[str]

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

        if self.commands is not None:
            properties["commands"] = self.commands


        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=self._view_id,
                        properties=properties,
                )],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())



        return resources


class ShopCommandsApply(ShopCommandsWrite):
    def __new__(cls, *args, **kwargs) -> ShopCommandsApply:
        warnings.warn(
            "ShopCommandsApply is deprecated and will be removed in v1.0. Use ShopCommandsWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ShopCommands.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ShopCommandsList(DomainModelList[ShopCommands]):
    """List of shop commands in the read version."""

    _INSTANCE = ShopCommands

    def as_write(self) -> ShopCommandsWriteList:
        """Convert these read versions of shop command to the writing versions."""
        return ShopCommandsWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ShopCommandsWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ShopCommandsWriteList(DomainModelWriteList[ShopCommandsWrite]):
    """List of shop commands in the writing version."""

    _INSTANCE = ShopCommandsWrite

class ShopCommandsApplyList(ShopCommandsWriteList): ...



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
