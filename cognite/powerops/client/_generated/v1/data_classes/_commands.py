from __future__ import annotations

import warnings
from typing import Any, Literal, Optional, Union

from cognite.client import data_modeling as dm

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    ResourcesWrite,
)


__all__ = [
    "Commands",
    "CommandsWrite",
    "CommandsApply",
    "CommandsList",
    "CommandsWriteList",
    "CommandsApplyList",
    "CommandsFields",
    "CommandsTextFields",
]


CommandsTextFields = Literal["commands"]
CommandsFields = Literal["commands"]

_COMMANDS_PROPERTIES_BY_FIELD = {
    "commands": "commands",
}


class Commands(DomainModel):
    """This represents the reading version of command.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the command.
        data_record: The data record of the command node.
        commands: The commands used in the shop model file
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("sp_powerops_types", "Commands")
    commands: Optional[list[str]] = None

    def as_write(self) -> CommandsWrite:
        """Convert this read version of command to the writing version."""
        return CommandsWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            commands=self.commands,
        )

    def as_apply(self) -> CommandsWrite:
        """Convert this read version of command to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class CommandsWrite(DomainModelWrite):
    """This represents the writing version of command.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the command.
        data_record: The data record of the command node.
        commands: The commands used in the shop model file
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("sp_powerops_types", "Commands")
    commands: list[str]

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(Commands, dm.ViewId("sp_powerops_models", "Commands", "1"))

        properties: dict[str, Any] = {}

        if self.commands is not None:
            properties["commands"] = self.commands

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.data_record.existing_version,
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

        return resources


class CommandsApply(CommandsWrite):
    def __new__(cls, *args, **kwargs) -> CommandsApply:
        warnings.warn(
            "CommandsApply is deprecated and will be removed in v1.0. Use CommandsWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Commands.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class CommandsList(DomainModelList[Commands]):
    """List of commands in the read version."""

    _INSTANCE = Commands

    def as_write(self) -> CommandsWriteList:
        """Convert these read versions of command to the writing versions."""
        return CommandsWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> CommandsWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class CommandsWriteList(DomainModelWriteList[CommandsWrite]):
    """List of commands in the writing version."""

    _INSTANCE = CommandsWrite


class CommandsApplyList(CommandsWriteList): ...


def _create_command_filter(
    view_id: dm.ViewId,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
