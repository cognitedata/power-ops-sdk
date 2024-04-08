from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field
from pydantic import field_validator, model_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
)

if TYPE_CHECKING:
    from ._commands import Commands, CommandsGraphQL, CommandsWrite
    from ._mapping import Mapping, MappingGraphQL, MappingWrite
    from ._model_template import ModelTemplate, ModelTemplateGraphQL, ModelTemplateWrite


__all__ = [
    "Scenario",
    "ScenarioWrite",
    "ScenarioApply",
    "ScenarioList",
    "ScenarioWriteList",
    "ScenarioApplyList",
    "ScenarioFields",
    "ScenarioTextFields",
]


ScenarioTextFields = Literal["name", "source"]
ScenarioFields = Literal["name", "source"]

_SCENARIO_PROPERTIES_BY_FIELD = {
    "name": "name",
    "source": "source",
}


class ScenarioGraphQL(GraphQLCore, protected_namespaces=()):
    """This represents the reading version of scenario, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the scenario.
        data_record: The data record of the scenario node.
        name: The name of the scenario to run
        model_template: The model template to use when running the scenario
        commands: The commands to run
        source: The source of the scenario
        mappings_override: An array of base mappings to override in shop model file
    """

    view_id = dm.ViewId("sp_powerops_models_temp", "Scenario", "1")
    name: Optional[str] = None
    model_template: Optional[ModelTemplateGraphQL] = Field(None, repr=False, alias="modelTemplate")
    commands: Optional[CommandsGraphQL] = Field(None, repr=False)
    source: Optional[str] = None
    mappings_override: Optional[list[MappingGraphQL]] = Field(default=None, repr=False, alias="mappingsOverride")

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

    @field_validator("model_template", "commands", "mappings_override", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> Scenario:
        """Convert this GraphQL format of scenario to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return Scenario(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            model_template=(
                self.model_template.as_read() if isinstance(self.model_template, GraphQLCore) else self.model_template
            ),
            commands=self.commands.as_read() if isinstance(self.commands, GraphQLCore) else self.commands,
            source=self.source,
            mappings_override=[
                mappings_override.as_read() if isinstance(mappings_override, GraphQLCore) else mappings_override
                for mappings_override in self.mappings_override or []
            ],
        )

    def as_write(self) -> ScenarioWrite:
        """Convert this GraphQL format of scenario to the writing format."""
        return ScenarioWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            model_template=(
                self.model_template.as_write() if isinstance(self.model_template, DomainModel) else self.model_template
            ),
            commands=self.commands.as_write() if isinstance(self.commands, DomainModel) else self.commands,
            source=self.source,
            mappings_override=[
                mappings_override.as_write() if isinstance(mappings_override, DomainModel) else mappings_override
                for mappings_override in self.mappings_override or []
            ],
        )


class Scenario(DomainModel, protected_namespaces=()):
    """This represents the reading version of scenario.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the scenario.
        data_record: The data record of the scenario node.
        name: The name of the scenario to run
        model_template: The model template to use when running the scenario
        commands: The commands to run
        source: The source of the scenario
        mappings_override: An array of base mappings to override in shop model file
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    name: str
    model_template: Union[ModelTemplate, str, dm.NodeId, None] = Field(None, repr=False, alias="modelTemplate")
    commands: Union[Commands, str, dm.NodeId, None] = Field(None, repr=False)
    source: Optional[str] = None
    mappings_override: Union[list[Mapping], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="mappingsOverride"
    )

    def as_write(self) -> ScenarioWrite:
        """Convert this read version of scenario to the writing version."""
        return ScenarioWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            model_template=(
                self.model_template.as_write() if isinstance(self.model_template, DomainModel) else self.model_template
            ),
            commands=self.commands.as_write() if isinstance(self.commands, DomainModel) else self.commands,
            source=self.source,
            mappings_override=[
                mappings_override.as_write() if isinstance(mappings_override, DomainModel) else mappings_override
                for mappings_override in self.mappings_override or []
            ],
        )

    def as_apply(self) -> ScenarioWrite:
        """Convert this read version of scenario to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ScenarioWrite(DomainModelWrite, protected_namespaces=()):
    """This represents the writing version of scenario.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the scenario.
        data_record: The data record of the scenario node.
        name: The name of the scenario to run
        model_template: The model template to use when running the scenario
        commands: The commands to run
        source: The source of the scenario
        mappings_override: An array of base mappings to override in shop model file
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    name: str
    model_template: Union[ModelTemplateWrite, str, dm.NodeId, None] = Field(None, repr=False, alias="modelTemplate")
    commands: Union[CommandsWrite, str, dm.NodeId, None] = Field(None, repr=False)
    source: Optional[str] = None
    mappings_override: Union[list[MappingWrite], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="mappingsOverride"
    )

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(Scenario, dm.ViewId("sp_powerops_models_temp", "Scenario", "1"))

        properties: dict[str, Any] = {}

        if self.name is not None:
            properties["name"] = self.name

        if self.model_template is not None:
            properties["modelTemplate"] = {
                "space": self.space if isinstance(self.model_template, str) else self.model_template.space,
                "externalId": (
                    self.model_template if isinstance(self.model_template, str) else self.model_template.external_id
                ),
            }

        if self.commands is not None:
            properties["commands"] = {
                "space": self.space if isinstance(self.commands, str) else self.commands.space,
                "externalId": self.commands if isinstance(self.commands, str) else self.commands.external_id,
            }

        if self.source is not None or write_none:
            properties["source"] = self.source

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
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

        edge_type = dm.DirectRelationReference("sp_powerops_types_temp", "Mapping")
        for mappings_override in self.mappings_override or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=mappings_override,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.model_template, DomainModelWrite):
            other_resources = self.model_template._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.commands, DomainModelWrite):
            other_resources = self.commands._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class ScenarioApply(ScenarioWrite):
    def __new__(cls, *args, **kwargs) -> ScenarioApply:
        warnings.warn(
            "ScenarioApply is deprecated and will be removed in v1.0. Use ScenarioWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "Scenario.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ScenarioList(DomainModelList[Scenario]):
    """List of scenarios in the read version."""

    _INSTANCE = Scenario

    def as_write(self) -> ScenarioWriteList:
        """Convert these read versions of scenario to the writing versions."""
        return ScenarioWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ScenarioWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ScenarioWriteList(DomainModelWriteList[ScenarioWrite]):
    """List of scenarios in the writing version."""

    _INSTANCE = ScenarioWrite


class ScenarioApplyList(ScenarioWriteList): ...


def _create_scenario_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    model_template: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    source: str | list[str] | None = None,
    source_prefix: str | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if isinstance(name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("name"), value=name))
    if name and isinstance(name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("name"), values=name))
    if name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("name"), value=name_prefix))
    if model_template and isinstance(model_template, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("modelTemplate"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": model_template},
            )
        )
    if model_template and isinstance(model_template, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("modelTemplate"),
                value={"space": model_template[0], "externalId": model_template[1]},
            )
        )
    if model_template and isinstance(model_template, list) and isinstance(model_template[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("modelTemplate"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in model_template],
            )
        )
    if model_template and isinstance(model_template, list) and isinstance(model_template[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("modelTemplate"),
                values=[{"space": item[0], "externalId": item[1]} for item in model_template],
            )
        )
    if commands and isinstance(commands, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("commands"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": commands}
            )
        )
    if commands and isinstance(commands, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("commands"), value={"space": commands[0], "externalId": commands[1]}
            )
        )
    if commands and isinstance(commands, list) and isinstance(commands[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("commands"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in commands],
            )
        )
    if commands and isinstance(commands, list) and isinstance(commands[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("commands"),
                values=[{"space": item[0], "externalId": item[1]} for item in commands],
            )
        )
    if isinstance(source, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("source"), value=source))
    if source and isinstance(source, list):
        filters.append(dm.filters.In(view_id.as_property_ref("source"), values=source))
    if source_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("source"), value=source_prefix))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
