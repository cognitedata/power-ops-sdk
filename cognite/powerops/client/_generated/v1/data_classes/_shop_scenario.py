from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, ClassVar, Literal,  no_type_check, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field
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

if TYPE_CHECKING:
    from ._shop_attribute_mapping import ShopAttributeMapping, ShopAttributeMappingGraphQL, ShopAttributeMappingWrite
    from ._shop_commands import ShopCommands, ShopCommandsGraphQL, ShopCommandsWrite
    from ._shop_model import ShopModel, ShopModelGraphQL, ShopModelWrite
    from ._shop_output_time_series_definition import ShopOutputTimeSeriesDefinition, ShopOutputTimeSeriesDefinitionGraphQL, ShopOutputTimeSeriesDefinitionWrite


__all__ = [
    "ShopScenario",
    "ShopScenarioWrite",
    "ShopScenarioApply",
    "ShopScenarioList",
    "ShopScenarioWriteList",
    "ShopScenarioApplyList",
    "ShopScenarioFields",
    "ShopScenarioTextFields",
    "ShopScenarioGraphQL",
]


ShopScenarioTextFields = Literal["name", "source"]
ShopScenarioFields = Literal["name", "source"]

_SHOPSCENARIO_PROPERTIES_BY_FIELD = {
    "name": "name",
    "source": "source",
}

class ShopScenarioGraphQL(GraphQLCore, protected_namespaces=()):
    """This represents the reading version of shop scenario, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop scenario.
        data_record: The data record of the shop scenario node.
        name: The name of the scenario to run
        model: The model template to use when running the scenario
        commands: The commands to run
        source: The source of the scenario
        output_definition: An array of output definitions for the time series
        attribute_mappings_override: An array of base mappings to override in shop model file
    """
    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopScenario", "1")
    name: Optional[str] = None
    model: Optional[ShopModelGraphQL] = Field(default=None, repr=False)
    commands: Optional[ShopCommandsGraphQL] = Field(default=None, repr=False)
    source: Optional[str] = None
    output_definition: Optional[list[ShopOutputTimeSeriesDefinitionGraphQL]] = Field(default=None, repr=False, alias="outputDefinition")
    attribute_mappings_override: Optional[list[ShopAttributeMappingGraphQL]] = Field(default=None, repr=False, alias="attributeMappingsOverride")

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
    @field_validator("model", "commands", "output_definition", "attribute_mappings_override", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> ShopScenario:
        """Convert this GraphQL format of shop scenario to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ShopScenario(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            model=self.model.as_read() if isinstance(self.model, GraphQLCore) else self.model,
            commands=self.commands.as_read() if isinstance(self.commands, GraphQLCore) else self.commands,
            source=self.source,
            output_definition=[output_definition.as_read() for output_definition in self.output_definition or []],
            attribute_mappings_override=[attribute_mappings_override.as_read() for attribute_mappings_override in self.attribute_mappings_override or []],
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopScenarioWrite:
        """Convert this GraphQL format of shop scenario to the writing format."""
        return ShopScenarioWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            model=self.model.as_write() if isinstance(self.model, GraphQLCore) else self.model,
            commands=self.commands.as_write() if isinstance(self.commands, GraphQLCore) else self.commands,
            source=self.source,
            output_definition=[output_definition.as_write() for output_definition in self.output_definition or []],
            attribute_mappings_override=[attribute_mappings_override.as_write() for attribute_mappings_override in self.attribute_mappings_override or []],
        )


class ShopScenario(DomainModel, protected_namespaces=()):
    """This represents the reading version of shop scenario.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop scenario.
        data_record: The data record of the shop scenario node.
        name: The name of the scenario to run
        model: The model template to use when running the scenario
        commands: The commands to run
        source: The source of the scenario
        output_definition: An array of output definitions for the time series
        attribute_mappings_override: An array of base mappings to override in shop model file
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopScenario", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    name: str
    model: Union[ShopModel, str, dm.NodeId, None] = Field(default=None, repr=False)
    commands: Union[ShopCommands, str, dm.NodeId, None] = Field(default=None, repr=False)
    source: Optional[str] = None
    output_definition: Optional[list[Union[ShopOutputTimeSeriesDefinition, str, dm.NodeId]]] = Field(default=None, repr=False, alias="outputDefinition")
    attribute_mappings_override: Optional[list[Union[ShopAttributeMapping, str, dm.NodeId]]] = Field(default=None, repr=False, alias="attributeMappingsOverride")

    def as_write(self) -> ShopScenarioWrite:
        """Convert this read version of shop scenario to the writing version."""
        return ShopScenarioWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            model=self.model.as_write() if isinstance(self.model, DomainModel) else self.model,
            commands=self.commands.as_write() if isinstance(self.commands, DomainModel) else self.commands,
            source=self.source,
            output_definition=[output_definition.as_write() if isinstance(output_definition, DomainModel) else output_definition for output_definition in self.output_definition or []],
            attribute_mappings_override=[attribute_mappings_override.as_write() if isinstance(attribute_mappings_override, DomainModel) else attribute_mappings_override for attribute_mappings_override in self.attribute_mappings_override or []],
        )

    def as_apply(self) -> ShopScenarioWrite:
        """Convert this read version of shop scenario to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ShopScenarioWrite(DomainModelWrite, protected_namespaces=()):
    """This represents the writing version of shop scenario.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop scenario.
        data_record: The data record of the shop scenario node.
        name: The name of the scenario to run
        model: The model template to use when running the scenario
        commands: The commands to run
        source: The source of the scenario
        output_definition: An array of output definitions for the time series
        attribute_mappings_override: An array of base mappings to override in shop model file
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopScenario", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = None
    name: str
    model: Union[ShopModelWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    commands: Union[ShopCommandsWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    source: Optional[str] = None
    output_definition: Optional[list[Union[ShopOutputTimeSeriesDefinitionWrite, str, dm.NodeId]]] = Field(default=None, repr=False, alias="outputDefinition")
    attribute_mappings_override: Optional[list[Union[ShopAttributeMappingWrite, str, dm.NodeId]]] = Field(default=None, repr=False, alias="attributeMappingsOverride")

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

        if self.model is not None:
            properties["model"] = {
                "space":  self.space if isinstance(self.model, str) else self.model.space,
                "externalId": self.model if isinstance(self.model, str) else self.model.external_id,
            }

        if self.commands is not None:
            properties["commands"] = {
                "space":  self.space if isinstance(self.commands, str) else self.commands.space,
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
                        source=self._view_id,
                        properties=properties,
                )],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())



        edge_type = dm.DirectRelationReference("power_ops_types", "ShopOutputTimeSeriesDefinition")
        for output_definition in self.output_definition or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=output_definition,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("power_ops_types", "ShopAttributeMapping")
        for attribute_mappings_override in self.attribute_mappings_override or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=attribute_mappings_override,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.model, DomainModelWrite):
            other_resources = self.model._to_instances_write(cache)
            resources.extend(other_resources)

        if isinstance(self.commands, DomainModelWrite):
            other_resources = self.commands._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class ShopScenarioApply(ShopScenarioWrite):
    def __new__(cls, *args, **kwargs) -> ShopScenarioApply:
        warnings.warn(
            "ShopScenarioApply is deprecated and will be removed in v1.0. Use ShopScenarioWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ShopScenario.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ShopScenarioList(DomainModelList[ShopScenario]):
    """List of shop scenarios in the read version."""

    _INSTANCE = ShopScenario

    def as_write(self) -> ShopScenarioWriteList:
        """Convert these read versions of shop scenario to the writing versions."""
        return ShopScenarioWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ShopScenarioWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ShopScenarioWriteList(DomainModelWriteList[ShopScenarioWrite]):
    """List of shop scenarios in the writing version."""

    _INSTANCE = ShopScenarioWrite

class ShopScenarioApplyList(ShopScenarioWriteList): ...



def _create_shop_scenario_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    model: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    source: str | list[str] | None = None,
    source_prefix: str | None = None,
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
    if model and isinstance(model, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("model"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": model}))
    if model and isinstance(model, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("model"), value={"space": model[0], "externalId": model[1]}))
    if model and isinstance(model, list) and isinstance(model[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("model"), values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in model]))
    if model and isinstance(model, list) and isinstance(model[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("model"), values=[{"space": item[0], "externalId": item[1]} for item in model]))
    if commands and isinstance(commands, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("commands"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": commands}))
    if commands and isinstance(commands, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("commands"), value={"space": commands[0], "externalId": commands[1]}))
    if commands and isinstance(commands, list) and isinstance(commands[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("commands"), values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in commands]))
    if commands and isinstance(commands, list) and isinstance(commands[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("commands"), values=[{"space": item[0], "externalId": item[1]} for item in commands]))
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
