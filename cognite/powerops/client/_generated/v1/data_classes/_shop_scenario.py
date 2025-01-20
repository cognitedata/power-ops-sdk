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

)
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._shop_attribute_mapping import ShopAttributeMapping, ShopAttributeMappingList, ShopAttributeMappingGraphQL, ShopAttributeMappingWrite, ShopAttributeMappingWriteList
    from cognite.powerops.client._generated.v1.data_classes._shop_commands import ShopCommands, ShopCommandsList, ShopCommandsGraphQL, ShopCommandsWrite, ShopCommandsWriteList
    from cognite.powerops.client._generated.v1.data_classes._shop_model import ShopModel, ShopModelList, ShopModelGraphQL, ShopModelWrite, ShopModelWriteList
    from cognite.powerops.client._generated.v1.data_classes._shop_output_time_series_definition import ShopOutputTimeSeriesDefinition, ShopOutputTimeSeriesDefinitionList, ShopOutputTimeSeriesDefinitionGraphQL, ShopOutputTimeSeriesDefinitionWrite, ShopOutputTimeSeriesDefinitionWriteList
    from cognite.powerops.client._generated.v1.data_classes._shop_time_resolution import ShopTimeResolution, ShopTimeResolutionList, ShopTimeResolutionGraphQL, ShopTimeResolutionWrite, ShopTimeResolutionWriteList


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


ShopScenarioTextFields = Literal["external_id", "name", "source"]
ShopScenarioFields = Literal["external_id", "name", "source"]

_SHOPSCENARIO_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
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
        time_resolution: The time resolutions to use within SHOP.
        output_definition: An array of output definitions for the time series
        attribute_mappings_override: An array of base mappings to override in shop model file
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopScenario", "1")
    name: Optional[str] = None
    model: Optional[ShopModelGraphQL] = Field(default=None, repr=False)
    commands: Optional[ShopCommandsGraphQL] = Field(default=None, repr=False)
    source: Optional[str] = None
    time_resolution: Optional[ShopTimeResolutionGraphQL] = Field(default=None, repr=False, alias="timeResolution")
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


    @field_validator("model", "commands", "time_resolution", "output_definition", "attribute_mappings_override", mode="before")
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
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            name=self.name,
            model=self.model.as_read()
if isinstance(self.model, GraphQLCore)
else self.model,
            commands=self.commands.as_read()
if isinstance(self.commands, GraphQLCore)
else self.commands,
            source=self.source,
            time_resolution=self.time_resolution.as_read()
if isinstance(self.time_resolution, GraphQLCore)
else self.time_resolution,
            output_definition=[output_definition.as_read() for output_definition in self.output_definition] if self.output_definition is not None else None,
            attribute_mappings_override=[attribute_mappings_override.as_read() for attribute_mappings_override in self.attribute_mappings_override] if self.attribute_mappings_override is not None else None,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopScenarioWrite:
        """Convert this GraphQL format of shop scenario to the writing format."""
        return ShopScenarioWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            name=self.name,
            model=self.model.as_write()
if isinstance(self.model, GraphQLCore)
else self.model,
            commands=self.commands.as_write()
if isinstance(self.commands, GraphQLCore)
else self.commands,
            source=self.source,
            time_resolution=self.time_resolution.as_write()
if isinstance(self.time_resolution, GraphQLCore)
else self.time_resolution,
            output_definition=[output_definition.as_write() for output_definition in self.output_definition] if self.output_definition is not None else None,
            attribute_mappings_override=[attribute_mappings_override.as_write() for attribute_mappings_override in self.attribute_mappings_override] if self.attribute_mappings_override is not None else None,
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
        time_resolution: The time resolutions to use within SHOP.
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
    time_resolution: Union[ShopTimeResolution, str, dm.NodeId, None] = Field(default=None, repr=False, alias="timeResolution")
    output_definition: Optional[list[Union[ShopOutputTimeSeriesDefinition, str, dm.NodeId]]] = Field(default=None, repr=False, alias="outputDefinition")
    attribute_mappings_override: Optional[list[Union[ShopAttributeMapping, str, dm.NodeId]]] = Field(default=None, repr=False, alias="attributeMappingsOverride")

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopScenarioWrite:
        """Convert this read version of shop scenario to the writing version."""
        return ShopScenarioWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            name=self.name,
            model=self.model.as_write()
if isinstance(self.model, DomainModel)
else self.model,
            commands=self.commands.as_write()
if isinstance(self.commands, DomainModel)
else self.commands,
            source=self.source,
            time_resolution=self.time_resolution.as_write()
if isinstance(self.time_resolution, DomainModel)
else self.time_resolution,
            output_definition=[output_definition.as_write() if isinstance(output_definition, DomainModel) else output_definition for output_definition in self.output_definition] if self.output_definition is not None else None,
            attribute_mappings_override=[attribute_mappings_override.as_write() if isinstance(attribute_mappings_override, DomainModel) else attribute_mappings_override for attribute_mappings_override in self.attribute_mappings_override] if self.attribute_mappings_override is not None else None,
        )

    def as_apply(self) -> ShopScenarioWrite:
        """Convert this read version of shop scenario to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()
    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, ShopScenario],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._shop_attribute_mapping import ShopAttributeMapping
        from ._shop_commands import ShopCommands
        from ._shop_model import ShopModel
        from ._shop_output_time_series_definition import ShopOutputTimeSeriesDefinition
        from ._shop_time_resolution import ShopTimeResolution
        for instance in instances.values():
            if isinstance(instance.model, (dm.NodeId, str)) and (model := nodes_by_id.get(instance.model)) and isinstance(
                    model, ShopModel
            ):
                instance.model = model
            if isinstance(instance.commands, (dm.NodeId, str)) and (commands := nodes_by_id.get(instance.commands)) and isinstance(
                    commands, ShopCommands
            ):
                instance.commands = commands
            if isinstance(instance.time_resolution, (dm.NodeId, str)) and (time_resolution := nodes_by_id.get(instance.time_resolution)) and isinstance(
                    time_resolution, ShopTimeResolution
            ):
                instance.time_resolution = time_resolution
            if edges := edges_by_source_node.get(instance.as_id()):
                output_definition: list[ShopOutputTimeSeriesDefinition | str | dm.NodeId] = []
                attribute_mappings_override: list[ShopAttributeMapping | str | dm.NodeId] = []
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

                    if edge_type == dm.DirectRelationReference("power_ops_types", "ShopOutputTimeSeriesDefinition") and isinstance(
                        value, (ShopOutputTimeSeriesDefinition, str, dm.NodeId)
                    ):
                        output_definition.append(value)
                    if edge_type == dm.DirectRelationReference("power_ops_types", "ShopAttributeMapping") and isinstance(
                        value, (ShopAttributeMapping, str, dm.NodeId)
                    ):
                        attribute_mappings_override.append(value)

                instance.output_definition = output_definition or None
                instance.attribute_mappings_override = attribute_mappings_override or None



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
        time_resolution: The time resolutions to use within SHOP.
        output_definition: An array of output definitions for the time series
        attribute_mappings_override: An array of base mappings to override in shop model file
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopScenario", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    name: str
    model: Union[ShopModelWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    commands: Union[ShopCommandsWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    source: Optional[str] = None
    time_resolution: Union[ShopTimeResolutionWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="timeResolution")
    output_definition: Optional[list[Union[ShopOutputTimeSeriesDefinitionWrite, str, dm.NodeId]]] = Field(default=None, repr=False, alias="outputDefinition")
    attribute_mappings_override: Optional[list[Union[ShopAttributeMappingWrite, str, dm.NodeId]]] = Field(default=None, repr=False, alias="attributeMappingsOverride")

    @field_validator("model", "commands", "time_resolution", "output_definition", "attribute_mappings_override", mode="before")
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

        if self.time_resolution is not None:
            properties["timeResolution"] = {
                "space":  self.space if isinstance(self.time_resolution, str) else self.time_resolution.space,
                "externalId": self.time_resolution if isinstance(self.time_resolution, str) else self.time_resolution.external_id,
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

        if isinstance(self.time_resolution, DomainModelWrite):
            other_resources = self.time_resolution._to_instances_write(cache)
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

    @property
    def model(self) -> ShopModelList:
        from ._shop_model import ShopModel, ShopModelList
        return ShopModelList([item.model for item in self.data if isinstance(item.model, ShopModel)])
    @property
    def commands(self) -> ShopCommandsList:
        from ._shop_commands import ShopCommands, ShopCommandsList
        return ShopCommandsList([item.commands for item in self.data if isinstance(item.commands, ShopCommands)])
    @property
    def time_resolution(self) -> ShopTimeResolutionList:
        from ._shop_time_resolution import ShopTimeResolution, ShopTimeResolutionList
        return ShopTimeResolutionList([item.time_resolution for item in self.data if isinstance(item.time_resolution, ShopTimeResolution)])
    @property
    def output_definition(self) -> ShopOutputTimeSeriesDefinitionList:
        from ._shop_output_time_series_definition import ShopOutputTimeSeriesDefinition, ShopOutputTimeSeriesDefinitionList
        return ShopOutputTimeSeriesDefinitionList([item for items in self.data for item in items.output_definition or [] if isinstance(item, ShopOutputTimeSeriesDefinition)])

    @property
    def attribute_mappings_override(self) -> ShopAttributeMappingList:
        from ._shop_attribute_mapping import ShopAttributeMapping, ShopAttributeMappingList
        return ShopAttributeMappingList([item for items in self.data for item in items.attribute_mappings_override or [] if isinstance(item, ShopAttributeMapping)])


class ShopScenarioWriteList(DomainModelWriteList[ShopScenarioWrite]):
    """List of shop scenarios in the writing version."""

    _INSTANCE = ShopScenarioWrite
    @property
    def model(self) -> ShopModelWriteList:
        from ._shop_model import ShopModelWrite, ShopModelWriteList
        return ShopModelWriteList([item.model for item in self.data if isinstance(item.model, ShopModelWrite)])
    @property
    def commands(self) -> ShopCommandsWriteList:
        from ._shop_commands import ShopCommandsWrite, ShopCommandsWriteList
        return ShopCommandsWriteList([item.commands for item in self.data if isinstance(item.commands, ShopCommandsWrite)])
    @property
    def time_resolution(self) -> ShopTimeResolutionWriteList:
        from ._shop_time_resolution import ShopTimeResolutionWrite, ShopTimeResolutionWriteList
        return ShopTimeResolutionWriteList([item.time_resolution for item in self.data if isinstance(item.time_resolution, ShopTimeResolutionWrite)])
    @property
    def output_definition(self) -> ShopOutputTimeSeriesDefinitionWriteList:
        from ._shop_output_time_series_definition import ShopOutputTimeSeriesDefinitionWrite, ShopOutputTimeSeriesDefinitionWriteList
        return ShopOutputTimeSeriesDefinitionWriteList([item for items in self.data for item in items.output_definition or [] if isinstance(item, ShopOutputTimeSeriesDefinitionWrite)])

    @property
    def attribute_mappings_override(self) -> ShopAttributeMappingWriteList:
        from ._shop_attribute_mapping import ShopAttributeMappingWrite, ShopAttributeMappingWriteList
        return ShopAttributeMappingWriteList([item for items in self.data for item in items.attribute_mappings_override or [] if isinstance(item, ShopAttributeMappingWrite)])


class ShopScenarioApplyList(ShopScenarioWriteList): ...


def _create_shop_scenario_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    model: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    commands: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    source: str | list[str] | None = None,
    source_prefix: str | None = None,
    time_resolution: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
    if isinstance(model, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(model):
        filters.append(dm.filters.Equals(view_id.as_property_ref("model"), value=as_instance_dict_id(model)))
    if model and isinstance(model, Sequence) and not isinstance(model, str) and not is_tuple_id(model):
        filters.append(dm.filters.In(view_id.as_property_ref("model"), values=[as_instance_dict_id(item) for item in model]))
    if isinstance(commands, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(commands):
        filters.append(dm.filters.Equals(view_id.as_property_ref("commands"), value=as_instance_dict_id(commands)))
    if commands and isinstance(commands, Sequence) and not isinstance(commands, str) and not is_tuple_id(commands):
        filters.append(dm.filters.In(view_id.as_property_ref("commands"), values=[as_instance_dict_id(item) for item in commands]))
    if isinstance(source, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("source"), value=source))
    if source and isinstance(source, list):
        filters.append(dm.filters.In(view_id.as_property_ref("source"), values=source))
    if source_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("source"), value=source_prefix))
    if isinstance(time_resolution, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(time_resolution):
        filters.append(dm.filters.Equals(view_id.as_property_ref("timeResolution"), value=as_instance_dict_id(time_resolution)))
    if time_resolution and isinstance(time_resolution, Sequence) and not isinstance(time_resolution, str) and not is_tuple_id(time_resolution):
        filters.append(dm.filters.In(view_id.as_property_ref("timeResolution"), values=[as_instance_dict_id(item) for item in time_resolution]))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _ShopScenarioQuery(NodeQueryCore[T_DomainModelList, ShopScenarioList]):
    _view_id = ShopScenario._view_id
    _result_cls = ShopScenario
    _result_list_cls_end = ShopScenarioList

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
        from ._shop_attribute_mapping import _ShopAttributeMappingQuery
        from ._shop_commands import _ShopCommandsQuery
        from ._shop_model import _ShopModelQuery
        from ._shop_output_time_series_definition import _ShopOutputTimeSeriesDefinitionQuery
        from ._shop_time_resolution import _ShopTimeResolutionQuery

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

        if _ShopModelQuery not in created_types:
            self.model = _ShopModelQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("model"),
                    direction="outwards",
                ),
                connection_name="model",
            )

        if _ShopCommandsQuery not in created_types:
            self.commands = _ShopCommandsQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("commands"),
                    direction="outwards",
                ),
                connection_name="commands",
            )

        if _ShopTimeResolutionQuery not in created_types:
            self.time_resolution = _ShopTimeResolutionQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("timeResolution"),
                    direction="outwards",
                ),
                connection_name="time_resolution",
            )

        if _ShopOutputTimeSeriesDefinitionQuery not in created_types:
            self.output_definition = _ShopOutputTimeSeriesDefinitionQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="output_definition",
            )

        if _ShopAttributeMappingQuery not in created_types:
            self.attribute_mappings_override = _ShopAttributeMappingQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="attribute_mappings_override",
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.source = StringFilter(self, self._view_id.as_property_ref("source"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
            self.source,
        ])

    def list_shop_scenario(self, limit: int = DEFAULT_QUERY_LIMIT) -> ShopScenarioList:
        return self._list(limit=limit)


class ShopScenarioQuery(_ShopScenarioQuery[ShopScenarioList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ShopScenarioList)
