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
    DirectRelationFilter,
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
    "ShopScenarioList",
    "ShopScenarioWriteList",
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
        time_resolution: The time resolutions to use within SHOP.
        source: The source of the scenario
        output_definition: An array of output definitions for the time series
        attribute_mappings_override: An array of base mappings to override in shop model file
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopScenario", "1")
    name: Optional[str] = None
    model: Optional[ShopModelGraphQL] = Field(default=None, repr=False)
    commands: Optional[ShopCommandsGraphQL] = Field(default=None, repr=False)
    time_resolution: Optional[ShopTimeResolutionGraphQL] = Field(default=None, repr=False, alias="timeResolution")
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


    @field_validator("model", "commands", "time_resolution", "output_definition", "attribute_mappings_override", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> ShopScenario:
        """Convert this GraphQL format of shop scenario to the reading format."""
        return ShopScenario.model_validate(as_read_args(self))

    def as_write(self) -> ShopScenarioWrite:
        """Convert this GraphQL format of shop scenario to the writing format."""
        return ShopScenarioWrite.model_validate(as_write_args(self))


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
        time_resolution: The time resolutions to use within SHOP.
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
    time_resolution: Union[ShopTimeResolution, str, dm.NodeId, None] = Field(default=None, repr=False, alias="timeResolution")
    source: Optional[str] = None
    output_definition: Optional[list[Union[ShopOutputTimeSeriesDefinition, str, dm.NodeId]]] = Field(default=None, repr=False, alias="outputDefinition")
    attribute_mappings_override: Optional[list[Union[ShopAttributeMapping, str, dm.NodeId]]] = Field(default=None, repr=False, alias="attributeMappingsOverride")
    @field_validator("model", "commands", "time_resolution", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    @field_validator("output_definition", "attribute_mappings_override", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> ShopScenarioWrite:
        """Convert this read version of shop scenario to the writing version."""
        return ShopScenarioWrite.model_validate(as_write_args(self))



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
        time_resolution: The time resolutions to use within SHOP.
        source: The source of the scenario
        output_definition: An array of output definitions for the time series
        attribute_mappings_override: An array of base mappings to override in shop model file
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("commands", "model", "name", "source", "time_resolution",)
    _outwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (("attribute_mappings_override", dm.DirectRelationReference("power_ops_types", "ShopAttributeMapping")), ("output_definition", dm.DirectRelationReference("power_ops_types", "ShopOutputTimeSeriesDefinition")),)
    _direct_relations: ClassVar[tuple[str, ...]] = ("commands", "model", "time_resolution",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopScenario", "1")

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = None
    name: str
    model: Union[ShopModelWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    commands: Union[ShopCommandsWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    time_resolution: Union[ShopTimeResolutionWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="timeResolution")
    source: Optional[str] = None
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


class ShopScenarioList(DomainModelList[ShopScenario]):
    """List of shop scenarios in the read version."""

    _INSTANCE = ShopScenario
    def as_write(self) -> ShopScenarioWriteList:
        """Convert these read versions of shop scenario to the writing versions."""
        return ShopScenarioWriteList([node.as_write() for node in self.data])


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



def _create_shop_scenario_filter(
    view_id: dm.ViewId,
    name: str | list[str] | None = None,
    name_prefix: str | None = None,
    model: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    commands: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    time_resolution: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
    if isinstance(model, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(model):
        filters.append(dm.filters.Equals(view_id.as_property_ref("model"), value=as_instance_dict_id(model)))
    if model and isinstance(model, Sequence) and not isinstance(model, str) and not is_tuple_id(model):
        filters.append(dm.filters.In(view_id.as_property_ref("model"), values=[as_instance_dict_id(item) for item in model]))
    if isinstance(commands, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(commands):
        filters.append(dm.filters.Equals(view_id.as_property_ref("commands"), value=as_instance_dict_id(commands)))
    if commands and isinstance(commands, Sequence) and not isinstance(commands, str) and not is_tuple_id(commands):
        filters.append(dm.filters.In(view_id.as_property_ref("commands"), values=[as_instance_dict_id(item) for item in commands]))
    if isinstance(time_resolution, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(time_resolution):
        filters.append(dm.filters.Equals(view_id.as_property_ref("timeResolution"), value=as_instance_dict_id(time_resolution)))
    if time_resolution and isinstance(time_resolution, Sequence) and not isinstance(time_resolution, str) and not is_tuple_id(time_resolution):
        filters.append(dm.filters.In(view_id.as_property_ref("timeResolution"), values=[as_instance_dict_id(item) for item in time_resolution]))
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
        expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.NodeOrEdgeResultSetExpression | None = None,
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
            connection_property,
            connection_type,
            reverse_expression,
        )

        if _ShopModelQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "model"),
            )

        if _ShopCommandsQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "commands"),
            )

        if _ShopTimeResolutionQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "timeResolution"),
            )

        if _ShopOutputTimeSeriesDefinitionQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "outputDefinition"),
            )

        if _ShopAttributeMappingQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
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
                connection_property=ViewPropertyId(self._view_id, "attributeMappingsOverride"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.name = StringFilter(self, self._view_id.as_property_ref("name"))
        self.model_filter = DirectRelationFilter(self, self._view_id.as_property_ref("model"))
        self.commands_filter = DirectRelationFilter(self, self._view_id.as_property_ref("commands"))
        self.time_resolution_filter = DirectRelationFilter(self, self._view_id.as_property_ref("timeResolution"))
        self.source = StringFilter(self, self._view_id.as_property_ref("source"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.name,
            self.model_filter,
            self.commands_filter,
            self.time_resolution_filter,
            self.source,
        ])

    def list_shop_scenario(self, limit: int = DEFAULT_QUERY_LIMIT) -> ShopScenarioList:
        return self._list(limit=limit)


class ShopScenarioQuery(_ShopScenarioQuery[ShopScenarioList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ShopScenarioList)
