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
    IntFilter,
)
from cognite.powerops.client._generated.v1.data_classes._function_output import FunctionOutput, FunctionOutputWrite
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._alert import Alert, AlertList, AlertGraphQL, AlertWrite, AlertWriteList
    from cognite.powerops.client._generated.v1.data_classes._shop_result import ShopResult, ShopResultList, ShopResultGraphQL, ShopResultWrite, ShopResultWriteList
    from cognite.powerops.client._generated.v1.data_classes._shop_trigger_input import ShopTriggerInput, ShopTriggerInputList, ShopTriggerInputGraphQL, ShopTriggerInputWrite, ShopTriggerInputWriteList


__all__ = [
    "ShopTriggerOutput",
    "ShopTriggerOutputWrite",
    "ShopTriggerOutputList",
    "ShopTriggerOutputWriteList",
    "ShopTriggerOutputFields",
    "ShopTriggerOutputTextFields",
    "ShopTriggerOutputGraphQL",
]


ShopTriggerOutputTextFields = Literal["external_id", "workflow_execution_id", "function_name", "function_call_id"]
ShopTriggerOutputFields = Literal["external_id", "workflow_execution_id", "workflow_step", "function_name", "function_call_id"]

_SHOPTRIGGEROUTPUT_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "workflow_execution_id": "workflowExecutionId",
    "workflow_step": "workflowStep",
    "function_name": "functionName",
    "function_call_id": "functionCallId",
}


class ShopTriggerOutputGraphQL(GraphQLCore):
    """This represents the reading version of shop trigger output, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop trigger output.
        data_record: The data record of the shop trigger output node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        function_input: The function input field.
        alerts: An array of calculation level Alerts.
        shop_result: The shop result field.
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopTriggerOutput", "1")
    workflow_execution_id: Optional[str] = Field(None, alias="workflowExecutionId")
    workflow_step: Optional[int] = Field(None, alias="workflowStep")
    function_name: Optional[str] = Field(None, alias="functionName")
    function_call_id: Optional[str] = Field(None, alias="functionCallId")
    function_input: Optional[ShopTriggerInputGraphQL] = Field(default=None, repr=False, alias="functionInput")
    alerts: Optional[list[AlertGraphQL]] = Field(default=None, repr=False)
    shop_result: Optional[ShopResultGraphQL] = Field(default=None, repr=False, alias="shopResult")

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


    @field_validator("function_input", "alerts", "shop_result", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> ShopTriggerOutput:
        """Convert this GraphQL format of shop trigger output to the reading format."""
        return ShopTriggerOutput.model_validate(as_read_args(self))

    def as_write(self) -> ShopTriggerOutputWrite:
        """Convert this GraphQL format of shop trigger output to the writing format."""
        return ShopTriggerOutputWrite.model_validate(as_write_args(self))


class ShopTriggerOutput(FunctionOutput):
    """This represents the reading version of shop trigger output.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop trigger output.
        data_record: The data record of the shop trigger output node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        function_input: The function input field.
        alerts: An array of calculation level Alerts.
        shop_result: The shop result field.
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopTriggerOutput", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "ShopTriggerOutput")
    function_input: Union[ShopTriggerInput, str, dm.NodeId, None] = Field(default=None, repr=False, alias="functionInput")
    shop_result: Union[ShopResult, str, dm.NodeId, None] = Field(default=None, repr=False, alias="shopResult")
    @field_validator("function_input", "shop_result", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)

    @field_validator("alerts", mode="before")
    @classmethod
    def parse_list(cls, value: Any, info: ValidationInfo) -> Any:
        if value is None:
            return None
        return [parse_single_connection(item, info.field_name) for item in value]

    def as_write(self) -> ShopTriggerOutputWrite:
        """Convert this read version of shop trigger output to the writing version."""
        return ShopTriggerOutputWrite.model_validate(as_write_args(self))



class ShopTriggerOutputWrite(FunctionOutputWrite):
    """This represents the writing version of shop trigger output.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop trigger output.
        data_record: The data record of the shop trigger output node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        function_input: The function input field.
        alerts: An array of calculation level Alerts.
        shop_result: The shop result field.
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("function_call_id", "function_input", "function_name", "shop_result", "workflow_execution_id", "workflow_step",)
    _outwards_edges: ClassVar[tuple[tuple[str, dm.DirectRelationReference], ...]] = (("alerts", dm.DirectRelationReference("power_ops_types", "calculationIssue")),)
    _direct_relations: ClassVar[tuple[str, ...]] = ("function_input", "shop_result",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopTriggerOutput", "1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "ShopTriggerOutput")
    function_input: Union[ShopTriggerInputWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="functionInput")
    shop_result: Union[ShopResultWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="shopResult")

    @field_validator("function_input", "shop_result", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class ShopTriggerOutputList(DomainModelList[ShopTriggerOutput]):
    """List of shop trigger outputs in the read version."""

    _INSTANCE = ShopTriggerOutput
    def as_write(self) -> ShopTriggerOutputWriteList:
        """Convert these read versions of shop trigger output to the writing versions."""
        return ShopTriggerOutputWriteList([node.as_write() for node in self.data])


    @property
    def function_input(self) -> ShopTriggerInputList:
        from ._shop_trigger_input import ShopTriggerInput, ShopTriggerInputList
        return ShopTriggerInputList([item.function_input for item in self.data if isinstance(item.function_input, ShopTriggerInput)])
    @property
    def alerts(self) -> AlertList:
        from ._alert import Alert, AlertList
        return AlertList([item for items in self.data for item in items.alerts or [] if isinstance(item, Alert)])

    @property
    def shop_result(self) -> ShopResultList:
        from ._shop_result import ShopResult, ShopResultList
        return ShopResultList([item.shop_result for item in self.data if isinstance(item.shop_result, ShopResult)])

class ShopTriggerOutputWriteList(DomainModelWriteList[ShopTriggerOutputWrite]):
    """List of shop trigger outputs in the writing version."""

    _INSTANCE = ShopTriggerOutputWrite
    @property
    def function_input(self) -> ShopTriggerInputWriteList:
        from ._shop_trigger_input import ShopTriggerInputWrite, ShopTriggerInputWriteList
        return ShopTriggerInputWriteList([item.function_input for item in self.data if isinstance(item.function_input, ShopTriggerInputWrite)])
    @property
    def alerts(self) -> AlertWriteList:
        from ._alert import AlertWrite, AlertWriteList
        return AlertWriteList([item for items in self.data for item in items.alerts or [] if isinstance(item, AlertWrite)])

    @property
    def shop_result(self) -> ShopResultWriteList:
        from ._shop_result import ShopResultWrite, ShopResultWriteList
        return ShopResultWriteList([item.shop_result for item in self.data if isinstance(item.shop_result, ShopResultWrite)])


def _create_shop_trigger_output_filter(
    view_id: dm.ViewId,
    workflow_execution_id: str | list[str] | None = None,
    workflow_execution_id_prefix: str | None = None,
    min_workflow_step: int | None = None,
    max_workflow_step: int | None = None,
    function_name: str | list[str] | None = None,
    function_name_prefix: str | None = None,
    function_call_id: str | list[str] | None = None,
    function_call_id_prefix: str | None = None,
    function_input: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    shop_result: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters: list[dm.Filter] = []
    if isinstance(workflow_execution_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("workflowExecutionId"), value=workflow_execution_id))
    if workflow_execution_id and isinstance(workflow_execution_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("workflowExecutionId"), values=workflow_execution_id))
    if workflow_execution_id_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("workflowExecutionId"), value=workflow_execution_id_prefix))
    if min_workflow_step is not None or max_workflow_step is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("workflowStep"), gte=min_workflow_step, lte=max_workflow_step))
    if isinstance(function_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("functionName"), value=function_name))
    if function_name and isinstance(function_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("functionName"), values=function_name))
    if function_name_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("functionName"), value=function_name_prefix))
    if isinstance(function_call_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("functionCallId"), value=function_call_id))
    if function_call_id and isinstance(function_call_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("functionCallId"), values=function_call_id))
    if function_call_id_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("functionCallId"), value=function_call_id_prefix))
    if isinstance(function_input, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(function_input):
        filters.append(dm.filters.Equals(view_id.as_property_ref("functionInput"), value=as_instance_dict_id(function_input)))
    if function_input and isinstance(function_input, Sequence) and not isinstance(function_input, str) and not is_tuple_id(function_input):
        filters.append(dm.filters.In(view_id.as_property_ref("functionInput"), values=[as_instance_dict_id(item) for item in function_input]))
    if isinstance(shop_result, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(shop_result):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopResult"), value=as_instance_dict_id(shop_result)))
    if shop_result and isinstance(shop_result, Sequence) and not isinstance(shop_result, str) and not is_tuple_id(shop_result):
        filters.append(dm.filters.In(view_id.as_property_ref("shopResult"), values=[as_instance_dict_id(item) for item in shop_result]))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _ShopTriggerOutputQuery(NodeQueryCore[T_DomainModelList, ShopTriggerOutputList]):
    _view_id = ShopTriggerOutput._view_id
    _result_cls = ShopTriggerOutput
    _result_list_cls_end = ShopTriggerOutputList

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
        from ._alert import _AlertQuery
        from ._shop_result import _ShopResultQuery
        from ._shop_trigger_input import _ShopTriggerInputQuery

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

        if _ShopTriggerInputQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.function_input = _ShopTriggerInputQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("functionInput"),
                    direction="outwards",
                ),
                connection_name="function_input",
                connection_property=ViewPropertyId(self._view_id, "functionInput"),
            )

        if _AlertQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.alerts = _AlertQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.EdgeResultSetExpression(
                    direction="outwards",
                    chain_to="destination",
                ),
                connection_name="alerts",
                connection_property=ViewPropertyId(self._view_id, "alerts"),
            )

        if _ShopResultQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.shop_result = _ShopResultQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("shopResult"),
                    direction="outwards",
                ),
                connection_name="shop_result",
                connection_property=ViewPropertyId(self._view_id, "shopResult"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.workflow_execution_id = StringFilter(self, self._view_id.as_property_ref("workflowExecutionId"))
        self.workflow_step = IntFilter(self, self._view_id.as_property_ref("workflowStep"))
        self.function_name = StringFilter(self, self._view_id.as_property_ref("functionName"))
        self.function_call_id = StringFilter(self, self._view_id.as_property_ref("functionCallId"))
        self.function_input_filter = DirectRelationFilter(self, self._view_id.as_property_ref("functionInput"))
        self.shop_result_filter = DirectRelationFilter(self, self._view_id.as_property_ref("shopResult"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.workflow_execution_id,
            self.workflow_step,
            self.function_name,
            self.function_call_id,
            self.function_input_filter,
            self.shop_result_filter,
        ])

    def list_shop_trigger_output(self, limit: int = DEFAULT_QUERY_LIMIT) -> ShopTriggerOutputList:
        return self._list(limit=limit)


class ShopTriggerOutputQuery(_ShopTriggerOutputQuery[ShopTriggerOutputList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ShopTriggerOutputList)
