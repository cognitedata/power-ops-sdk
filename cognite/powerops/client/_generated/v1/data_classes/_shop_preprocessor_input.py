from __future__ import annotations

import datetime
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
    TimestampFilter,
)
from cognite.powerops.client._generated.v1.data_classes._function_input import FunctionInput, FunctionInputWrite
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._shop_scenario import ShopScenario, ShopScenarioList, ShopScenarioGraphQL, ShopScenarioWrite, ShopScenarioWriteList


__all__ = [
    "ShopPreprocessorInput",
    "ShopPreprocessorInputWrite",
    "ShopPreprocessorInputList",
    "ShopPreprocessorInputWriteList",
    "ShopPreprocessorInputFields",
    "ShopPreprocessorInputTextFields",
    "ShopPreprocessorInputGraphQL",
]


ShopPreprocessorInputTextFields = Literal["external_id", "workflow_execution_id", "function_name", "function_call_id"]
ShopPreprocessorInputFields = Literal["external_id", "workflow_execution_id", "workflow_step", "function_name", "function_call_id", "start_time", "end_time"]

_SHOPPREPROCESSORINPUT_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "workflow_execution_id": "workflowExecutionId",
    "workflow_step": "workflowStep",
    "function_name": "functionName",
    "function_call_id": "functionCallId",
    "start_time": "startTime",
    "end_time": "endTime",
}


class ShopPreprocessorInputGraphQL(GraphQLCore):
    """This represents the reading version of shop preprocessor input, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop preprocessor input.
        data_record: The data record of the shop preprocessor input node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        scenario: The scenario to run shop with
        start_time: Start date of bid period TODO
        end_time: End date of bid period TODO
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopPreprocessorInput", "1")
    workflow_execution_id: Optional[str] = Field(None, alias="workflowExecutionId")
    workflow_step: Optional[int] = Field(None, alias="workflowStep")
    function_name: Optional[str] = Field(None, alias="functionName")
    function_call_id: Optional[str] = Field(None, alias="functionCallId")
    scenario: Optional[ShopScenarioGraphQL] = Field(default=None, repr=False)
    start_time: Optional[datetime.datetime] = Field(None, alias="startTime")
    end_time: Optional[datetime.datetime] = Field(None, alias="endTime")

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


    @field_validator("scenario", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> ShopPreprocessorInput:
        """Convert this GraphQL format of shop preprocessor input to the reading format."""
        return ShopPreprocessorInput.model_validate(as_read_args(self))

    def as_write(self) -> ShopPreprocessorInputWrite:
        """Convert this GraphQL format of shop preprocessor input to the writing format."""
        return ShopPreprocessorInputWrite.model_validate(as_write_args(self))


class ShopPreprocessorInput(FunctionInput):
    """This represents the reading version of shop preprocessor input.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop preprocessor input.
        data_record: The data record of the shop preprocessor input node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        scenario: The scenario to run shop with
        start_time: Start date of bid period TODO
        end_time: End date of bid period TODO
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopPreprocessorInput", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "ShopPreprocessorInput")
    scenario: Union[ShopScenario, str, dm.NodeId, None] = Field(default=None, repr=False)
    start_time: Optional[datetime.datetime] = Field(None, alias="startTime")
    end_time: Optional[datetime.datetime] = Field(None, alias="endTime")
    @field_validator("scenario", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)


    def as_write(self) -> ShopPreprocessorInputWrite:
        """Convert this read version of shop preprocessor input to the writing version."""
        return ShopPreprocessorInputWrite.model_validate(as_write_args(self))



class ShopPreprocessorInputWrite(FunctionInputWrite):
    """This represents the writing version of shop preprocessor input.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop preprocessor input.
        data_record: The data record of the shop preprocessor input node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        scenario: The scenario to run shop with
        start_time: Start date of bid period TODO
        end_time: End date of bid period TODO
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("end_time", "function_call_id", "function_name", "scenario", "start_time", "workflow_execution_id", "workflow_step",)
    _direct_relations: ClassVar[tuple[str, ...]] = ("scenario",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopPreprocessorInput", "1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "ShopPreprocessorInput")
    scenario: Union[ShopScenarioWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    start_time: Optional[datetime.datetime] = Field(None, alias="startTime")
    end_time: Optional[datetime.datetime] = Field(None, alias="endTime")

    @field_validator("scenario", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class ShopPreprocessorInputList(DomainModelList[ShopPreprocessorInput]):
    """List of shop preprocessor inputs in the read version."""

    _INSTANCE = ShopPreprocessorInput
    def as_write(self) -> ShopPreprocessorInputWriteList:
        """Convert these read versions of shop preprocessor input to the writing versions."""
        return ShopPreprocessorInputWriteList([node.as_write() for node in self.data])


    @property
    def scenario(self) -> ShopScenarioList:
        from ._shop_scenario import ShopScenario, ShopScenarioList
        return ShopScenarioList([item.scenario for item in self.data if isinstance(item.scenario, ShopScenario)])

class ShopPreprocessorInputWriteList(DomainModelWriteList[ShopPreprocessorInputWrite]):
    """List of shop preprocessor inputs in the writing version."""

    _INSTANCE = ShopPreprocessorInputWrite
    @property
    def scenario(self) -> ShopScenarioWriteList:
        from ._shop_scenario import ShopScenarioWrite, ShopScenarioWriteList
        return ShopScenarioWriteList([item.scenario for item in self.data if isinstance(item.scenario, ShopScenarioWrite)])


def _create_shop_preprocessor_input_filter(
    view_id: dm.ViewId,
    workflow_execution_id: str | list[str] | None = None,
    workflow_execution_id_prefix: str | None = None,
    min_workflow_step: int | None = None,
    max_workflow_step: int | None = None,
    function_name: str | list[str] | None = None,
    function_name_prefix: str | None = None,
    function_call_id: str | list[str] | None = None,
    function_call_id_prefix: str | None = None,
    scenario: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    min_start_time: datetime.datetime | None = None,
    max_start_time: datetime.datetime | None = None,
    min_end_time: datetime.datetime | None = None,
    max_end_time: datetime.datetime | None = None,
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
    if isinstance(scenario, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(scenario):
        filters.append(dm.filters.Equals(view_id.as_property_ref("scenario"), value=as_instance_dict_id(scenario)))
    if scenario and isinstance(scenario, Sequence) and not isinstance(scenario, str) and not is_tuple_id(scenario):
        filters.append(dm.filters.In(view_id.as_property_ref("scenario"), values=[as_instance_dict_id(item) for item in scenario]))
    if min_start_time is not None or max_start_time is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("startTime"), gte=min_start_time.isoformat(timespec="milliseconds") if min_start_time else None, lte=max_start_time.isoformat(timespec="milliseconds") if max_start_time else None))
    if min_end_time is not None or max_end_time is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("endTime"), gte=min_end_time.isoformat(timespec="milliseconds") if min_end_time else None, lte=max_end_time.isoformat(timespec="milliseconds") if max_end_time else None))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _ShopPreprocessorInputQuery(NodeQueryCore[T_DomainModelList, ShopPreprocessorInputList]):
    _view_id = ShopPreprocessorInput._view_id
    _result_cls = ShopPreprocessorInput
    _result_list_cls_end = ShopPreprocessorInputList

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
        from ._shop_scenario import _ShopScenarioQuery

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

        if _ShopScenarioQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.scenario = _ShopScenarioQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("scenario"),
                    direction="outwards",
                ),
                connection_name="scenario",
                connection_property=ViewPropertyId(self._view_id, "scenario"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.workflow_execution_id = StringFilter(self, self._view_id.as_property_ref("workflowExecutionId"))
        self.workflow_step = IntFilter(self, self._view_id.as_property_ref("workflowStep"))
        self.function_name = StringFilter(self, self._view_id.as_property_ref("functionName"))
        self.function_call_id = StringFilter(self, self._view_id.as_property_ref("functionCallId"))
        self.scenario_filter = DirectRelationFilter(self, self._view_id.as_property_ref("scenario"))
        self.start_time = TimestampFilter(self, self._view_id.as_property_ref("startTime"))
        self.end_time = TimestampFilter(self, self._view_id.as_property_ref("endTime"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.workflow_execution_id,
            self.workflow_step,
            self.function_name,
            self.function_call_id,
            self.scenario_filter,
            self.start_time,
            self.end_time,
        ])

    def list_shop_preprocessor_input(self, limit: int = DEFAULT_QUERY_LIMIT) -> ShopPreprocessorInputList:
        return self._list(limit=limit)


class ShopPreprocessorInputQuery(_ShopPreprocessorInputQuery[ShopPreprocessorInputList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, ShopPreprocessorInputList)
