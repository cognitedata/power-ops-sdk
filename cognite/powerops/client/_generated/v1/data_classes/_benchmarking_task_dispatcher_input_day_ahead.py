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
    from cognite.powerops.client._generated.v1.data_classes._benchmarking_configuration_day_ahead import BenchmarkingConfigurationDayAhead, BenchmarkingConfigurationDayAheadList, BenchmarkingConfigurationDayAheadGraphQL, BenchmarkingConfigurationDayAheadWrite, BenchmarkingConfigurationDayAheadWriteList


__all__ = [
    "BenchmarkingTaskDispatcherInputDayAhead",
    "BenchmarkingTaskDispatcherInputDayAheadWrite",
    "BenchmarkingTaskDispatcherInputDayAheadList",
    "BenchmarkingTaskDispatcherInputDayAheadWriteList",
    "BenchmarkingTaskDispatcherInputDayAheadFields",
    "BenchmarkingTaskDispatcherInputDayAheadTextFields",
    "BenchmarkingTaskDispatcherInputDayAheadGraphQL",
]


BenchmarkingTaskDispatcherInputDayAheadTextFields = Literal["external_id", "workflow_execution_id", "function_name", "function_call_id"]
BenchmarkingTaskDispatcherInputDayAheadFields = Literal["external_id", "workflow_execution_id", "workflow_step", "function_name", "function_call_id", "delivery_date"]

_BENCHMARKINGTASKDISPATCHERINPUTDAYAHEAD_PROPERTIES_BY_FIELD = {
    "external_id": "externalId",
    "workflow_execution_id": "workflowExecutionId",
    "workflow_step": "workflowStep",
    "function_name": "functionName",
    "function_call_id": "functionCallId",
    "delivery_date": "deliveryDate",
}


class BenchmarkingTaskDispatcherInputDayAheadGraphQL(GraphQLCore):
    """This represents the reading version of benchmarking task dispatcher input day ahead, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the benchmarking task dispatcher input day ahead.
        data_record: The data record of the benchmarking task dispatcher input day ahead node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        benchmarking_config: The benchmarking config field.
        delivery_date: The timestamp for the delivery date
    """

    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BenchmarkingTaskDispatcherInputDayAhead", "1")
    workflow_execution_id: Optional[str] = Field(None, alias="workflowExecutionId")
    workflow_step: Optional[int] = Field(None, alias="workflowStep")
    function_name: Optional[str] = Field(None, alias="functionName")
    function_call_id: Optional[str] = Field(None, alias="functionCallId")
    benchmarking_config: Optional[BenchmarkingConfigurationDayAheadGraphQL] = Field(default=None, repr=False, alias="benchmarkingConfig")
    delivery_date: Optional[datetime.datetime] = Field(None, alias="deliveryDate")

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


    @field_validator("benchmarking_config", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> BenchmarkingTaskDispatcherInputDayAhead:
        """Convert this GraphQL format of benchmarking task dispatcher input day ahead to the reading format."""
        return BenchmarkingTaskDispatcherInputDayAhead.model_validate(as_read_args(self))

    def as_write(self) -> BenchmarkingTaskDispatcherInputDayAheadWrite:
        """Convert this GraphQL format of benchmarking task dispatcher input day ahead to the writing format."""
        return BenchmarkingTaskDispatcherInputDayAheadWrite.model_validate(as_write_args(self))


class BenchmarkingTaskDispatcherInputDayAhead(FunctionInput):
    """This represents the reading version of benchmarking task dispatcher input day ahead.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the benchmarking task dispatcher input day ahead.
        data_record: The data record of the benchmarking task dispatcher input day ahead node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        benchmarking_config: The benchmarking config field.
        delivery_date: The timestamp for the delivery date
    """

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BenchmarkingTaskDispatcherInputDayAhead", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "BenchmarkingTaskDispatcherInputDayAhead")
    benchmarking_config: Union[BenchmarkingConfigurationDayAhead, str, dm.NodeId, None] = Field(default=None, repr=False, alias="benchmarkingConfig")
    delivery_date: Optional[datetime.datetime] = Field(None, alias="deliveryDate")
    @field_validator("benchmarking_config", mode="before")
    @classmethod
    def parse_single(cls, value: Any, info: ValidationInfo) -> Any:
        return parse_single_connection(value, info.field_name)


    def as_write(self) -> BenchmarkingTaskDispatcherInputDayAheadWrite:
        """Convert this read version of benchmarking task dispatcher input day ahead to the writing version."""
        return BenchmarkingTaskDispatcherInputDayAheadWrite.model_validate(as_write_args(self))



class BenchmarkingTaskDispatcherInputDayAheadWrite(FunctionInputWrite):
    """This represents the writing version of benchmarking task dispatcher input day ahead.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the benchmarking task dispatcher input day ahead.
        data_record: The data record of the benchmarking task dispatcher input day ahead node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        benchmarking_config: The benchmarking config field.
        delivery_date: The timestamp for the delivery date
    """
    _container_fields: ClassVar[tuple[str, ...]] = ("benchmarking_config", "delivery_date", "function_call_id", "function_name", "workflow_execution_id", "workflow_step",)
    _direct_relations: ClassVar[tuple[str, ...]] = ("benchmarking_config",)

    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BenchmarkingTaskDispatcherInputDayAhead", "1")

    node_type: Union[dm.DirectRelationReference, dm.NodeId, tuple[str, str], None] = dm.DirectRelationReference("power_ops_types", "BenchmarkingTaskDispatcherInputDayAhead")
    benchmarking_config: Union[BenchmarkingConfigurationDayAheadWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="benchmarkingConfig")
    delivery_date: Optional[datetime.datetime] = Field(None, alias="deliveryDate")

    @field_validator("benchmarking_config", mode="before")
    def as_node_id(cls, value: Any) -> Any:
        if isinstance(value, dm.DirectRelationReference):
            return dm.NodeId(value.space, value.external_id)
        elif isinstance(value, tuple) and len(value) == 2 and all(isinstance(item, str) for item in value):
            return dm.NodeId(value[0], value[1])
        elif isinstance(value, list):
            return [cls.as_node_id(item) for item in value]
        return value


class BenchmarkingTaskDispatcherInputDayAheadList(DomainModelList[BenchmarkingTaskDispatcherInputDayAhead]):
    """List of benchmarking task dispatcher input day aheads in the read version."""

    _INSTANCE = BenchmarkingTaskDispatcherInputDayAhead
    def as_write(self) -> BenchmarkingTaskDispatcherInputDayAheadWriteList:
        """Convert these read versions of benchmarking task dispatcher input day ahead to the writing versions."""
        return BenchmarkingTaskDispatcherInputDayAheadWriteList([node.as_write() for node in self.data])


    @property
    def benchmarking_config(self) -> BenchmarkingConfigurationDayAheadList:
        from ._benchmarking_configuration_day_ahead import BenchmarkingConfigurationDayAhead, BenchmarkingConfigurationDayAheadList
        return BenchmarkingConfigurationDayAheadList([item.benchmarking_config for item in self.data if isinstance(item.benchmarking_config, BenchmarkingConfigurationDayAhead)])

class BenchmarkingTaskDispatcherInputDayAheadWriteList(DomainModelWriteList[BenchmarkingTaskDispatcherInputDayAheadWrite]):
    """List of benchmarking task dispatcher input day aheads in the writing version."""

    _INSTANCE = BenchmarkingTaskDispatcherInputDayAheadWrite
    @property
    def benchmarking_config(self) -> BenchmarkingConfigurationDayAheadWriteList:
        from ._benchmarking_configuration_day_ahead import BenchmarkingConfigurationDayAheadWrite, BenchmarkingConfigurationDayAheadWriteList
        return BenchmarkingConfigurationDayAheadWriteList([item.benchmarking_config for item in self.data if isinstance(item.benchmarking_config, BenchmarkingConfigurationDayAheadWrite)])


def _create_benchmarking_task_dispatcher_input_day_ahead_filter(
    view_id: dm.ViewId,
    workflow_execution_id: str | list[str] | None = None,
    workflow_execution_id_prefix: str | None = None,
    min_workflow_step: int | None = None,
    max_workflow_step: int | None = None,
    function_name: str | list[str] | None = None,
    function_name_prefix: str | None = None,
    function_call_id: str | list[str] | None = None,
    function_call_id_prefix: str | None = None,
    benchmarking_config: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
    min_delivery_date: datetime.datetime | None = None,
    max_delivery_date: datetime.datetime | None = None,
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
    if isinstance(benchmarking_config, str | dm.NodeId | dm.DirectRelationReference) or is_tuple_id(benchmarking_config):
        filters.append(dm.filters.Equals(view_id.as_property_ref("benchmarkingConfig"), value=as_instance_dict_id(benchmarking_config)))
    if benchmarking_config and isinstance(benchmarking_config, Sequence) and not isinstance(benchmarking_config, str) and not is_tuple_id(benchmarking_config):
        filters.append(dm.filters.In(view_id.as_property_ref("benchmarkingConfig"), values=[as_instance_dict_id(item) for item in benchmarking_config]))
    if min_delivery_date is not None or max_delivery_date is not None:
        filters.append(dm.filters.Range(view_id.as_property_ref("deliveryDate"), gte=min_delivery_date.isoformat(timespec="milliseconds") if min_delivery_date else None, lte=max_delivery_date.isoformat(timespec="milliseconds") if max_delivery_date else None))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None


class _BenchmarkingTaskDispatcherInputDayAheadQuery(NodeQueryCore[T_DomainModelList, BenchmarkingTaskDispatcherInputDayAheadList]):
    _view_id = BenchmarkingTaskDispatcherInputDayAhead._view_id
    _result_cls = BenchmarkingTaskDispatcherInputDayAhead
    _result_list_cls_end = BenchmarkingTaskDispatcherInputDayAheadList

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
        from ._benchmarking_configuration_day_ahead import _BenchmarkingConfigurationDayAheadQuery

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

        if _BenchmarkingConfigurationDayAheadQuery not in created_types and len(creation_path) + 1 < global_config.max_select_depth:
            self.benchmarking_config = _BenchmarkingConfigurationDayAheadQuery(
                created_types.copy(),
                self._creation_path,
                client,
                result_list_cls,
                dm.query.NodeResultSetExpression(
                    through=self._view_id.as_property_ref("benchmarkingConfig"),
                    direction="outwards",
                ),
                connection_name="benchmarking_config",
                connection_property=ViewPropertyId(self._view_id, "benchmarkingConfig"),
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.workflow_execution_id = StringFilter(self, self._view_id.as_property_ref("workflowExecutionId"))
        self.workflow_step = IntFilter(self, self._view_id.as_property_ref("workflowStep"))
        self.function_name = StringFilter(self, self._view_id.as_property_ref("functionName"))
        self.function_call_id = StringFilter(self, self._view_id.as_property_ref("functionCallId"))
        self.benchmarking_config_filter = DirectRelationFilter(self, self._view_id.as_property_ref("benchmarkingConfig"))
        self.delivery_date = TimestampFilter(self, self._view_id.as_property_ref("deliveryDate"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.workflow_execution_id,
            self.workflow_step,
            self.function_name,
            self.function_call_id,
            self.benchmarking_config_filter,
            self.delivery_date,
        ])

    def list_benchmarking_task_dispatcher_input_day_ahead(self, limit: int = DEFAULT_QUERY_LIMIT) -> BenchmarkingTaskDispatcherInputDayAheadList:
        return self._list(limit=limit)


class BenchmarkingTaskDispatcherInputDayAheadQuery(_BenchmarkingTaskDispatcherInputDayAheadQuery[BenchmarkingTaskDispatcherInputDayAheadList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, BenchmarkingTaskDispatcherInputDayAheadList)
