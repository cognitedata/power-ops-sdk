from __future__ import annotations

import datetime
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
    IntFilter,
    TimestampFilter,
)
from cognite.powerops.client._generated.v1.data_classes._function_input import FunctionInput, FunctionInputWrite
if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1.data_classes._benchmarking_configuration_day_ahead import BenchmarkingConfigurationDayAhead, BenchmarkingConfigurationDayAheadList, BenchmarkingConfigurationDayAheadGraphQL, BenchmarkingConfigurationDayAheadWrite, BenchmarkingConfigurationDayAheadWriteList


__all__ = [
    "BenchmarkingTaskDispatcherInputDayAhead",
    "BenchmarkingTaskDispatcherInputDayAheadWrite",
    "BenchmarkingTaskDispatcherInputDayAheadApply",
    "BenchmarkingTaskDispatcherInputDayAheadList",
    "BenchmarkingTaskDispatcherInputDayAheadWriteList",
    "BenchmarkingTaskDispatcherInputDayAheadApplyList",
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> BenchmarkingTaskDispatcherInputDayAhead:
        """Convert this GraphQL format of benchmarking task dispatcher input day ahead to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return BenchmarkingTaskDispatcherInputDayAhead(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            workflow_execution_id=self.workflow_execution_id,
            workflow_step=self.workflow_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            benchmarking_config=self.benchmarking_config.as_read()
if isinstance(self.benchmarking_config, GraphQLCore)
else self.benchmarking_config,
            delivery_date=self.delivery_date,
        )

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> BenchmarkingTaskDispatcherInputDayAheadWrite:
        """Convert this GraphQL format of benchmarking task dispatcher input day ahead to the writing format."""
        return BenchmarkingTaskDispatcherInputDayAheadWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            workflow_execution_id=self.workflow_execution_id,
            workflow_step=self.workflow_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            benchmarking_config=self.benchmarking_config.as_write()
if isinstance(self.benchmarking_config, GraphQLCore)
else self.benchmarking_config,
            delivery_date=self.delivery_date,
        )


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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> BenchmarkingTaskDispatcherInputDayAheadWrite:
        """Convert this read version of benchmarking task dispatcher input day ahead to the writing version."""
        return BenchmarkingTaskDispatcherInputDayAheadWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            workflow_execution_id=self.workflow_execution_id,
            workflow_step=self.workflow_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            benchmarking_config=self.benchmarking_config.as_write()
if isinstance(self.benchmarking_config, DomainModel)
else self.benchmarking_config,
            delivery_date=self.delivery_date,
        )

    def as_apply(self) -> BenchmarkingTaskDispatcherInputDayAheadWrite:
        """Convert this read version of benchmarking task dispatcher input day ahead to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()
    @classmethod
    def _update_connections(
        cls,
        instances: dict[dm.NodeId | str, BenchmarkingTaskDispatcherInputDayAhead],  # type: ignore[override]
        nodes_by_id: dict[dm.NodeId | str, DomainModel],
        edges_by_source_node: dict[dm.NodeId, list[dm.Edge | DomainRelation]],
    ) -> None:
        from ._benchmarking_configuration_day_ahead import BenchmarkingConfigurationDayAhead
        for instance in instances.values():
            if isinstance(instance.benchmarking_config, (dm.NodeId, str)) and (benchmarking_config := nodes_by_id.get(instance.benchmarking_config)) and isinstance(
                    benchmarking_config, BenchmarkingConfigurationDayAhead
            ):
                instance.benchmarking_config = benchmarking_config


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

        if self.workflow_execution_id is not None:
            properties["workflowExecutionId"] = self.workflow_execution_id

        if self.workflow_step is not None:
            properties["workflowStep"] = self.workflow_step

        if self.function_name is not None:
            properties["functionName"] = self.function_name

        if self.function_call_id is not None:
            properties["functionCallId"] = self.function_call_id

        if self.benchmarking_config is not None:
            properties["benchmarkingConfig"] = {
                "space":  self.space if isinstance(self.benchmarking_config, str) else self.benchmarking_config.space,
                "externalId": self.benchmarking_config if isinstance(self.benchmarking_config, str) else self.benchmarking_config.external_id,
            }

        if self.delivery_date is not None or write_none:
            properties["deliveryDate"] = self.delivery_date.isoformat(timespec="milliseconds") if self.delivery_date else None

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

        if isinstance(self.benchmarking_config, DomainModelWrite):
            other_resources = self.benchmarking_config._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class BenchmarkingTaskDispatcherInputDayAheadApply(BenchmarkingTaskDispatcherInputDayAheadWrite):
    def __new__(cls, *args, **kwargs) -> BenchmarkingTaskDispatcherInputDayAheadApply:
        warnings.warn(
            "BenchmarkingTaskDispatcherInputDayAheadApply is deprecated and will be removed in v1.0. Use BenchmarkingTaskDispatcherInputDayAheadWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "BenchmarkingTaskDispatcherInputDayAhead.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)

class BenchmarkingTaskDispatcherInputDayAheadList(DomainModelList[BenchmarkingTaskDispatcherInputDayAhead]):
    """List of benchmarking task dispatcher input day aheads in the read version."""

    _INSTANCE = BenchmarkingTaskDispatcherInputDayAhead
    def as_write(self) -> BenchmarkingTaskDispatcherInputDayAheadWriteList:
        """Convert these read versions of benchmarking task dispatcher input day ahead to the writing versions."""
        return BenchmarkingTaskDispatcherInputDayAheadWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> BenchmarkingTaskDispatcherInputDayAheadWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()

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

class BenchmarkingTaskDispatcherInputDayAheadApplyList(BenchmarkingTaskDispatcherInputDayAheadWriteList): ...


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
        expression: dm.query.ResultSetExpression | None = None,
        connection_name: str | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
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
            connection_type,
            reverse_expression,
        )

        if _BenchmarkingConfigurationDayAheadQuery not in created_types:
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
            )

        self.space = StringFilter(self, ["node", "space"])
        self.external_id = StringFilter(self, ["node", "externalId"])
        self.workflow_execution_id = StringFilter(self, self._view_id.as_property_ref("workflowExecutionId"))
        self.workflow_step = IntFilter(self, self._view_id.as_property_ref("workflowStep"))
        self.function_name = StringFilter(self, self._view_id.as_property_ref("functionName"))
        self.function_call_id = StringFilter(self, self._view_id.as_property_ref("functionCallId"))
        self.delivery_date = TimestampFilter(self, self._view_id.as_property_ref("deliveryDate"))
        self._filter_classes.extend([
            self.space,
            self.external_id,
            self.workflow_execution_id,
            self.workflow_step,
            self.function_name,
            self.function_call_id,
            self.delivery_date,
        ])

    def list_benchmarking_task_dispatcher_input_day_ahead(self, limit: int = DEFAULT_QUERY_LIMIT) -> BenchmarkingTaskDispatcherInputDayAheadList:
        return self._list(limit=limit)


class BenchmarkingTaskDispatcherInputDayAheadQuery(_BenchmarkingTaskDispatcherInputDayAheadQuery[BenchmarkingTaskDispatcherInputDayAheadList]):
    def __init__(self, client: CogniteClient):
        super().__init__(set(), [], client, BenchmarkingTaskDispatcherInputDayAheadList)
