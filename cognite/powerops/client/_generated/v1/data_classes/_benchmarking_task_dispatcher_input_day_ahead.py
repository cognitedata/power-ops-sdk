from __future__ import annotations

import datetime
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
from ._function_input import FunctionInput, FunctionInputWrite

if TYPE_CHECKING:
    from ._benchmarking_configuration_day_ahead import BenchmarkingConfigurationDayAhead, BenchmarkingConfigurationDayAheadGraphQL, BenchmarkingConfigurationDayAheadWrite


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


BenchmarkingTaskDispatcherInputDayAheadTextFields = Literal["workflow_execution_id", "function_name", "function_call_id"]
BenchmarkingTaskDispatcherInputDayAheadFields = Literal["workflow_execution_id", "workflow_step", "function_name", "function_call_id", "delivery_date"]

_BENCHMARKINGTASKDISPATCHERINPUTDAYAHEAD_PROPERTIES_BY_FIELD = {
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
            space=self.space or DEFAULT_INSTANCE_SPACE,
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
            benchmarking_config=self.benchmarking_config.as_read() if isinstance(self.benchmarking_config, GraphQLCore) else self.benchmarking_config,
            delivery_date=self.delivery_date,
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> BenchmarkingTaskDispatcherInputDayAheadWrite:
        """Convert this GraphQL format of benchmarking task dispatcher input day ahead to the writing format."""
        return BenchmarkingTaskDispatcherInputDayAheadWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            workflow_execution_id=self.workflow_execution_id,
            workflow_step=self.workflow_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            benchmarking_config=self.benchmarking_config.as_write() if isinstance(self.benchmarking_config, GraphQLCore) else self.benchmarking_config,
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
            benchmarking_config=self.benchmarking_config.as_write() if isinstance(self.benchmarking_config, DomainModel) else self.benchmarking_config,
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

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "BenchmarkingTaskDispatcherInputDayAhead")
    benchmarking_config: Union[BenchmarkingConfigurationDayAheadWrite, str, dm.NodeId, None] = Field(default=None, repr=False, alias="benchmarkingConfig")
    delivery_date: Optional[datetime.datetime] = Field(None, alias="deliveryDate")

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
                type=self.node_type,
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


class BenchmarkingTaskDispatcherInputDayAheadWriteList(DomainModelWriteList[BenchmarkingTaskDispatcherInputDayAheadWrite]):
    """List of benchmarking task dispatcher input day aheads in the writing version."""

    _INSTANCE = BenchmarkingTaskDispatcherInputDayAheadWrite

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
    benchmarking_config: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
    if benchmarking_config and isinstance(benchmarking_config, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("benchmarkingConfig"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": benchmarking_config}))
    if benchmarking_config and isinstance(benchmarking_config, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("benchmarkingConfig"), value={"space": benchmarking_config[0], "externalId": benchmarking_config[1]}))
    if benchmarking_config and isinstance(benchmarking_config, list) and isinstance(benchmarking_config[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("benchmarkingConfig"), values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in benchmarking_config]))
    if benchmarking_config and isinstance(benchmarking_config, list) and isinstance(benchmarking_config[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("benchmarkingConfig"), values=[{"space": item[0], "externalId": item[1]} for item in benchmarking_config]))
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
