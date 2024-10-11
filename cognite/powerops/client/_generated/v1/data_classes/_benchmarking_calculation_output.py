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
from ._function_output import FunctionOutput, FunctionOutputWrite

if TYPE_CHECKING:
    from ._alert import Alert, AlertGraphQL, AlertWrite
    from ._benchmarking_calculation_input import BenchmarkingCalculationInput, BenchmarkingCalculationInputGraphQL, BenchmarkingCalculationInputWrite
    from ._benchmarking_result_day_ahead import BenchmarkingResultDayAhead, BenchmarkingResultDayAheadGraphQL, BenchmarkingResultDayAheadWrite


__all__ = [
    "BenchmarkingCalculationOutput",
    "BenchmarkingCalculationOutputWrite",
    "BenchmarkingCalculationOutputApply",
    "BenchmarkingCalculationOutputList",
    "BenchmarkingCalculationOutputWriteList",
    "BenchmarkingCalculationOutputApplyList",
    "BenchmarkingCalculationOutputFields",
    "BenchmarkingCalculationOutputTextFields",
    "BenchmarkingCalculationOutputGraphQL",
]


BenchmarkingCalculationOutputTextFields = Literal["workflow_execution_id", "function_name", "function_call_id"]
BenchmarkingCalculationOutputFields = Literal["workflow_execution_id", "workflow_step", "function_name", "function_call_id"]

_BENCHMARKINGCALCULATIONOUTPUT_PROPERTIES_BY_FIELD = {
    "workflow_execution_id": "workflowExecutionId",
    "workflow_step": "workflowStep",
    "function_name": "functionName",
    "function_call_id": "functionCallId",
}

class BenchmarkingCalculationOutputGraphQL(GraphQLCore):
    """This represents the reading version of benchmarking calculation output, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the benchmarking calculation output.
        data_record: The data record of the benchmarking calculation output node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        function_input: The function input field.
        alerts: An array of calculation level Alerts.
        benchmarking_results: An array of benchmarking shop run results for the day-ahead market.
    """
    view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BenchmarkingCalculationOutput", "1")
    workflow_execution_id: Optional[str] = Field(None, alias="workflowExecutionId")
    workflow_step: Optional[int] = Field(None, alias="workflowStep")
    function_name: Optional[str] = Field(None, alias="functionName")
    function_call_id: Optional[str] = Field(None, alias="functionCallId")
    function_input: Optional[BenchmarkingCalculationInputGraphQL] = Field(default=None, repr=False, alias="functionInput")
    alerts: Optional[list[AlertGraphQL]] = Field(default=None, repr=False)
    benchmarking_results: Optional[list[BenchmarkingResultDayAheadGraphQL]] = Field(default=None, repr=False, alias="benchmarkingResults")

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
    @field_validator("function_input", "alerts", "benchmarking_results", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> BenchmarkingCalculationOutput:
        """Convert this GraphQL format of benchmarking calculation output to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return BenchmarkingCalculationOutput(
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
            function_input=self.function_input.as_read() if isinstance(self.function_input, GraphQLCore) else self.function_input,
            alerts=[alert.as_read() for alert in self.alerts or []],
            benchmarking_results=[benchmarking_result.as_read() for benchmarking_result in self.benchmarking_results or []],
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> BenchmarkingCalculationOutputWrite:
        """Convert this GraphQL format of benchmarking calculation output to the writing format."""
        return BenchmarkingCalculationOutputWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            workflow_execution_id=self.workflow_execution_id,
            workflow_step=self.workflow_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            function_input=self.function_input.as_write() if isinstance(self.function_input, GraphQLCore) else self.function_input,
            alerts=[alert.as_write() for alert in self.alerts or []],
            benchmarking_results=[benchmarking_result.as_write() for benchmarking_result in self.benchmarking_results or []],
        )


class BenchmarkingCalculationOutput(FunctionOutput):
    """This represents the reading version of benchmarking calculation output.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the benchmarking calculation output.
        data_record: The data record of the benchmarking calculation output node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        function_input: The function input field.
        alerts: An array of calculation level Alerts.
        benchmarking_results: An array of benchmarking shop run results for the day-ahead market.
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BenchmarkingCalculationOutput", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "BenchmarkingCalculationOutput")
    benchmarking_results: Optional[list[Union[BenchmarkingResultDayAhead, str, dm.NodeId]]] = Field(default=None, repr=False, alias="benchmarkingResults")

    def as_write(self) -> BenchmarkingCalculationOutputWrite:
        """Convert this read version of benchmarking calculation output to the writing version."""
        return BenchmarkingCalculationOutputWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            workflow_execution_id=self.workflow_execution_id,
            workflow_step=self.workflow_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            function_input=self.function_input.as_write() if isinstance(self.function_input, DomainModel) else self.function_input,
            alerts=[alert.as_write() if isinstance(alert, DomainModel) else alert for alert in self.alerts or []],
            benchmarking_results=[benchmarking_result.as_write() if isinstance(benchmarking_result, DomainModel) else benchmarking_result for benchmarking_result in self.benchmarking_results or []],
        )

    def as_apply(self) -> BenchmarkingCalculationOutputWrite:
        """Convert this read version of benchmarking calculation output to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BenchmarkingCalculationOutputWrite(FunctionOutputWrite):
    """This represents the writing version of benchmarking calculation output.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the benchmarking calculation output.
        data_record: The data record of the benchmarking calculation output node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        function_input: The function input field.
        alerts: An array of calculation level Alerts.
        benchmarking_results: An array of benchmarking shop run results for the day-ahead market.
    """
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "BenchmarkingCalculationOutput", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "BenchmarkingCalculationOutput")
    benchmarking_results: Optional[list[Union[BenchmarkingResultDayAheadWrite, str, dm.NodeId]]] = Field(default=None, repr=False, alias="benchmarkingResults")

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

        if self.function_input is not None:
            properties["functionInput"] = {
                "space":  self.space if isinstance(self.function_input, str) else self.function_input.space,
                "externalId": self.function_input if isinstance(self.function_input, str) else self.function_input.external_id,
            }


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



        edge_type = dm.DirectRelationReference("power_ops_types", "calculationIssue")
        for alert in self.alerts or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=alert,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        edge_type = dm.DirectRelationReference("power_ops_types", "BenchmarkingResultsDayAhead")
        for benchmarking_result in self.benchmarking_results or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=benchmarking_result,
                edge_type=edge_type,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.function_input, DomainModelWrite):
            other_resources = self.function_input._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class BenchmarkingCalculationOutputApply(BenchmarkingCalculationOutputWrite):
    def __new__(cls, *args, **kwargs) -> BenchmarkingCalculationOutputApply:
        warnings.warn(
            "BenchmarkingCalculationOutputApply is deprecated and will be removed in v1.0. Use BenchmarkingCalculationOutputWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "BenchmarkingCalculationOutput.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class BenchmarkingCalculationOutputList(DomainModelList[BenchmarkingCalculationOutput]):
    """List of benchmarking calculation outputs in the read version."""

    _INSTANCE = BenchmarkingCalculationOutput

    def as_write(self) -> BenchmarkingCalculationOutputWriteList:
        """Convert these read versions of benchmarking calculation output to the writing versions."""
        return BenchmarkingCalculationOutputWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> BenchmarkingCalculationOutputWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class BenchmarkingCalculationOutputWriteList(DomainModelWriteList[BenchmarkingCalculationOutputWrite]):
    """List of benchmarking calculation outputs in the writing version."""

    _INSTANCE = BenchmarkingCalculationOutputWrite

class BenchmarkingCalculationOutputApplyList(BenchmarkingCalculationOutputWriteList): ...



def _create_benchmarking_calculation_output_filter(
    view_id: dm.ViewId,
    workflow_execution_id: str | list[str] | None = None,
    workflow_execution_id_prefix: str | None = None,
    min_workflow_step: int | None = None,
    max_workflow_step: int | None = None,
    function_name: str | list[str] | None = None,
    function_name_prefix: str | None = None,
    function_call_id: str | list[str] | None = None,
    function_call_id_prefix: str | None = None,
    function_input: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
    if function_input and isinstance(function_input, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("functionInput"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": function_input}))
    if function_input and isinstance(function_input, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("functionInput"), value={"space": function_input[0], "externalId": function_input[1]}))
    if function_input and isinstance(function_input, list) and isinstance(function_input[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("functionInput"), values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in function_input]))
    if function_input and isinstance(function_input, list) and isinstance(function_input[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("functionInput"), values=[{"space": item[0], "externalId": item[1]} for item in function_input]))
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
