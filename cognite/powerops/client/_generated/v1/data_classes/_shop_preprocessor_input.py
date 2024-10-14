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
    from ._shop_scenario import ShopScenario, ShopScenarioGraphQL, ShopScenarioWrite


__all__ = [
    "ShopPreprocessorInput",
    "ShopPreprocessorInputWrite",
    "ShopPreprocessorInputApply",
    "ShopPreprocessorInputList",
    "ShopPreprocessorInputWriteList",
    "ShopPreprocessorInputApplyList",
    "ShopPreprocessorInputFields",
    "ShopPreprocessorInputTextFields",
    "ShopPreprocessorInputGraphQL",
]


ShopPreprocessorInputTextFields = Literal["workflow_execution_id", "function_name", "function_call_id"]
ShopPreprocessorInputFields = Literal["workflow_execution_id", "workflow_step", "function_name", "function_call_id", "start_time", "end_time"]

_SHOPPREPROCESSORINPUT_PROPERTIES_BY_FIELD = {
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

    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_read(self) -> ShopPreprocessorInput:
        """Convert this GraphQL format of shop preprocessor input to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ShopPreprocessorInput(
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
            scenario=self.scenario.as_read() if isinstance(self.scenario, GraphQLCore) else self.scenario,
            start_time=self.start_time,
            end_time=self.end_time,
        )


    # We do the ignore argument type as we let pydantic handle the type checking
    @no_type_check
    def as_write(self) -> ShopPreprocessorInputWrite:
        """Convert this GraphQL format of shop preprocessor input to the writing format."""
        return ShopPreprocessorInputWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            workflow_execution_id=self.workflow_execution_id,
            workflow_step=self.workflow_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            scenario=self.scenario.as_write() if isinstance(self.scenario, GraphQLCore) else self.scenario,
            start_time=self.start_time,
            end_time=self.end_time,
        )


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

    def as_write(self) -> ShopPreprocessorInputWrite:
        """Convert this read version of shop preprocessor input to the writing version."""
        return ShopPreprocessorInputWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            workflow_execution_id=self.workflow_execution_id,
            workflow_step=self.workflow_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            scenario=self.scenario.as_write() if isinstance(self.scenario, DomainModel) else self.scenario,
            start_time=self.start_time,
            end_time=self.end_time,
        )

    def as_apply(self) -> ShopPreprocessorInputWrite:
        """Convert this read version of shop preprocessor input to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


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
    _view_id: ClassVar[dm.ViewId] = dm.ViewId("power_ops_core", "ShopPreprocessorInput", "1")

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference("power_ops_types", "ShopPreprocessorInput")
    scenario: Union[ShopScenarioWrite, str, dm.NodeId, None] = Field(default=None, repr=False)
    start_time: Optional[datetime.datetime] = Field(None, alias="startTime")
    end_time: Optional[datetime.datetime] = Field(None, alias="endTime")

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

        if self.scenario is not None:
            properties["scenario"] = {
                "space":  self.space if isinstance(self.scenario, str) else self.scenario.space,
                "externalId": self.scenario if isinstance(self.scenario, str) else self.scenario.external_id,
            }

        if self.start_time is not None or write_none:
            properties["startTime"] = self.start_time.isoformat(timespec="milliseconds") if self.start_time else None

        if self.end_time is not None or write_none:
            properties["endTime"] = self.end_time.isoformat(timespec="milliseconds") if self.end_time else None


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



        if isinstance(self.scenario, DomainModelWrite):
            other_resources = self.scenario._to_instances_write(cache)
            resources.extend(other_resources)

        return resources


class ShopPreprocessorInputApply(ShopPreprocessorInputWrite):
    def __new__(cls, *args, **kwargs) -> ShopPreprocessorInputApply:
        warnings.warn(
            "ShopPreprocessorInputApply is deprecated and will be removed in v1.0. Use ShopPreprocessorInputWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ShopPreprocessorInput.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ShopPreprocessorInputList(DomainModelList[ShopPreprocessorInput]):
    """List of shop preprocessor inputs in the read version."""

    _INSTANCE = ShopPreprocessorInput

    def as_write(self) -> ShopPreprocessorInputWriteList:
        """Convert these read versions of shop preprocessor input to the writing versions."""
        return ShopPreprocessorInputWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ShopPreprocessorInputWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ShopPreprocessorInputWriteList(DomainModelWriteList[ShopPreprocessorInputWrite]):
    """List of shop preprocessor inputs in the writing version."""

    _INSTANCE = ShopPreprocessorInputWrite

class ShopPreprocessorInputApplyList(ShopPreprocessorInputWriteList): ...



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
    scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
    if scenario and isinstance(scenario, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("scenario"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": scenario}))
    if scenario and isinstance(scenario, tuple):
        filters.append(dm.filters.Equals(view_id.as_property_ref("scenario"), value={"space": scenario[0], "externalId": scenario[1]}))
    if scenario and isinstance(scenario, list) and isinstance(scenario[0], str):
        filters.append(dm.filters.In(view_id.as_property_ref("scenario"), values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in scenario]))
    if scenario and isinstance(scenario, list) and isinstance(scenario[0], tuple):
        filters.append(dm.filters.In(view_id.as_property_ref("scenario"), values=[{"space": item[0], "externalId": item[1]} for item in scenario]))
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
