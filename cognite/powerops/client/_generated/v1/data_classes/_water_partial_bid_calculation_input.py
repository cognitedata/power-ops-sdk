from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field
from pydantic import field_validator, model_validator

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordGraphQL,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    GraphQLCore,
    ResourcesWrite,
)

if TYPE_CHECKING:
    from ._bid_calculation_task import BidCalculationTask, BidCalculationTaskGraphQL, BidCalculationTaskWrite


__all__ = [
    "WaterPartialBidCalculationInput",
    "WaterPartialBidCalculationInputWrite",
    "WaterPartialBidCalculationInputApply",
    "WaterPartialBidCalculationInputList",
    "WaterPartialBidCalculationInputWriteList",
    "WaterPartialBidCalculationInputApplyList",
    "WaterPartialBidCalculationInputFields",
    "WaterPartialBidCalculationInputTextFields",
]


WaterPartialBidCalculationInputTextFields = Literal["process_id", "function_name", "function_call_id"]
WaterPartialBidCalculationInputFields = Literal["process_id", "process_step", "function_name", "function_call_id"]

_WATERPARTIALBIDCALCULATIONINPUT_PROPERTIES_BY_FIELD = {
    "process_id": "processId",
    "process_step": "processStep",
    "function_name": "functionName",
    "function_call_id": "functionCallId",
}


class WaterPartialBidCalculationInputGraphQL(GraphQLCore):
    """This represents the reading version of water partial bid calculation input, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the water partial bid calculation input.
        data_record: The data record of the water partial bid calculation input node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        calculation_task: The calculation task field.
    """

    view_id = dm.ViewId("sp_powerops_models", "WaterPartialBidCalculationInput", "1")
    process_id: Optional[str] = Field(None, alias="processId")
    process_step: Optional[int] = Field(None, alias="processStep")
    function_name: Optional[str] = Field(None, alias="functionName")
    function_call_id: Optional[str] = Field(None, alias="functionCallId")
    calculation_task: Optional[BidCalculationTaskGraphQL] = Field(None, repr=False, alias="calculationTask")

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

    @field_validator("calculation_task", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> WaterPartialBidCalculationInput:
        """Convert this GraphQL format of water partial bid calculation input to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return WaterPartialBidCalculationInput(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecord(
                version=0,
                last_updated_time=self.data_record.last_updated_time,
                created_time=self.data_record.created_time,
            ),
            process_id=self.process_id,
            process_step=self.process_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            calculation_task=(
                self.calculation_task.as_read()
                if isinstance(self.calculation_task, GraphQLCore)
                else self.calculation_task
            ),
        )

    def as_write(self) -> WaterPartialBidCalculationInputWrite:
        """Convert this GraphQL format of water partial bid calculation input to the writing format."""
        return WaterPartialBidCalculationInputWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            process_id=self.process_id,
            process_step=self.process_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            calculation_task=(
                self.calculation_task.as_write()
                if isinstance(self.calculation_task, DomainModel)
                else self.calculation_task
            ),
        )


class WaterPartialBidCalculationInput(DomainModel):
    """This represents the reading version of water partial bid calculation input.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the water partial bid calculation input.
        data_record: The data record of the water partial bid calculation input node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        calculation_task: The calculation task field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "WaterPartialBidCalculationInput"
    )
    process_id: str = Field(alias="processId")
    process_step: int = Field(alias="processStep")
    function_name: str = Field(alias="functionName")
    function_call_id: str = Field(alias="functionCallId")
    calculation_task: Union[BidCalculationTask, str, dm.NodeId, None] = Field(None, repr=False, alias="calculationTask")

    def as_write(self) -> WaterPartialBidCalculationInputWrite:
        """Convert this read version of water partial bid calculation input to the writing version."""
        return WaterPartialBidCalculationInputWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            process_id=self.process_id,
            process_step=self.process_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            calculation_task=(
                self.calculation_task.as_write()
                if isinstance(self.calculation_task, DomainModel)
                else self.calculation_task
            ),
        )

    def as_apply(self) -> WaterPartialBidCalculationInputWrite:
        """Convert this read version of water partial bid calculation input to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class WaterPartialBidCalculationInputWrite(DomainModelWrite):
    """This represents the writing version of water partial bid calculation input.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the water partial bid calculation input.
        data_record: The data record of the water partial bid calculation input node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        calculation_task: The calculation task field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "WaterPartialBidCalculationInput"
    )
    process_id: str = Field(alias="processId")
    process_step: int = Field(alias="processStep")
    function_name: str = Field(alias="functionName")
    function_call_id: str = Field(alias="functionCallId")
    calculation_task: Union[BidCalculationTaskWrite, str, dm.NodeId, None] = Field(
        None, repr=False, alias="calculationTask"
    )

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(
            WaterPartialBidCalculationInput, dm.ViewId("sp_powerops_models", "WaterPartialBidCalculationInput", "1")
        )

        properties: dict[str, Any] = {}

        if self.process_id is not None:
            properties["processId"] = self.process_id

        if self.process_step is not None:
            properties["processStep"] = self.process_step

        if self.function_name is not None:
            properties["functionName"] = self.function_name

        if self.function_call_id is not None:
            properties["functionCallId"] = self.function_call_id

        if self.calculation_task is not None:
            properties["calculationTask"] = {
                "space": self.space if isinstance(self.calculation_task, str) else self.calculation_task.space,
                "externalId": (
                    self.calculation_task
                    if isinstance(self.calculation_task, str)
                    else self.calculation_task.external_id
                ),
            }

        if properties:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=None if allow_version_increase else self.data_record.existing_version,
                type=self.node_type,
                sources=[
                    dm.NodeOrEdgeData(
                        source=write_view,
                        properties=properties,
                    )
                ],
            )
            resources.nodes.append(this_node)
            cache.add(self.as_tuple_id())

        if isinstance(self.calculation_task, DomainModelWrite):
            other_resources = self.calculation_task._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class WaterPartialBidCalculationInputApply(WaterPartialBidCalculationInputWrite):
    def __new__(cls, *args, **kwargs) -> WaterPartialBidCalculationInputApply:
        warnings.warn(
            "WaterPartialBidCalculationInputApply is deprecated and will be removed in v1.0. Use WaterPartialBidCalculationInputWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "WaterPartialBidCalculationInput.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class WaterPartialBidCalculationInputList(DomainModelList[WaterPartialBidCalculationInput]):
    """List of water partial bid calculation inputs in the read version."""

    _INSTANCE = WaterPartialBidCalculationInput

    def as_write(self) -> WaterPartialBidCalculationInputWriteList:
        """Convert these read versions of water partial bid calculation input to the writing versions."""
        return WaterPartialBidCalculationInputWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> WaterPartialBidCalculationInputWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class WaterPartialBidCalculationInputWriteList(DomainModelWriteList[WaterPartialBidCalculationInputWrite]):
    """List of water partial bid calculation inputs in the writing version."""

    _INSTANCE = WaterPartialBidCalculationInputWrite


class WaterPartialBidCalculationInputApplyList(WaterPartialBidCalculationInputWriteList): ...


def _create_water_partial_bid_calculation_input_filter(
    view_id: dm.ViewId,
    process_id: str | list[str] | None = None,
    process_id_prefix: str | None = None,
    min_process_step: int | None = None,
    max_process_step: int | None = None,
    function_name: str | list[str] | None = None,
    function_name_prefix: str | None = None,
    function_call_id: str | list[str] | None = None,
    function_call_id_prefix: str | None = None,
    calculation_task: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if isinstance(process_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("processId"), value=process_id))
    if process_id and isinstance(process_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("processId"), values=process_id))
    if process_id_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("processId"), value=process_id_prefix))
    if min_process_step is not None or max_process_step is not None:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("processStep"), gte=min_process_step, lte=max_process_step)
        )
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
    if calculation_task and isinstance(calculation_task, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("calculationTask"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": calculation_task},
            )
        )
    if calculation_task and isinstance(calculation_task, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("calculationTask"),
                value={"space": calculation_task[0], "externalId": calculation_task[1]},
            )
        )
    if calculation_task and isinstance(calculation_task, list) and isinstance(calculation_task[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("calculationTask"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in calculation_task],
            )
        )
    if calculation_task and isinstance(calculation_task, list) and isinstance(calculation_task[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("calculationTask"),
                values=[{"space": item[0], "externalId": item[1]} for item in calculation_task],
            )
        )
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
