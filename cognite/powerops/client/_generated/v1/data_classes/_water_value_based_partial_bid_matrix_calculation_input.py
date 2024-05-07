from __future__ import annotations

import datetime
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
from ._partial_bid_matrix_calculation_input import (
    PartialBidMatrixCalculationInput,
    PartialBidMatrixCalculationInputWrite,
)

if TYPE_CHECKING:
    from ._bid_configuration_day_ahead import (
        BidConfigurationDayAhead,
        BidConfigurationDayAheadGraphQL,
        BidConfigurationDayAheadWrite,
    )
    from ._water_value_based_partial_bid_configuration import (
        WaterValueBasedPartialBidConfiguration,
        WaterValueBasedPartialBidConfigurationGraphQL,
        WaterValueBasedPartialBidConfigurationWrite,
    )


__all__ = [
    "WaterValueBasedPartialBidMatrixCalculationInput",
    "WaterValueBasedPartialBidMatrixCalculationInputWrite",
    "WaterValueBasedPartialBidMatrixCalculationInputApply",
    "WaterValueBasedPartialBidMatrixCalculationInputList",
    "WaterValueBasedPartialBidMatrixCalculationInputWriteList",
    "WaterValueBasedPartialBidMatrixCalculationInputApplyList",
    "WaterValueBasedPartialBidMatrixCalculationInputFields",
    "WaterValueBasedPartialBidMatrixCalculationInputTextFields",
]


WaterValueBasedPartialBidMatrixCalculationInputTextFields = Literal[
    "workflow_execution_id", "function_name", "function_call_id"
]
WaterValueBasedPartialBidMatrixCalculationInputFields = Literal[
    "workflow_execution_id", "workflow_step", "function_name", "function_call_id", "bid_date"
]

_WATERVALUEBASEDPARTIALBIDMATRIXCALCULATIONINPUT_PROPERTIES_BY_FIELD = {
    "workflow_execution_id": "workflowExecutionId",
    "workflow_step": "workflowStep",
    "function_name": "functionName",
    "function_call_id": "functionCallId",
    "bid_date": "bidDate",
}


class WaterValueBasedPartialBidMatrixCalculationInputGraphQL(GraphQLCore):
    """This represents the reading version of water value based partial bid matrix calculation input, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the water value based partial bid matrix calculation input.
        data_record: The data record of the water value based partial bid matrix calculation input node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        bid_date: The bid date
        bid_configuration: TODO description
        partial_bid_configuration: The partial bid configuration related to the bid calculation task
    """

    view_id = dm.ViewId("sp_power_ops_models", "WaterValueBasedPartialBidMatrixCalculationInput", "1")
    workflow_execution_id: Optional[str] = Field(None, alias="workflowExecutionId")
    workflow_step: Optional[int] = Field(None, alias="workflowStep")
    function_name: Optional[str] = Field(None, alias="functionName")
    function_call_id: Optional[str] = Field(None, alias="functionCallId")
    bid_date: Optional[datetime.date] = Field(None, alias="bidDate")
    bid_configuration: Optional[BidConfigurationDayAheadGraphQL] = Field(None, repr=False, alias="bidConfiguration")
    partial_bid_configuration: Optional[WaterValueBasedPartialBidConfigurationGraphQL] = Field(
        None, repr=False, alias="partialBidConfiguration"
    )

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

    @field_validator("bid_configuration", "partial_bid_configuration", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> WaterValueBasedPartialBidMatrixCalculationInput:
        """Convert this GraphQL format of water value based partial bid matrix calculation input to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return WaterValueBasedPartialBidMatrixCalculationInput(
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
            bid_date=self.bid_date,
            bid_configuration=(
                self.bid_configuration.as_read()
                if isinstance(self.bid_configuration, GraphQLCore)
                else self.bid_configuration
            ),
            partial_bid_configuration=(
                self.partial_bid_configuration.as_read()
                if isinstance(self.partial_bid_configuration, GraphQLCore)
                else self.partial_bid_configuration
            ),
        )

    def as_write(self) -> WaterValueBasedPartialBidMatrixCalculationInputWrite:
        """Convert this GraphQL format of water value based partial bid matrix calculation input to the writing format."""
        return WaterValueBasedPartialBidMatrixCalculationInputWrite(
            space=self.space or DEFAULT_INSTANCE_SPACE,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            workflow_execution_id=self.workflow_execution_id,
            workflow_step=self.workflow_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            bid_date=self.bid_date,
            bid_configuration=(
                self.bid_configuration.as_write()
                if isinstance(self.bid_configuration, DomainModel)
                else self.bid_configuration
            ),
            partial_bid_configuration=(
                self.partial_bid_configuration.as_write()
                if isinstance(self.partial_bid_configuration, DomainModel)
                else self.partial_bid_configuration
            ),
        )


class WaterValueBasedPartialBidMatrixCalculationInput(PartialBidMatrixCalculationInput):
    """This represents the reading version of water value based partial bid matrix calculation input.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the water value based partial bid matrix calculation input.
        data_record: The data record of the water value based partial bid matrix calculation input node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        bid_date: The bid date
        bid_configuration: TODO description
        partial_bid_configuration: The partial bid configuration related to the bid calculation task
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_power_ops_types", "WaterValueBasedPartialBidMatrixCalculationInput"
    )

    def as_write(self) -> WaterValueBasedPartialBidMatrixCalculationInputWrite:
        """Convert this read version of water value based partial bid matrix calculation input to the writing version."""
        return WaterValueBasedPartialBidMatrixCalculationInputWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            workflow_execution_id=self.workflow_execution_id,
            workflow_step=self.workflow_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            bid_date=self.bid_date,
            bid_configuration=(
                self.bid_configuration.as_write()
                if isinstance(self.bid_configuration, DomainModel)
                else self.bid_configuration
            ),
            partial_bid_configuration=(
                self.partial_bid_configuration.as_write()
                if isinstance(self.partial_bid_configuration, DomainModel)
                else self.partial_bid_configuration
            ),
        )

    def as_apply(self) -> WaterValueBasedPartialBidMatrixCalculationInputWrite:
        """Convert this read version of water value based partial bid matrix calculation input to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class WaterValueBasedPartialBidMatrixCalculationInputWrite(PartialBidMatrixCalculationInputWrite):
    """This represents the writing version of water value based partial bid matrix calculation input.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the water value based partial bid matrix calculation input.
        data_record: The data record of the water value based partial bid matrix calculation input node.
        workflow_execution_id: The process associated with the function execution
        workflow_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        bid_date: The bid date
        bid_configuration: TODO description
        partial_bid_configuration: The partial bid configuration related to the bid calculation task
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_power_ops_types", "WaterValueBasedPartialBidMatrixCalculationInput"
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
            WaterValueBasedPartialBidMatrixCalculationInput,
            dm.ViewId("sp_power_ops_models", "WaterValueBasedPartialBidMatrixCalculationInput", "1"),
        )

        properties: dict[str, Any] = {}

        if self.workflow_execution_id is not None:
            properties["workflowExecutionId"] = self.workflow_execution_id

        if self.workflow_step is not None:
            properties["workflowStep"] = self.workflow_step

        if self.function_name is not None:
            properties["functionName"] = self.function_name

        if self.function_call_id is not None:
            properties["functionCallId"] = self.function_call_id

        if self.bid_date is not None or write_none:
            properties["bidDate"] = self.bid_date.isoformat() if self.bid_date else None

        if self.bid_configuration is not None:
            properties["bidConfiguration"] = {
                "space": self.space if isinstance(self.bid_configuration, str) else self.bid_configuration.space,
                "externalId": (
                    self.bid_configuration
                    if isinstance(self.bid_configuration, str)
                    else self.bid_configuration.external_id
                ),
            }

        if self.partial_bid_configuration is not None:
            properties["partialBidConfiguration"] = {
                "space": (
                    self.space
                    if isinstance(self.partial_bid_configuration, str)
                    else self.partial_bid_configuration.space
                ),
                "externalId": (
                    self.partial_bid_configuration
                    if isinstance(self.partial_bid_configuration, str)
                    else self.partial_bid_configuration.external_id
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

        if isinstance(self.bid_configuration, DomainModelWrite):
            other_resources = self.bid_configuration._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.partial_bid_configuration, DomainModelWrite):
            other_resources = self.partial_bid_configuration._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class WaterValueBasedPartialBidMatrixCalculationInputApply(WaterValueBasedPartialBidMatrixCalculationInputWrite):
    def __new__(cls, *args, **kwargs) -> WaterValueBasedPartialBidMatrixCalculationInputApply:
        warnings.warn(
            "WaterValueBasedPartialBidMatrixCalculationInputApply is deprecated and will be removed in v1.0. Use WaterValueBasedPartialBidMatrixCalculationInputWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "WaterValueBasedPartialBidMatrixCalculationInput.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class WaterValueBasedPartialBidMatrixCalculationInputList(
    DomainModelList[WaterValueBasedPartialBidMatrixCalculationInput]
):
    """List of water value based partial bid matrix calculation inputs in the read version."""

    _INSTANCE = WaterValueBasedPartialBidMatrixCalculationInput

    def as_write(self) -> WaterValueBasedPartialBidMatrixCalculationInputWriteList:
        """Convert these read versions of water value based partial bid matrix calculation input to the writing versions."""
        return WaterValueBasedPartialBidMatrixCalculationInputWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> WaterValueBasedPartialBidMatrixCalculationInputWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class WaterValueBasedPartialBidMatrixCalculationInputWriteList(
    DomainModelWriteList[WaterValueBasedPartialBidMatrixCalculationInputWrite]
):
    """List of water value based partial bid matrix calculation inputs in the writing version."""

    _INSTANCE = WaterValueBasedPartialBidMatrixCalculationInputWrite


class WaterValueBasedPartialBidMatrixCalculationInputApplyList(
    WaterValueBasedPartialBidMatrixCalculationInputWriteList
): ...


def _create_water_value_based_partial_bid_matrix_calculation_input_filter(
    view_id: dm.ViewId,
    workflow_execution_id: str | list[str] | None = None,
    workflow_execution_id_prefix: str | None = None,
    min_workflow_step: int | None = None,
    max_workflow_step: int | None = None,
    function_name: str | list[str] | None = None,
    function_name_prefix: str | None = None,
    function_call_id: str | list[str] | None = None,
    function_call_id_prefix: str | None = None,
    min_bid_date: datetime.date | None = None,
    max_bid_date: datetime.date | None = None,
    bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    partial_bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    external_id_prefix: str | None = None,
    space: str | list[str] | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if isinstance(workflow_execution_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("workflowExecutionId"), value=workflow_execution_id))
    if workflow_execution_id and isinstance(workflow_execution_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("workflowExecutionId"), values=workflow_execution_id))
    if workflow_execution_id_prefix is not None:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("workflowExecutionId"), value=workflow_execution_id_prefix)
        )
    if min_workflow_step is not None or max_workflow_step is not None:
        filters.append(
            dm.filters.Range(view_id.as_property_ref("workflowStep"), gte=min_workflow_step, lte=max_workflow_step)
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
    if min_bid_date is not None or max_bid_date is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("bidDate"),
                gte=min_bid_date.isoformat() if min_bid_date else None,
                lte=max_bid_date.isoformat() if max_bid_date else None,
            )
        )
    if bid_configuration and isinstance(bid_configuration, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("bidConfiguration"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": bid_configuration},
            )
        )
    if bid_configuration and isinstance(bid_configuration, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("bidConfiguration"),
                value={"space": bid_configuration[0], "externalId": bid_configuration[1]},
            )
        )
    if bid_configuration and isinstance(bid_configuration, list) and isinstance(bid_configuration[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("bidConfiguration"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in bid_configuration],
            )
        )
    if bid_configuration and isinstance(bid_configuration, list) and isinstance(bid_configuration[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("bidConfiguration"),
                values=[{"space": item[0], "externalId": item[1]} for item in bid_configuration],
            )
        )
    if partial_bid_configuration and isinstance(partial_bid_configuration, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("partialBidConfiguration"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": partial_bid_configuration},
            )
        )
    if partial_bid_configuration and isinstance(partial_bid_configuration, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("partialBidConfiguration"),
                value={"space": partial_bid_configuration[0], "externalId": partial_bid_configuration[1]},
            )
        )
    if (
        partial_bid_configuration
        and isinstance(partial_bid_configuration, list)
        and isinstance(partial_bid_configuration[0], str)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("partialBidConfiguration"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in partial_bid_configuration],
            )
        )
    if (
        partial_bid_configuration
        and isinstance(partial_bid_configuration, list)
        and isinstance(partial_bid_configuration[0], tuple)
    ):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("partialBidConfiguration"),
                values=[{"space": item[0], "externalId": item[1]} for item in partial_bid_configuration],
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
