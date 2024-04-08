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
from ._function_output import FunctionOutput, FunctionOutputWrite

if TYPE_CHECKING:
    from ._alert import Alert, AlertGraphQL, AlertWrite
    from ._bid_configuration import BidConfiguration, BidConfigurationGraphQL, BidConfigurationWrite
    from ._bid_matrix import BidMatrix, BidMatrixGraphQL, BidMatrixWrite
    from ._partial_bid_matrix_calculation_input import (
        PartialBidMatrixCalculationInput,
        PartialBidMatrixCalculationInputGraphQL,
        PartialBidMatrixCalculationInputWrite,
    )


__all__ = [
    "PartialBidMatrixCalculationOutput",
    "PartialBidMatrixCalculationOutputWrite",
    "PartialBidMatrixCalculationOutputApply",
    "PartialBidMatrixCalculationOutputList",
    "PartialBidMatrixCalculationOutputWriteList",
    "PartialBidMatrixCalculationOutputApplyList",
    "PartialBidMatrixCalculationOutputFields",
    "PartialBidMatrixCalculationOutputTextFields",
]


PartialBidMatrixCalculationOutputTextFields = Literal["process_id", "function_name", "function_call_id"]
PartialBidMatrixCalculationOutputFields = Literal["process_id", "process_step", "function_name", "function_call_id"]

_PARTIALBIDMATRIXCALCULATIONOUTPUT_PROPERTIES_BY_FIELD = {
    "process_id": "processId",
    "process_step": "processStep",
    "function_name": "functionName",
    "function_call_id": "functionCallId",
}


class PartialBidMatrixCalculationOutputGraphQL(GraphQLCore):
    """This represents the reading version of partial bid matrix calculation output, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the partial bid matrix calculation output.
        data_record: The data record of the partial bid matrix calculation output node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        alerts: An array of calculation level Alerts.
        partial_matrix: The partial matrix field.
        bid_configuration: The bid configuration field.
        input_: The input field.
    """

    view_id = dm.ViewId("sp_powerops_models_temp", "PartialBidMatrixCalculationOutput", "1")
    process_id: Optional[str] = Field(None, alias="processId")
    process_step: Optional[int] = Field(None, alias="processStep")
    function_name: Optional[str] = Field(None, alias="functionName")
    function_call_id: Optional[str] = Field(None, alias="functionCallId")
    alerts: Optional[list[AlertGraphQL]] = Field(default=None, repr=False)
    partial_matrix: Optional[BidMatrixGraphQL] = Field(None, repr=False, alias="partialMatrix")
    bid_configuration: Optional[BidConfigurationGraphQL] = Field(None, repr=False, alias="bidConfiguration")
    input_: Optional[PartialBidMatrixCalculationInputGraphQL] = Field(None, repr=False, alias="input")

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

    @field_validator("alerts", "partial_matrix", "bid_configuration", "input_", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> PartialBidMatrixCalculationOutput:
        """Convert this GraphQL format of partial bid matrix calculation output to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return PartialBidMatrixCalculationOutput(
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
            alerts=[alert.as_read() if isinstance(alert, GraphQLCore) else alert for alert in self.alerts or []],
            partial_matrix=(
                self.partial_matrix.as_read() if isinstance(self.partial_matrix, GraphQLCore) else self.partial_matrix
            ),
            bid_configuration=(
                self.bid_configuration.as_read()
                if isinstance(self.bid_configuration, GraphQLCore)
                else self.bid_configuration
            ),
            input_=self.input_.as_read() if isinstance(self.input_, GraphQLCore) else self.input_,
        )

    def as_write(self) -> PartialBidMatrixCalculationOutputWrite:
        """Convert this GraphQL format of partial bid matrix calculation output to the writing format."""
        return PartialBidMatrixCalculationOutputWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            process_id=self.process_id,
            process_step=self.process_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            alerts=[alert.as_write() if isinstance(alert, DomainModel) else alert for alert in self.alerts or []],
            partial_matrix=(
                self.partial_matrix.as_write() if isinstance(self.partial_matrix, DomainModel) else self.partial_matrix
            ),
            bid_configuration=(
                self.bid_configuration.as_write()
                if isinstance(self.bid_configuration, DomainModel)
                else self.bid_configuration
            ),
            input_=self.input_.as_write() if isinstance(self.input_, DomainModel) else self.input_,
        )


class PartialBidMatrixCalculationOutput(FunctionOutput):
    """This represents the reading version of partial bid matrix calculation output.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the partial bid matrix calculation output.
        data_record: The data record of the partial bid matrix calculation output node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        alerts: An array of calculation level Alerts.
        partial_matrix: The partial matrix field.
        bid_configuration: The bid configuration field.
        input_: The input field.
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types_temp", "PartialBidMatrixCalculationOutput"
    )
    partial_matrix: Union[BidMatrix, str, dm.NodeId, None] = Field(None, repr=False, alias="partialMatrix")
    bid_configuration: Union[BidConfiguration, str, dm.NodeId, None] = Field(None, repr=False, alias="bidConfiguration")
    input_: Union[PartialBidMatrixCalculationInput, str, dm.NodeId, None] = Field(None, repr=False, alias="input")

    def as_write(self) -> PartialBidMatrixCalculationOutputWrite:
        """Convert this read version of partial bid matrix calculation output to the writing version."""
        return PartialBidMatrixCalculationOutputWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            process_id=self.process_id,
            process_step=self.process_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            alerts=[alert.as_write() if isinstance(alert, DomainModel) else alert for alert in self.alerts or []],
            partial_matrix=(
                self.partial_matrix.as_write() if isinstance(self.partial_matrix, DomainModel) else self.partial_matrix
            ),
            bid_configuration=(
                self.bid_configuration.as_write()
                if isinstance(self.bid_configuration, DomainModel)
                else self.bid_configuration
            ),
            input_=self.input_.as_write() if isinstance(self.input_, DomainModel) else self.input_,
        )

    def as_apply(self) -> PartialBidMatrixCalculationOutputWrite:
        """Convert this read version of partial bid matrix calculation output to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PartialBidMatrixCalculationOutputWrite(FunctionOutputWrite):
    """This represents the writing version of partial bid matrix calculation output.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the partial bid matrix calculation output.
        data_record: The data record of the partial bid matrix calculation output node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        alerts: An array of calculation level Alerts.
        partial_matrix: The partial matrix field.
        bid_configuration: The bid configuration field.
        input_: The input field.
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types_temp", "PartialBidMatrixCalculationOutput"
    )
    partial_matrix: Union[BidMatrixWrite, str, dm.NodeId, None] = Field(None, repr=False, alias="partialMatrix")
    bid_configuration: Union[BidConfigurationWrite, str, dm.NodeId, None] = Field(
        None, repr=False, alias="bidConfiguration"
    )
    input_: Union[PartialBidMatrixCalculationInputWrite, str, dm.NodeId, None] = Field(None, repr=False, alias="input")

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
            PartialBidMatrixCalculationOutput,
            dm.ViewId("sp_powerops_models_temp", "PartialBidMatrixCalculationOutput", "1"),
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

        if self.partial_matrix is not None:
            properties["partialMatrix"] = {
                "space": self.space if isinstance(self.partial_matrix, str) else self.partial_matrix.space,
                "externalId": (
                    self.partial_matrix if isinstance(self.partial_matrix, str) else self.partial_matrix.external_id
                ),
            }

        if self.bid_configuration is not None:
            properties["bidConfiguration"] = {
                "space": self.space if isinstance(self.bid_configuration, str) else self.bid_configuration.space,
                "externalId": (
                    self.bid_configuration
                    if isinstance(self.bid_configuration, str)
                    else self.bid_configuration.external_id
                ),
            }

        if self.input_ is not None:
            properties["input"] = {
                "space": self.space if isinstance(self.input_, str) else self.input_.space,
                "externalId": self.input_ if isinstance(self.input_, str) else self.input_.external_id,
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

        edge_type = dm.DirectRelationReference("sp_powerops_types_temp", "calculationIssue")
        for alert in self.alerts or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=alert,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.partial_matrix, DomainModelWrite):
            other_resources = self.partial_matrix._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.bid_configuration, DomainModelWrite):
            other_resources = self.bid_configuration._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.input_, DomainModelWrite):
            other_resources = self.input_._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class PartialBidMatrixCalculationOutputApply(PartialBidMatrixCalculationOutputWrite):
    def __new__(cls, *args, **kwargs) -> PartialBidMatrixCalculationOutputApply:
        warnings.warn(
            "PartialBidMatrixCalculationOutputApply is deprecated and will be removed in v1.0. Use PartialBidMatrixCalculationOutputWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "PartialBidMatrixCalculationOutput.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class PartialBidMatrixCalculationOutputList(DomainModelList[PartialBidMatrixCalculationOutput]):
    """List of partial bid matrix calculation outputs in the read version."""

    _INSTANCE = PartialBidMatrixCalculationOutput

    def as_write(self) -> PartialBidMatrixCalculationOutputWriteList:
        """Convert these read versions of partial bid matrix calculation output to the writing versions."""
        return PartialBidMatrixCalculationOutputWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> PartialBidMatrixCalculationOutputWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class PartialBidMatrixCalculationOutputWriteList(DomainModelWriteList[PartialBidMatrixCalculationOutputWrite]):
    """List of partial bid matrix calculation outputs in the writing version."""

    _INSTANCE = PartialBidMatrixCalculationOutputWrite


class PartialBidMatrixCalculationOutputApplyList(PartialBidMatrixCalculationOutputWriteList): ...


def _create_partial_bid_matrix_calculation_output_filter(
    view_id: dm.ViewId,
    process_id: str | list[str] | None = None,
    process_id_prefix: str | None = None,
    min_process_step: int | None = None,
    max_process_step: int | None = None,
    function_name: str | list[str] | None = None,
    function_name_prefix: str | None = None,
    function_call_id: str | list[str] | None = None,
    function_call_id_prefix: str | None = None,
    partial_matrix: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    input_: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
    if partial_matrix and isinstance(partial_matrix, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("partialMatrix"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": partial_matrix},
            )
        )
    if partial_matrix and isinstance(partial_matrix, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("partialMatrix"),
                value={"space": partial_matrix[0], "externalId": partial_matrix[1]},
            )
        )
    if partial_matrix and isinstance(partial_matrix, list) and isinstance(partial_matrix[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("partialMatrix"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in partial_matrix],
            )
        )
    if partial_matrix and isinstance(partial_matrix, list) and isinstance(partial_matrix[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("partialMatrix"),
                values=[{"space": item[0], "externalId": item[1]} for item in partial_matrix],
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
    if input_ and isinstance(input_, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("input"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": input_}
            )
        )
    if input_ and isinstance(input_, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("input"), value={"space": input_[0], "externalId": input_[1]})
        )
    if input_ and isinstance(input_, list) and isinstance(input_[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("input"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in input_],
            )
        )
    if input_ and isinstance(input_, list) and isinstance(input_[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("input"), values=[{"space": item[0], "externalId": item[1]} for item in input_]
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
