from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecordWrite,
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    DomainModelWriteList,
    DomainModelList,
    DomainRelationWrite,
    ResourcesWrite,
)

if TYPE_CHECKING:
    from ._alert import Alert, AlertWrite
    from ._bid_matrix_raw import BidMatrixRaw, BidMatrixRawWrite
    from ._water_partial_bid_calculation_input import (
        WaterPartialBidCalculationInput,
        WaterPartialBidCalculationInputWrite,
    )


__all__ = [
    "WaterPartialBidCalculationOutput",
    "WaterPartialBidCalculationOutputWrite",
    "WaterPartialBidCalculationOutputApply",
    "WaterPartialBidCalculationOutputList",
    "WaterPartialBidCalculationOutputWriteList",
    "WaterPartialBidCalculationOutputApplyList",
    "WaterPartialBidCalculationOutputFields",
    "WaterPartialBidCalculationOutputTextFields",
]


WaterPartialBidCalculationOutputTextFields = Literal["process_id", "function_name", "function_call_id"]
WaterPartialBidCalculationOutputFields = Literal["process_id", "process_step", "function_name", "function_call_id"]

_WATERPARTIALBIDCALCULATIONOUTPUT_PROPERTIES_BY_FIELD = {
    "process_id": "processId",
    "process_step": "processStep",
    "function_name": "functionName",
    "function_call_id": "functionCallId",
}


class WaterPartialBidCalculationOutput(DomainModel):
    """This represents the reading version of water partial bid calculation output.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the water partial bid calculation output.
        data_record: The data record of the water partial bid calculation output node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        alerts: An array of calculation level Alerts.
        raw_partial_matrix: The raw partial matrix field.
        input_: The input field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "WaterPartialBidCalculationOutput"
    )
    process_id: str = Field(alias="processId")
    process_step: int = Field(alias="processStep")
    function_name: str = Field(alias="functionName")
    function_call_id: str = Field(alias="functionCallId")
    alerts: Union[list[Alert], list[str], None] = Field(default=None, repr=False)
    raw_partial_matrix: Union[BidMatrixRaw, str, dm.NodeId, None] = Field(None, repr=False, alias="rawPartialMatrix")
    input_: Union[WaterPartialBidCalculationInput, str, dm.NodeId, None] = Field(None, repr=False, alias="input")

    def as_write(self) -> WaterPartialBidCalculationOutputWrite:
        """Convert this read version of water partial bid calculation output to the writing version."""
        return WaterPartialBidCalculationOutputWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            process_id=self.process_id,
            process_step=self.process_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            alerts=[alert.as_write() if isinstance(alert, DomainModel) else alert for alert in self.alerts or []],
            raw_partial_matrix=(
                self.raw_partial_matrix.as_write()
                if isinstance(self.raw_partial_matrix, DomainModel)
                else self.raw_partial_matrix
            ),
            input_=self.input_.as_write() if isinstance(self.input_, DomainModel) else self.input_,
        )

    def as_apply(self) -> WaterPartialBidCalculationOutputWrite:
        """Convert this read version of water partial bid calculation output to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class WaterPartialBidCalculationOutputWrite(DomainModelWrite):
    """This represents the writing version of water partial bid calculation output.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the water partial bid calculation output.
        data_record: The data record of the water partial bid calculation output node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        alerts: An array of calculation level Alerts.
        raw_partial_matrix: The raw partial matrix field.
        input_: The input field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "WaterPartialBidCalculationOutput"
    )
    process_id: str = Field(alias="processId")
    process_step: int = Field(alias="processStep")
    function_name: str = Field(alias="functionName")
    function_call_id: str = Field(alias="functionCallId")
    alerts: Union[list[AlertWrite], list[str], None] = Field(default=None, repr=False)
    raw_partial_matrix: Union[BidMatrixRawWrite, str, dm.NodeId, None] = Field(
        None, repr=False, alias="rawPartialMatrix"
    )
    input_: Union[WaterPartialBidCalculationInputWrite, str, dm.NodeId, None] = Field(None, repr=False, alias="input")

    def _to_instances_write(
        self,
        cache: set[tuple[str, str]],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId] | None,
        write_none: bool = False,
    ) -> ResourcesWrite:
        resources = ResourcesWrite()
        if self.as_tuple_id() in cache:
            return resources

        write_view = (view_by_read_class or {}).get(
            WaterPartialBidCalculationOutput, dm.ViewId("sp_powerops_models", "WaterPartialBidCalculationOutput", "1")
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

        if self.raw_partial_matrix is not None:
            properties["rawPartialMatrix"] = {
                "space": self.space if isinstance(self.raw_partial_matrix, str) else self.raw_partial_matrix.space,
                "externalId": (
                    self.raw_partial_matrix
                    if isinstance(self.raw_partial_matrix, str)
                    else self.raw_partial_matrix.external_id
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
                existing_version=self.data_record.existing_version,
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

        edge_type = dm.DirectRelationReference("sp_powerops_types", "calculationIssue")
        for alert in self.alerts or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache, start_node=self, end_node=alert, edge_type=edge_type, view_by_read_class=view_by_read_class
            )
            resources.extend(other_resources)

        if isinstance(self.raw_partial_matrix, DomainModelWrite):
            other_resources = self.raw_partial_matrix._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.input_, DomainModelWrite):
            other_resources = self.input_._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class WaterPartialBidCalculationOutputApply(WaterPartialBidCalculationOutputWrite):
    def __new__(cls, *args, **kwargs) -> WaterPartialBidCalculationOutputApply:
        warnings.warn(
            "WaterPartialBidCalculationOutputApply is deprecated and will be removed in v1.0. Use WaterPartialBidCalculationOutputWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "WaterPartialBidCalculationOutput.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class WaterPartialBidCalculationOutputList(DomainModelList[WaterPartialBidCalculationOutput]):
    """List of water partial bid calculation outputs in the read version."""

    _INSTANCE = WaterPartialBidCalculationOutput

    def as_write(self) -> WaterPartialBidCalculationOutputWriteList:
        """Convert these read versions of water partial bid calculation output to the writing versions."""
        return WaterPartialBidCalculationOutputWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> WaterPartialBidCalculationOutputWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class WaterPartialBidCalculationOutputWriteList(DomainModelWriteList[WaterPartialBidCalculationOutputWrite]):
    """List of water partial bid calculation outputs in the writing version."""

    _INSTANCE = WaterPartialBidCalculationOutputWrite


class WaterPartialBidCalculationOutputApplyList(WaterPartialBidCalculationOutputWriteList): ...


def _create_water_partial_bid_calculation_output_filter(
    view_id: dm.ViewId,
    process_id: str | list[str] | None = None,
    process_id_prefix: str | None = None,
    min_process_step: int | None = None,
    max_process_step: int | None = None,
    function_name: str | list[str] | None = None,
    function_name_prefix: str | None = None,
    function_call_id: str | list[str] | None = None,
    function_call_id_prefix: str | None = None,
    raw_partial_matrix: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
    if raw_partial_matrix and isinstance(raw_partial_matrix, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("rawPartialMatrix"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": raw_partial_matrix},
            )
        )
    if raw_partial_matrix and isinstance(raw_partial_matrix, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("rawPartialMatrix"),
                value={"space": raw_partial_matrix[0], "externalId": raw_partial_matrix[1]},
            )
        )
    if raw_partial_matrix and isinstance(raw_partial_matrix, list) and isinstance(raw_partial_matrix[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("rawPartialMatrix"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in raw_partial_matrix],
            )
        )
    if raw_partial_matrix and isinstance(raw_partial_matrix, list) and isinstance(raw_partial_matrix[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("rawPartialMatrix"),
                values=[{"space": item[0], "externalId": item[1]} for item in raw_partial_matrix],
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
