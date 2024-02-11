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
    from ._bid_matrix import BidMatrix, BidMatrixWrite


__all__ = [
    "TotalBidMatrixCalculationInput",
    "TotalBidMatrixCalculationInputList",
    "TotalBidMatrixCalculationInputFields",
    "TotalBidMatrixCalculationInputTextFields",
]


TotalBidMatrixCalculationInputTextFields = Literal["process_id", "function_name"]
TotalBidMatrixCalculationInputFields = Literal["process_id", "process_step", "function_name"]

_TOTALBIDMATRIXCALCULATIONINPUT_PROPERTIES_BY_FIELD = {
    "process_id": "processId",
    "process_step": "processStep",
    "function_name": "functionName",
}


class TotalBidMatrixCalculationInput(DomainModel):
    """This represents the reading version of total bid matrix calculation input.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the total bid matrix calculation input.
        data_record: The data record of the total bid matrix calculation input node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        partial_bid_matrices: The partial bid matrices that are used to calculate the total bid matrix.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "TotalBidMatrixCalculationInput"
    )
    process_id: str = Field(alias="processId")
    process_step: int = Field(alias="processStep")
    function_name: str = Field(alias="functionName")
    partial_bid_matrices: Union[list[BidMatrix], list[str], None] = Field(
        default=None, repr=False, alias="partialBidMatrices"
    )


class TotalBidMatrixCalculationInputList(DomainModelList[TotalBidMatrixCalculationInput]):
    """List of total bid matrix calculation inputs in the read version."""

    _INSTANCE = TotalBidMatrixCalculationInput


def _create_total_bid_matrix_calculation_input_filter(
    view_id: dm.ViewId,
    process_id: str | list[str] | None = None,
    process_id_prefix: str | None = None,
    min_process_step: int | None = None,
    max_process_step: int | None = None,
    function_name: str | list[str] | None = None,
    function_name_prefix: str | None = None,
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
    if external_id_prefix is not None:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if isinstance(space, str):
        filters.append(dm.filters.Equals(["node", "space"], value=space))
    if space and isinstance(space, list):
        filters.append(dm.filters.In(["node", "space"], values=space))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
