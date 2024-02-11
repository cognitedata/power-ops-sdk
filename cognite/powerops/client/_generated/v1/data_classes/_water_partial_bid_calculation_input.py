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
    from ._bid_calculation_task import BidCalculationTask, BidCalculationTaskWrite


__all__ = [
    "WaterPartialBidCalculationInput",
    "WaterPartialBidCalculationInputList",
    "WaterPartialBidCalculationInputFields",
    "WaterPartialBidCalculationInputTextFields",
]


WaterPartialBidCalculationInputTextFields = Literal["process_id", "function_name"]
WaterPartialBidCalculationInputFields = Literal["process_id", "process_step", "function_name"]

_WATERPARTIALBIDCALCULATIONINPUT_PROPERTIES_BY_FIELD = {
    "process_id": "processId",
    "process_step": "processStep",
    "function_name": "functionName",
}


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
        calculation_task: The calculation task field.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "WaterPartialBidCalculationInput"
    )
    process_id: str = Field(alias="processId")
    process_step: int = Field(alias="processStep")
    function_name: str = Field(alias="functionName")
    calculation_task: Union[BidCalculationTask, str, dm.NodeId, None] = Field(None, repr=False, alias="calculationTask")


class WaterPartialBidCalculationInputList(DomainModelList[WaterPartialBidCalculationInput]):
    """List of water partial bid calculation inputs in the read version."""

    _INSTANCE = WaterPartialBidCalculationInput


def _create_water_partial_bid_calculation_input_filter(
    view_id: dm.ViewId,
    process_id: str | list[str] | None = None,
    process_id_prefix: str | None = None,
    min_process_step: int | None = None,
    max_process_step: int | None = None,
    function_name: str | list[str] | None = None,
    function_name_prefix: str | None = None,
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
