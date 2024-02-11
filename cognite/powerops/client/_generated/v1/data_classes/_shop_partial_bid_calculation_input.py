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
    from ._plant import Plant, PlantWrite
    from ._shop_result import SHOPResult, SHOPResultWrite


__all__ = [
    "ShopPartialBidCalculationInput",
    "ShopPartialBidCalculationInputList",
    "ShopPartialBidCalculationInputFields",
    "ShopPartialBidCalculationInputTextFields",
]


ShopPartialBidCalculationInputTextFields = Literal["process_id", "function_name"]
ShopPartialBidCalculationInputFields = Literal["process_id", "process_step", "function_name"]

_SHOPPARTIALBIDCALCULATIONINPUT_PROPERTIES_BY_FIELD = {
    "process_id": "processId",
    "process_step": "processStep",
    "function_name": "functionName",
}


class ShopPartialBidCalculationInput(DomainModel):
    """This represents the reading version of shop partial bid calculation input.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop partial bid calculation input.
        data_record: The data record of the shop partial bid calculation input node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        plant: The plant to calculate the partial bid for
        alerts: An array of calculation level Alerts.
        shop_results: An array of shop results.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "ShopPartialBidCalculationInput"
    )
    process_id: str = Field(alias="processId")
    process_step: int = Field(alias="processStep")
    function_name: str = Field(alias="functionName")
    plant: Union[Plant, str, dm.NodeId, None] = Field(None, repr=False)
    alerts: Union[list[Alert], list[str], None] = Field(default=None, repr=False)
    shop_results: Union[list[SHOPResult], list[str], None] = Field(default=None, repr=False, alias="shopResults")


class ShopPartialBidCalculationInputList(DomainModelList[ShopPartialBidCalculationInput]):
    """List of shop partial bid calculation inputs in the read version."""

    _INSTANCE = ShopPartialBidCalculationInput


def _create_shop_partial_bid_calculation_input_filter(
    view_id: dm.ViewId,
    process_id: str | list[str] | None = None,
    process_id_prefix: str | None = None,
    min_process_step: int | None = None,
    max_process_step: int | None = None,
    function_name: str | list[str] | None = None,
    function_name_prefix: str | None = None,
    plant: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
    if plant and isinstance(plant, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("plant"), value={"space": DEFAULT_INSTANCE_SPACE, "externalId": plant}
            )
        )
    if plant and isinstance(plant, tuple):
        filters.append(
            dm.filters.Equals(view_id.as_property_ref("plant"), value={"space": plant[0], "externalId": plant[1]})
        )
    if plant and isinstance(plant, list) and isinstance(plant[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("plant"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in plant],
            )
        )
    if plant and isinstance(plant, list) and isinstance(plant[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("plant"), values=[{"space": item[0], "externalId": item[1]} for item in plant]
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
