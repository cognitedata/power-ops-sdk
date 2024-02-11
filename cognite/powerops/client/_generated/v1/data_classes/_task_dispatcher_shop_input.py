from __future__ import annotations

import datetime
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
    from ._bid_configuration_shop import BidConfigurationShop, BidConfigurationShopWrite


__all__ = [
    "TaskDispatcherShopInput",
    "TaskDispatcherShopInputList",
    "TaskDispatcherShopInputFields",
    "TaskDispatcherShopInputTextFields",
]


TaskDispatcherShopInputTextFields = Literal["process_id", "function_name"]
TaskDispatcherShopInputFields = Literal[
    "process_id", "process_step", "function_name", "bid_date", "shop_start", "shop_end"
]

_TASKDISPATCHERSHOPINPUT_PROPERTIES_BY_FIELD = {
    "process_id": "processId",
    "process_step": "processStep",
    "function_name": "functionName",
    "bid_date": "bidDate",
    "shop_start": "shopStart",
    "shop_end": "shopEnd",
}


class TaskDispatcherShopInput(DomainModel):
    """This represents the reading version of task dispatcher shop input.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the task dispatcher shop input.
        data_record: The data record of the task dispatcher shop input node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        bid_configuration: The bid configuration field.
        bid_date: The bid date
        shop_start: The shop start date
        shop_end: The shop end date
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "TaskDispatcherShopInput"
    )
    process_id: str = Field(alias="processId")
    process_step: int = Field(alias="processStep")
    function_name: str = Field(alias="functionName")
    bid_configuration: Union[BidConfigurationShop, str, dm.NodeId, None] = Field(
        None, repr=False, alias="bidConfiguration"
    )
    bid_date: Optional[datetime.date] = Field(None, alias="bidDate")
    shop_start: Optional[datetime.date] = Field(None, alias="shopStart")
    shop_end: Optional[datetime.date] = Field(None, alias="shopEnd")


class TaskDispatcherShopInputList(DomainModelList[TaskDispatcherShopInput]):
    """List of task dispatcher shop inputs in the read version."""

    _INSTANCE = TaskDispatcherShopInput


def _create_task_dispatcher_shop_input_filter(
    view_id: dm.ViewId,
    process_id: str | list[str] | None = None,
    process_id_prefix: str | None = None,
    min_process_step: int | None = None,
    max_process_step: int | None = None,
    function_name: str | list[str] | None = None,
    function_name_prefix: str | None = None,
    bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    min_bid_date: datetime.date | None = None,
    max_bid_date: datetime.date | None = None,
    min_shop_start: datetime.date | None = None,
    max_shop_start: datetime.date | None = None,
    min_shop_end: datetime.date | None = None,
    max_shop_end: datetime.date | None = None,
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
    if min_bid_date is not None or max_bid_date is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("bidDate"),
                gte=min_bid_date.isoformat() if min_bid_date else None,
                lte=max_bid_date.isoformat() if max_bid_date else None,
            )
        )
    if min_shop_start is not None or max_shop_start is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("shopStart"),
                gte=min_shop_start.isoformat() if min_shop_start else None,
                lte=max_shop_start.isoformat() if max_shop_start else None,
            )
        )
    if min_shop_end is not None or max_shop_end is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("shopEnd"),
                gte=min_shop_end.isoformat() if min_shop_end else None,
                lte=max_shop_end.isoformat() if max_shop_end else None,
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
