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
    "TaskDispatcherShopInputWrite",
    "TaskDispatcherShopInputApply",
    "TaskDispatcherShopInputList",
    "TaskDispatcherShopInputWriteList",
    "TaskDispatcherShopInputApplyList",
    "TaskDispatcherShopInputFields",
    "TaskDispatcherShopInputTextFields",
]


TaskDispatcherShopInputTextFields = Literal["process_id", "function_name", "function_call_id"]
TaskDispatcherShopInputFields = Literal[
    "process_id", "process_step", "function_name", "function_call_id", "bid_date", "shop_start", "shop_end"
]

_TASKDISPATCHERSHOPINPUT_PROPERTIES_BY_FIELD = {
    "process_id": "processId",
    "process_step": "processStep",
    "function_name": "functionName",
    "function_call_id": "functionCallId",
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
        function_call_id: The function call id
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
    function_call_id: str = Field(alias="functionCallId")
    bid_configuration: Union[BidConfigurationShop, str, dm.NodeId, None] = Field(
        None, repr=False, alias="bidConfiguration"
    )
    bid_date: Optional[datetime.date] = Field(None, alias="bidDate")
    shop_start: Optional[datetime.date] = Field(None, alias="shopStart")
    shop_end: Optional[datetime.date] = Field(None, alias="shopEnd")

    def as_write(self) -> TaskDispatcherShopInputWrite:
        """Convert this read version of task dispatcher shop input to the writing version."""
        return TaskDispatcherShopInputWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            process_id=self.process_id,
            process_step=self.process_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            bid_configuration=(
                self.bid_configuration.as_write()
                if isinstance(self.bid_configuration, DomainModel)
                else self.bid_configuration
            ),
            bid_date=self.bid_date,
            shop_start=self.shop_start,
            shop_end=self.shop_end,
        )

    def as_apply(self) -> TaskDispatcherShopInputWrite:
        """Convert this read version of task dispatcher shop input to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class TaskDispatcherShopInputWrite(DomainModelWrite):
    """This represents the writing version of task dispatcher shop input.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the task dispatcher shop input.
        data_record: The data record of the task dispatcher shop input node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
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
    function_call_id: str = Field(alias="functionCallId")
    bid_configuration: Union[BidConfigurationShopWrite, str, dm.NodeId, None] = Field(
        None, repr=False, alias="bidConfiguration"
    )
    bid_date: Optional[datetime.date] = Field(None, alias="bidDate")
    shop_start: Optional[datetime.date] = Field(None, alias="shopStart")
    shop_end: Optional[datetime.date] = Field(None, alias="shopEnd")

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
            TaskDispatcherShopInput, dm.ViewId("sp_powerops_models", "TaskDispatcherShopInput", "1")
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

        if self.bid_configuration is not None:
            properties["bidConfiguration"] = {
                "space": self.space if isinstance(self.bid_configuration, str) else self.bid_configuration.space,
                "externalId": (
                    self.bid_configuration
                    if isinstance(self.bid_configuration, str)
                    else self.bid_configuration.external_id
                ),
            }

        if self.bid_date is not None or write_none:
            properties["bidDate"] = self.bid_date.isoformat() if self.bid_date else None

        if self.shop_start is not None or write_none:
            properties["shopStart"] = self.shop_start.isoformat() if self.shop_start else None

        if self.shop_end is not None or write_none:
            properties["shopEnd"] = self.shop_end.isoformat() if self.shop_end else None

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

        if isinstance(self.bid_configuration, DomainModelWrite):
            other_resources = self.bid_configuration._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class TaskDispatcherShopInputApply(TaskDispatcherShopInputWrite):
    def __new__(cls, *args, **kwargs) -> TaskDispatcherShopInputApply:
        warnings.warn(
            "TaskDispatcherShopInputApply is deprecated and will be removed in v1.0. Use TaskDispatcherShopInputWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "TaskDispatcherShopInput.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class TaskDispatcherShopInputList(DomainModelList[TaskDispatcherShopInput]):
    """List of task dispatcher shop inputs in the read version."""

    _INSTANCE = TaskDispatcherShopInput

    def as_write(self) -> TaskDispatcherShopInputWriteList:
        """Convert these read versions of task dispatcher shop input to the writing versions."""
        return TaskDispatcherShopInputWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> TaskDispatcherShopInputWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class TaskDispatcherShopInputWriteList(DomainModelWriteList[TaskDispatcherShopInputWrite]):
    """List of task dispatcher shop inputs in the writing version."""

    _INSTANCE = TaskDispatcherShopInputWrite


class TaskDispatcherShopInputApplyList(TaskDispatcherShopInputWriteList): ...


def _create_task_dispatcher_shop_input_filter(
    view_id: dm.ViewId,
    process_id: str | list[str] | None = None,
    process_id_prefix: str | None = None,
    min_process_step: int | None = None,
    max_process_step: int | None = None,
    function_name: str | list[str] | None = None,
    function_name_prefix: str | None = None,
    function_call_id: str | list[str] | None = None,
    function_call_id_prefix: str | None = None,
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
    if isinstance(function_call_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("functionCallId"), value=function_call_id))
    if function_call_id and isinstance(function_call_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("functionCallId"), values=function_call_id))
    if function_call_id_prefix is not None:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("functionCallId"), value=function_call_id_prefix))
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
