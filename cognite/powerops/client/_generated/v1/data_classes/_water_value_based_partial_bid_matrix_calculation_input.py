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
from ._partial_bid_matrix_calculation_input import (
    PartialBidMatrixCalculationInput,
    PartialBidMatrixCalculationInputWrite,
)

if TYPE_CHECKING:
    from ._bid_configuration import BidConfiguration, BidConfigurationWrite
    from ._water_value_based_partial_bid_configuration import (
        WaterValueBasedPartialBidConfiguration,
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


WaterValueBasedPartialBidMatrixCalculationInputTextFields = Literal["process_id", "function_name", "function_call_id"]
WaterValueBasedPartialBidMatrixCalculationInputFields = Literal[
    "process_id", "process_step", "function_name", "function_call_id", "bid_date"
]

_WATERVALUEBASEDPARTIALBIDMATRIXCALCULATIONINPUT_PROPERTIES_BY_FIELD = {
    "process_id": "processId",
    "process_step": "processStep",
    "function_name": "functionName",
    "function_call_id": "functionCallId",
    "bid_date": "bidDate",
}


class WaterValueBasedPartialBidMatrixCalculationInput(PartialBidMatrixCalculationInput):
    """This represents the reading version of water value based partial bid matrix calculation input.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the water value based partial bid matrix calculation input.
        data_record: The data record of the water value based partial bid matrix calculation input node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        bid_date: The bid date
        bid_configuration: TODO description
        partial_bid_configuration: The partial bid configuration related to the bid calculation task
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types_temp", "WaterValueBasedPartialBidMatrixCalculationInput"
    )
    partial_bid_configuration: Union[WaterValueBasedPartialBidConfiguration, str, dm.NodeId, None] = Field(
        None, repr=False, alias="partialBidConfiguration"
    )

    def as_write(self) -> WaterValueBasedPartialBidMatrixCalculationInputWrite:
        """Convert this read version of water value based partial bid matrix calculation input to the writing version."""
        return WaterValueBasedPartialBidMatrixCalculationInputWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            process_id=self.process_id,
            process_step=self.process_step,
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
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        bid_date: The bid date
        bid_configuration: TODO description
        partial_bid_configuration: The partial bid configuration related to the bid calculation task
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types_temp", "WaterValueBasedPartialBidMatrixCalculationInput"
    )
    partial_bid_configuration: Union[WaterValueBasedPartialBidConfigurationWrite, str, dm.NodeId, None] = Field(
        None, repr=False, alias="partialBidConfiguration"
    )

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
            WaterValueBasedPartialBidMatrixCalculationInput,
            dm.ViewId("sp_powerops_models_temp", "WaterValueBasedPartialBidMatrixCalculationInput", "1"),
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
    process_id: str | list[str] | None = None,
    process_id_prefix: str | None = None,
    min_process_step: int | None = None,
    max_process_step: int | None = None,
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
