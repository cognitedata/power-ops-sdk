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
    from ._bid_matrix_raw import BidMatrixRaw, BidMatrixRawWrite
    from ._market_configuration import MarketConfiguration, MarketConfigurationWrite


__all__ = [
    "PartialPostProcessingInput",
    "PartialPostProcessingInputList",
    "PartialPostProcessingInputFields",
    "PartialPostProcessingInputTextFields",
]


PartialPostProcessingInputTextFields = Literal["process_id", "function_name"]
PartialPostProcessingInputFields = Literal["process_id", "process_step", "function_name"]

_PARTIALPOSTPROCESSINGINPUT_PROPERTIES_BY_FIELD = {
    "process_id": "processId",
    "process_step": "processStep",
    "function_name": "functionName",
}


class PartialPostProcessingInput(DomainModel):
    """This represents the reading version of partial post processing input.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the partial post processing input.
        data_record: The data record of the partial post processing input node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        market_config: The market config field.
        partial_bid_matrices_raw: The partial bid matrices that needs post processing.
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "PartialPostProcessingInput"
    )
    process_id: str = Field(alias="processId")
    process_step: int = Field(alias="processStep")
    function_name: str = Field(alias="functionName")
    market_config: Union[MarketConfiguration, str, dm.NodeId, None] = Field(None, repr=False, alias="marketConfig")
    partial_bid_matrices_raw: Union[list[BidMatrixRaw], list[str], None] = Field(
        default=None, repr=False, alias="partialBidMatricesRaw"
    )


class PartialPostProcessingInputList(DomainModelList[PartialPostProcessingInput]):
    """List of partial post processing inputs in the read version."""

    _INSTANCE = PartialPostProcessingInput


def _create_partial_post_processing_input_filter(
    view_id: dm.ViewId,
    process_id: str | list[str] | None = None,
    process_id_prefix: str | None = None,
    min_process_step: int | None = None,
    max_process_step: int | None = None,
    function_name: str | list[str] | None = None,
    function_name_prefix: str | None = None,
    market_config: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
    if market_config and isinstance(market_config, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("marketConfig"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": market_config},
            )
        )
    if market_config and isinstance(market_config, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("marketConfig"),
                value={"space": market_config[0], "externalId": market_config[1]},
            )
        )
    if market_config and isinstance(market_config, list) and isinstance(market_config[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("marketConfig"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in market_config],
            )
        )
    if market_config and isinstance(market_config, list) and isinstance(market_config[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("marketConfig"),
                values=[{"space": item[0], "externalId": item[1]} for item in market_config],
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
