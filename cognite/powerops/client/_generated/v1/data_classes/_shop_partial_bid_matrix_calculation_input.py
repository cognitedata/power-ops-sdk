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
    from ._price_production import PriceProduction, PriceProductionWrite
    from ._shop_based_partial_bid_configuration import (
        ShopBasedPartialBidConfiguration,
        ShopBasedPartialBidConfigurationWrite,
    )


__all__ = [
    "ShopPartialBidMatrixCalculationInput",
    "ShopPartialBidMatrixCalculationInputWrite",
    "ShopPartialBidMatrixCalculationInputApply",
    "ShopPartialBidMatrixCalculationInputList",
    "ShopPartialBidMatrixCalculationInputWriteList",
    "ShopPartialBidMatrixCalculationInputApplyList",
    "ShopPartialBidMatrixCalculationInputFields",
    "ShopPartialBidMatrixCalculationInputTextFields",
]


ShopPartialBidMatrixCalculationInputTextFields = Literal["process_id", "function_name", "function_call_id"]
ShopPartialBidMatrixCalculationInputFields = Literal[
    "process_id", "process_step", "function_name", "function_call_id", "bid_date"
]

_SHOPPARTIALBIDMATRIXCALCULATIONINPUT_PROPERTIES_BY_FIELD = {
    "process_id": "processId",
    "process_step": "processStep",
    "function_name": "functionName",
    "function_call_id": "functionCallId",
    "bid_date": "bidDate",
}


class ShopPartialBidMatrixCalculationInput(PartialBidMatrixCalculationInput):
    """This represents the reading version of shop partial bid matrix calculation input.

    It is used to when data is retrieved from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop partial bid matrix calculation input.
        data_record: The data record of the shop partial bid matrix calculation input node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        bid_date: The bid date
        bid_configuration: TODO description
        partial_bid_configuration: The partial bid configuration related to the bid calculation task
        price_production: An array of shop results with price/prod timeseries pairs for all plants included in the respective shop scenario
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types_temp", "ShopPartialBidMatrixCalculationInput"
    )
    partial_bid_configuration: Union[ShopBasedPartialBidConfiguration, str, dm.NodeId, None] = Field(
        None, repr=False, alias="partialBidConfiguration"
    )
    price_production: Union[list[PriceProduction], list[str], None] = Field(
        default=None, repr=False, alias="priceProduction"
    )

    def as_write(self) -> ShopPartialBidMatrixCalculationInputWrite:
        """Convert this read version of shop partial bid matrix calculation input to the writing version."""
        return ShopPartialBidMatrixCalculationInputWrite(
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
            price_production=[
                price_production.as_write() if isinstance(price_production, DomainModel) else price_production
                for price_production in self.price_production or []
            ],
        )

    def as_apply(self) -> ShopPartialBidMatrixCalculationInputWrite:
        """Convert this read version of shop partial bid matrix calculation input to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ShopPartialBidMatrixCalculationInputWrite(PartialBidMatrixCalculationInputWrite):
    """This represents the writing version of shop partial bid matrix calculation input.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop partial bid matrix calculation input.
        data_record: The data record of the shop partial bid matrix calculation input node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        bid_date: The bid date
        bid_configuration: TODO description
        partial_bid_configuration: The partial bid configuration related to the bid calculation task
        price_production: An array of shop results with price/prod timeseries pairs for all plants included in the respective shop scenario
    """

    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types_temp", "ShopPartialBidMatrixCalculationInput"
    )
    partial_bid_configuration: Union[ShopBasedPartialBidConfigurationWrite, str, dm.NodeId, None] = Field(
        None, repr=False, alias="partialBidConfiguration"
    )
    price_production: Union[list[PriceProductionWrite], list[str], None] = Field(
        default=None, repr=False, alias="priceProduction"
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
            ShopPartialBidMatrixCalculationInput,
            dm.ViewId("sp_powerops_models_temp", "ShopPartialBidMatrixCalculationInput", "1"),
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

        edge_type = dm.DirectRelationReference("sp_powerops_types_temp", "PriceProduction")
        for price_production in self.price_production or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=price_production,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
            )
            resources.extend(other_resources)

        if isinstance(self.bid_configuration, DomainModelWrite):
            other_resources = self.bid_configuration._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.partial_bid_configuration, DomainModelWrite):
            other_resources = self.partial_bid_configuration._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class ShopPartialBidMatrixCalculationInputApply(ShopPartialBidMatrixCalculationInputWrite):
    def __new__(cls, *args, **kwargs) -> ShopPartialBidMatrixCalculationInputApply:
        warnings.warn(
            "ShopPartialBidMatrixCalculationInputApply is deprecated and will be removed in v1.0. Use ShopPartialBidMatrixCalculationInputWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ShopPartialBidMatrixCalculationInput.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ShopPartialBidMatrixCalculationInputList(DomainModelList[ShopPartialBidMatrixCalculationInput]):
    """List of shop partial bid matrix calculation inputs in the read version."""

    _INSTANCE = ShopPartialBidMatrixCalculationInput

    def as_write(self) -> ShopPartialBidMatrixCalculationInputWriteList:
        """Convert these read versions of shop partial bid matrix calculation input to the writing versions."""
        return ShopPartialBidMatrixCalculationInputWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ShopPartialBidMatrixCalculationInputWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ShopPartialBidMatrixCalculationInputWriteList(DomainModelWriteList[ShopPartialBidMatrixCalculationInputWrite]):
    """List of shop partial bid matrix calculation inputs in the writing version."""

    _INSTANCE = ShopPartialBidMatrixCalculationInputWrite


class ShopPartialBidMatrixCalculationInputApplyList(ShopPartialBidMatrixCalculationInputWriteList): ...


def _create_shop_partial_bid_matrix_calculation_input_filter(
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
