from __future__ import annotations

import datetime
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

if TYPE_CHECKING:
    from ._market_configuration import MarketConfiguration, MarketConfigurationGraphQL, MarketConfigurationWrite
    from ._plant_shop import PlantShop, PlantShopGraphQL, PlantShopWrite
    from ._shop_result_price_prod import SHOPResultPriceProd, SHOPResultPriceProdGraphQL, SHOPResultPriceProdWrite


__all__ = [
    "ShopPartialBidCalculationInput",
    "ShopPartialBidCalculationInputWrite",
    "ShopPartialBidCalculationInputApply",
    "ShopPartialBidCalculationInputList",
    "ShopPartialBidCalculationInputWriteList",
    "ShopPartialBidCalculationInputApplyList",
    "ShopPartialBidCalculationInputFields",
    "ShopPartialBidCalculationInputTextFields",
]


ShopPartialBidCalculationInputTextFields = Literal["process_id", "function_name", "function_call_id"]
ShopPartialBidCalculationInputFields = Literal[
    "process_id", "process_step", "function_name", "function_call_id", "step_enabled", "bid_date"
]

_SHOPPARTIALBIDCALCULATIONINPUT_PROPERTIES_BY_FIELD = {
    "process_id": "processId",
    "process_step": "processStep",
    "function_name": "functionName",
    "function_call_id": "functionCallId",
    "step_enabled": "stepEnabled",
    "bid_date": "bidDate",
}


class ShopPartialBidCalculationInputGraphQL(GraphQLCore):
    """This represents the reading version of shop partial bid calculation input, used
    when data is retrieved from CDF using GraphQL.

    It is used when retrieving data from CDF using GraphQL.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop partial bid calculation input.
        data_record: The data record of the shop partial bid calculation input node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        plant: The plant to calculate the partial bid for. Extract price/prod timeseries from Shop Results
        market_configuration: The market configuration to be used to generate the partial bid matrix
        step_enabled: Whether the step is enabled or not
        bid_date: The bid date
        shop_result_price_prod: An array of shop results with price/prod timeserires pairs for all plants included in the respective shop scenario
    """

    view_id = dm.ViewId("sp_powerops_models", "ShopPartialBidCalculationInput", "1")
    process_id: Optional[str] = Field(None, alias="processId")
    process_step: Optional[int] = Field(None, alias="processStep")
    function_name: Optional[str] = Field(None, alias="functionName")
    function_call_id: Optional[str] = Field(None, alias="functionCallId")
    plant: Optional[PlantShopGraphQL] = Field(None, repr=False)
    market_configuration: Optional[MarketConfigurationGraphQL] = Field(None, repr=False, alias="marketConfiguration")
    step_enabled: Optional[bool] = Field(None, alias="stepEnabled")
    bid_date: Optional[datetime.date] = Field(None, alias="bidDate")
    shop_result_price_prod: Optional[list[SHOPResultPriceProdGraphQL]] = Field(
        default=None, repr=False, alias="shopResultPriceProd"
    )

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

    @field_validator("plant", "market_configuration", "shop_result_price_prod", mode="before")
    def parse_graphql(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "items" in value:
            return value["items"]
        return value

    def as_read(self) -> ShopPartialBidCalculationInput:
        """Convert this GraphQL format of shop partial bid calculation input to the reading format."""
        if self.data_record is None:
            raise ValueError("This object cannot be converted to a read format because it lacks a data record.")
        return ShopPartialBidCalculationInput(
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
            plant=self.plant.as_read() if isinstance(self.plant, GraphQLCore) else self.plant,
            market_configuration=(
                self.market_configuration.as_read()
                if isinstance(self.market_configuration, GraphQLCore)
                else self.market_configuration
            ),
            step_enabled=self.step_enabled,
            bid_date=self.bid_date,
            shop_result_price_prod=[
                (
                    shop_result_price_prod.as_read()
                    if isinstance(shop_result_price_prod, GraphQLCore)
                    else shop_result_price_prod
                )
                for shop_result_price_prod in self.shop_result_price_prod or []
            ],
        )

    def as_write(self) -> ShopPartialBidCalculationInputWrite:
        """Convert this GraphQL format of shop partial bid calculation input to the writing format."""
        return ShopPartialBidCalculationInputWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=0),
            process_id=self.process_id,
            process_step=self.process_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            plant=self.plant.as_write() if isinstance(self.plant, DomainModel) else self.plant,
            market_configuration=(
                self.market_configuration.as_write()
                if isinstance(self.market_configuration, DomainModel)
                else self.market_configuration
            ),
            step_enabled=self.step_enabled,
            bid_date=self.bid_date,
            shop_result_price_prod=[
                (
                    shop_result_price_prod.as_write()
                    if isinstance(shop_result_price_prod, DomainModel)
                    else shop_result_price_prod
                )
                for shop_result_price_prod in self.shop_result_price_prod or []
            ],
        )


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
        function_call_id: The function call id
        plant: The plant to calculate the partial bid for. Extract price/prod timeseries from Shop Results
        market_configuration: The market configuration to be used to generate the partial bid matrix
        step_enabled: Whether the step is enabled or not
        bid_date: The bid date
        shop_result_price_prod: An array of shop results with price/prod timeserires pairs for all plants included in the respective shop scenario
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "ShopPartialBidCalculationInput"
    )
    process_id: str = Field(alias="processId")
    process_step: int = Field(alias="processStep")
    function_name: str = Field(alias="functionName")
    function_call_id: str = Field(alias="functionCallId")
    plant: Union[PlantShop, str, dm.NodeId, None] = Field(None, repr=False)
    market_configuration: Union[MarketConfiguration, str, dm.NodeId, None] = Field(
        None, repr=False, alias="marketConfiguration"
    )
    step_enabled: Optional[bool] = Field(None, alias="stepEnabled")
    bid_date: Optional[datetime.date] = Field(None, alias="bidDate")
    shop_result_price_prod: Union[list[SHOPResultPriceProd], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="shopResultPriceProd"
    )

    def as_write(self) -> ShopPartialBidCalculationInputWrite:
        """Convert this read version of shop partial bid calculation input to the writing version."""
        return ShopPartialBidCalculationInputWrite(
            space=self.space,
            external_id=self.external_id,
            data_record=DataRecordWrite(existing_version=self.data_record.version),
            process_id=self.process_id,
            process_step=self.process_step,
            function_name=self.function_name,
            function_call_id=self.function_call_id,
            plant=self.plant.as_write() if isinstance(self.plant, DomainModel) else self.plant,
            market_configuration=(
                self.market_configuration.as_write()
                if isinstance(self.market_configuration, DomainModel)
                else self.market_configuration
            ),
            step_enabled=self.step_enabled,
            bid_date=self.bid_date,
            shop_result_price_prod=[
                (
                    shop_result_price_prod.as_write()
                    if isinstance(shop_result_price_prod, DomainModel)
                    else shop_result_price_prod
                )
                for shop_result_price_prod in self.shop_result_price_prod or []
            ],
        )

    def as_apply(self) -> ShopPartialBidCalculationInputWrite:
        """Convert this read version of shop partial bid calculation input to the writing version."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ShopPartialBidCalculationInputWrite(DomainModelWrite):
    """This represents the writing version of shop partial bid calculation input.

    It is used to when data is sent to CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the shop partial bid calculation input.
        data_record: The data record of the shop partial bid calculation input node.
        process_id: The process associated with the function execution
        process_step: This is the step in the process.
        function_name: The name of the function
        function_call_id: The function call id
        plant: The plant to calculate the partial bid for. Extract price/prod timeseries from Shop Results
        market_configuration: The market configuration to be used to generate the partial bid matrix
        step_enabled: Whether the step is enabled or not
        bid_date: The bid date
        shop_result_price_prod: An array of shop results with price/prod timeserires pairs for all plants included in the respective shop scenario
    """

    space: str = DEFAULT_INSTANCE_SPACE
    node_type: Union[dm.DirectRelationReference, None] = dm.DirectRelationReference(
        "sp_powerops_types", "ShopPartialBidCalculationInput"
    )
    process_id: str = Field(alias="processId")
    process_step: int = Field(alias="processStep")
    function_name: str = Field(alias="functionName")
    function_call_id: str = Field(alias="functionCallId")
    plant: Union[PlantShopWrite, str, dm.NodeId, None] = Field(None, repr=False)
    market_configuration: Union[MarketConfigurationWrite, str, dm.NodeId, None] = Field(
        None, repr=False, alias="marketConfiguration"
    )
    step_enabled: Optional[bool] = Field(None, alias="stepEnabled")
    bid_date: Optional[datetime.date] = Field(None, alias="bidDate")
    shop_result_price_prod: Union[list[SHOPResultPriceProdWrite], list[str], list[dm.NodeId], None] = Field(
        default=None, repr=False, alias="shopResultPriceProd"
    )

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
            ShopPartialBidCalculationInput, dm.ViewId("sp_powerops_models", "ShopPartialBidCalculationInput", "1")
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

        if self.plant is not None:
            properties["plant"] = {
                "space": self.space if isinstance(self.plant, str) else self.plant.space,
                "externalId": self.plant if isinstance(self.plant, str) else self.plant.external_id,
            }

        if self.market_configuration is not None:
            properties["marketConfiguration"] = {
                "space": self.space if isinstance(self.market_configuration, str) else self.market_configuration.space,
                "externalId": (
                    self.market_configuration
                    if isinstance(self.market_configuration, str)
                    else self.market_configuration.external_id
                ),
            }

        if self.step_enabled is not None or write_none:
            properties["stepEnabled"] = self.step_enabled

        if self.bid_date is not None or write_none:
            properties["bidDate"] = self.bid_date.isoformat() if self.bid_date else None

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

        edge_type = dm.DirectRelationReference("sp_powerops_types", "SHOPResultPriceProd")
        for shop_result_price_prod in self.shop_result_price_prod or []:
            other_resources = DomainRelationWrite.from_edge_to_resources(
                cache,
                start_node=self,
                end_node=shop_result_price_prod,
                edge_type=edge_type,
                view_by_read_class=view_by_read_class,
                write_none=write_none,
                allow_version_increase=allow_version_increase,
            )
            resources.extend(other_resources)

        if isinstance(self.plant, DomainModelWrite):
            other_resources = self.plant._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        if isinstance(self.market_configuration, DomainModelWrite):
            other_resources = self.market_configuration._to_instances_write(cache, view_by_read_class)
            resources.extend(other_resources)

        return resources


class ShopPartialBidCalculationInputApply(ShopPartialBidCalculationInputWrite):
    def __new__(cls, *args, **kwargs) -> ShopPartialBidCalculationInputApply:
        warnings.warn(
            "ShopPartialBidCalculationInputApply is deprecated and will be removed in v1.0. Use ShopPartialBidCalculationInputWrite instead."
            "The motivation for this change is that Write is a more descriptive name for the writing version of the"
            "ShopPartialBidCalculationInput.",
            UserWarning,
            stacklevel=2,
        )
        return super().__new__(cls)


class ShopPartialBidCalculationInputList(DomainModelList[ShopPartialBidCalculationInput]):
    """List of shop partial bid calculation inputs in the read version."""

    _INSTANCE = ShopPartialBidCalculationInput

    def as_write(self) -> ShopPartialBidCalculationInputWriteList:
        """Convert these read versions of shop partial bid calculation input to the writing versions."""
        return ShopPartialBidCalculationInputWriteList([node.as_write() for node in self.data])

    def as_apply(self) -> ShopPartialBidCalculationInputWriteList:
        """Convert these read versions of primitive nullable to the writing versions."""
        warnings.warn(
            "as_apply is deprecated and will be removed in v1.0. Use as_write instead.",
            UserWarning,
            stacklevel=2,
        )
        return self.as_write()


class ShopPartialBidCalculationInputWriteList(DomainModelWriteList[ShopPartialBidCalculationInputWrite]):
    """List of shop partial bid calculation inputs in the writing version."""

    _INSTANCE = ShopPartialBidCalculationInputWrite


class ShopPartialBidCalculationInputApplyList(ShopPartialBidCalculationInputWriteList): ...


def _create_shop_partial_bid_calculation_input_filter(
    view_id: dm.ViewId,
    process_id: str | list[str] | None = None,
    process_id_prefix: str | None = None,
    min_process_step: int | None = None,
    max_process_step: int | None = None,
    function_name: str | list[str] | None = None,
    function_name_prefix: str | None = None,
    function_call_id: str | list[str] | None = None,
    function_call_id_prefix: str | None = None,
    plant: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    market_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    step_enabled: bool | None = None,
    min_bid_date: datetime.date | None = None,
    max_bid_date: datetime.date | None = None,
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
    if market_configuration and isinstance(market_configuration, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("marketConfiguration"),
                value={"space": DEFAULT_INSTANCE_SPACE, "externalId": market_configuration},
            )
        )
    if market_configuration and isinstance(market_configuration, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("marketConfiguration"),
                value={"space": market_configuration[0], "externalId": market_configuration[1]},
            )
        )
    if market_configuration and isinstance(market_configuration, list) and isinstance(market_configuration[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("marketConfiguration"),
                values=[{"space": DEFAULT_INSTANCE_SPACE, "externalId": item} for item in market_configuration],
            )
        )
    if market_configuration and isinstance(market_configuration, list) and isinstance(market_configuration[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("marketConfiguration"),
                values=[{"space": item[0], "externalId": item[1]} for item in market_configuration],
            )
        )
    if isinstance(step_enabled, bool):
        filters.append(dm.filters.Equals(view_id.as_property_ref("stepEnabled"), value=step_enabled))
    if min_bid_date is not None or max_bid_date is not None:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("bidDate"),
                gte=min_bid_date.isoformat() if min_bid_date else None,
                lte=max_bid_date.isoformat() if max_bid_date else None,
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
