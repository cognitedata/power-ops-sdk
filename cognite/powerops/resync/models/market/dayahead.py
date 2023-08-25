from __future__ import annotations


from typing import ClassVar, Optional, Union

from pydantic import Field, field_validator

from cognite.powerops.cdf_labels import AssetLabel
from cognite.powerops.resync.models.cdf_resources import CDFSequence

from .base import Bid, Market, Process, ShopTransformation
from cognite.powerops.resync.utils.serializer import try_load_dict


class DayAheadBid(Bid):
    is_default_config_for_price_area: bool = True
    main_scenario: str
    price_area: str
    price_scenarios: dict[str, str]
    no_shop: bool = False
    bid_process_configuration_name: str
    bid_matrix_generator_config_external_id: str
    market_config_external_id: str

    @field_validator("price_scenarios", mode="before")
    def parse_str(cls, value) -> dict:
        value = try_load_dict(value)
        if isinstance(value, list) and not value:
            return {}
        return value


class DayAheadProcess(Process):
    parent_external_id = "bid_process_configurations"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.BID_PROCESS_CONFIGURATION
    parent_description: ClassVar[str] = "Configurations used in bid matrix generation processes"
    shop: ShopTransformation
    bid: DayAheadBid
    bid_matrix_generator_config: Optional[CDFSequence] = None
    incremental_mapping: list[CDFSequence] = Field(default_factory=list)

    @field_validator("bid", mode="before")
    def parse_str(cls, value) -> dict:
        return try_load_dict(value)


class NordPoolMarket(Market):
    max_price: float
    min_price: float
    price_steps: int
    price_unit: str
    tick_size: float
    time_unit: str
    trade_lot: float
