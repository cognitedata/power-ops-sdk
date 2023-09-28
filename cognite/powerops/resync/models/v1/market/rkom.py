from __future__ import annotations

from typing import ClassVar, Optional, Union

from pydantic import Field, field_validator

from cognite.powerops.cdf_labels import AssetLabel
from cognite.powerops.resync.models.base import AssetType, CDFSequence, NonAssetType
from cognite.powerops.utils.serialization import try_load_list

from .base import Bid, Market, Process, ShopTransformation


class RKOMBid(Bid):
    auction: str
    block: str
    method: str
    minimum_price: str
    price_premium: str
    product: str
    watercourse: str
    price_scenarios: str
    reserve_scenarios: str


class RKOMPlants(NonAssetType):
    plants: str


class RKOMProcess(Process):
    parent_external_id: ClassVar[str] = "rkom_bid_process_configurations"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.RKOM_BID_CONFIGURATION
    parent_description: ClassVar[str] = "Configurations used in RKOM bid generation processes"
    shop: ShopTransformation
    bid: RKOMBid
    process_events: list[str] = Field(default_factory=list)
    timezone: str
    rkom: RKOMPlants
    incremental_mapping: list[CDFSequence] = Field(default_factory=list)

    @field_validator("incremental_mapping", mode="after")
    def ordering(cls, value: list[CDFSequence]) -> list[CDFSequence]:
        return sorted(value, key=lambda x: x.external_id)

    @field_validator("process_events", mode="before")
    def ordering_events(cls, value: list[str]) -> list[str]:
        return sorted(value)

    @field_validator("process_events", mode="before")
    def parse_str(cls, value) -> list:
        return try_load_list(value)

    def standardize(self) -> None:
        self.incremental_mapping = self.ordering(self.incremental_mapping)
        self.process_events = self.ordering_events(self.process_events)


class RKOMCombinationBid(NonAssetType):
    auction: str
    combination_name: str
    rkom_bid_configs: list[str]

    @field_validator("rkom_bid_configs", mode="before")
    def parse_str(cls, value) -> list:
        return try_load_list(value)

    @field_validator("rkom_bid_configs", mode="after")
    def ordering(cls, value: list[str]) -> list[str]:
        return sorted(value)

    def standardize(self) -> None:
        self.rkom_bid_configs = self.ordering(self.rkom_bid_configs)


class RKOMBidCombination(AssetType):
    parent_external_id: ClassVar[str] = "rkom_bid_combination_configurations"
    label: ClassVar[str] = AssetLabel.RKOM_BID_CONFIGURATION
    parent_description: ClassVar[str] = "Configurations for which bids should be combined into a total RKOM bid form"
    bid: RKOMCombinationBid

    def standardize(self) -> None:
        self.bid.standardize()


class RKOMMarket(Market):
    # Temporary optional to fix that this is missing in CDF.
    start_of_week: Optional[int] = None
