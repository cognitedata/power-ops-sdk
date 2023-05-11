import json
from typing import ClassVar, List

from cognite.client.data_classes import Asset
from pydantic import BaseModel, validator

from cognite.powerops.data_classes.reserve_scenario import Auction


class RKOMBidCombinationConfig(BaseModel):
    auction: Auction
    name: str = "default"
    rkom_bid_config_external_ids: List[str]
    parent_external_id: ClassVar[str] = "rkom_bid_combination_configurations"

    @validator("auction", pre=True)
    def to_enum(cls, value):
        return Auction[value] if isinstance(value, str) else value

    @property
    def cdf_asset(self) -> Asset:
        sequence_external_id = f"RKOM_bid_combination_configuration_{self.auction.value}_{self.name}"

        return Asset(
            name=sequence_external_id.replace("_", " "),
            description="Defining which RKOM bid methods should be combined (into the total bid form)",
            external_id=sequence_external_id,
            metadata={
                "bid:auction": self.auction.value,
                "bid:combination_name": self.name,
                "bid:rkom_bid_configs": json.dumps(self.rkom_bid_config_external_ids),
            },
            parent_external_id=self.parent_external_id,
        )
