import ast
import json
from typing import ClassVar, List

from cognite.client.data_classes import Asset
from pydantic import Field, validator

from cognite.powerops.data_classes.config_model import Configuration
from cognite.powerops.data_classes.reserve_scenario import Auction


class RKOMBidCombinationConfig(Configuration):
    auction: Auction = Field(alias="bid_auction")
    name: str = Field("default", alias="bid_combination_name")
    rkom_bid_config_external_ids: List[str] = Field(alias="bid_rkom_bid_configs")
    parent_external_id: ClassVar[str] = "rkom_bid_combination_configurations"

    @validator("auction", pre=True)
    def to_enum(cls, value):
        return Auction[value] if isinstance(value, str) else value

    @validator("rkom_bid_config_external_ids", pre=True)
    def parse_string(cls, value):
        return [external_id for external_id in ast.literal_eval(value)] if isinstance(value, str) else value

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
