from typing import ClassVar, Optional

from cognite.client.data_classes import Asset
from pydantic.dataclasses import Field

from cognite.powerops.resync.models.base import AssetModel

from .base import Bid, DateTransformation, Market, Process, ShopTransformation
from .benchmark import BenchmarkBid, BenchmarkProcess, ProductionPlanTimeSeries
from .dayahead import DayAheadBid, DayAheadProcess, NordPoolMarket
from .rkom import RKOMBid, RKOMBidCombination, RKOMCombinationBid, RKOMMarket, RKOMPlants, RKOMProcess


class MarketModel(AssetModel):
    root_asset: ClassVar[Optional[Asset]] = None
    markets: list[Market] = Field(default_factory=list)
    processes: list[Process] = Field(default_factory=list)
    combinations: list[RKOMBidCombination] = Field(default_factory=list)

    @classmethod
    def set_root_asset(
        cls, shop_service_url: str, organization_subdomain: str, tenant_id: str, core_root_asset_external_id: str
    ) -> None:
        if shop_service_url == "https://shop-staging.az-inso-powerops.cognite.ai/submit-run":
            customer = "cognite"
        else:
            customer = organization_subdomain

        cls.root_asset = Asset(
            parent_external_id=core_root_asset_external_id,
            external_id="configurations",
            name="Configurations",
            description="Configurations used for PowerOps",
            metadata={
                "shop_service_url": shop_service_url,
                "organization_subdomain": organization_subdomain,
                "customer": customer,
                "tenant_id": tenant_id,
            },
        )

    @classmethod
    def from_cdf(cls, client) -> "MarketModel":
        # TODO:
        # * Missing a from `from_asset` method on each AssetType
        # * Handle the rewrite from `type_` to `parent_external_id` on AssetType
        raise NotImplementedError()


__all__ = [
    "RKOMBid",
    "RKOMBidCombination",
    "RKOMMarket",
    "RKOMProcess",
    "RKOMCombinationBid",
    "RKOMPlants",
    "DayAheadBid",
    "NordPoolMarket",
    "DayAheadProcess",
    "BenchmarkBid",
    "BenchmarkProcess",
    "ProductionPlanTimeSeries",
    "Market",
    "DateTransformation",
    "ShopTransformation",
    "Bid",
    "Process",
    "MarketModel",
]
