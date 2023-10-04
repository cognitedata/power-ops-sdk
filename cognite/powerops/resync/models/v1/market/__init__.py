from typing import ClassVar, Optional

from cognite.client.data_classes import Asset
from pydantic import field_validator
from pydantic.dataclasses import Field

from cognite.powerops.resync.models.base import AssetModel, T_Asset_Type

from .base import Bid, DateTransformation, Market, Process, ShopTransformation
from .benchmark import BenchmarkBid, BenchmarkProcess, ProductionPlanTimeSeries
from .dayahead import DayAheadBid, DayAheadProcess, NordPoolMarket
from .rkom import RKOMBid, RKOMBidCombination, RKOMCombinationBid, RKOMMarket, RKOMPlants, RKOMProcess


class MarketModel(AssetModel):
    root_asset: ClassVar[Optional[Asset]] = None
    nordpool_market: list[NordPoolMarket] = Field(default_factory=list)
    rkom_market: list[RKOMMarket] = Field(default_factory=list)
    dayahead_processes: list[DayAheadProcess] = Field(default_factory=list)
    benchmark_processes: list[BenchmarkProcess] = Field(default_factory=list)
    rkom_processes: list[RKOMProcess] = Field(default_factory=list)
    combinations: list[RKOMBidCombination] = Field(default_factory=list)

    @field_validator(
        "nordpool_market",
        "rkom_market",
        "dayahead_processes",
        "benchmark_processes",
        "rkom_processes",
        "combinations",
        mode="after",
    )
    def ordering(cls, value: list[T_Asset_Type]) -> list[T_Asset_Type]:
        # To ensure loading the production model always yields the same result, we sort the assets by external_id.
        return sorted(value, key=lambda x: x.external_id)

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

    def standardize(self) -> None:
        self.dayahead_processes = self.ordering(self.dayahead_processes)
        self.benchmark_processes = self.ordering(self.benchmark_processes)
        self.rkom_processes = self.ordering(self.rkom_processes)
        self.combinations = self.ordering(self.combinations)
        for field in [
            self.dayahead_processes,
            self.benchmark_processes,
            self.rkom_processes,
            self.combinations,
            self.nordpool_market,
            self.rkom_market,
        ]:
            for item in field:
                item.standardize()

    @property
    def processes(self):
        return self.dayahead_processes + self.benchmark_processes + self.rkom_processes


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
