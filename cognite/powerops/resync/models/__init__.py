from __future__ import annotations

from cognite.powerops.resync.models.base import Model

from . import migration
from .v1 import CogShop1Asset, MarketModel, ProductionModel
from .v2 import (
    AFRRMarket,
    BenchmarkMarketDataModel,
    CapacityBidModel,
    CogShopDataModel,
    DayAheadMarketDataModel,
    ProductionModelDM,
    RKOMMarketDataModel,
)

V1_MODELS: list[type[Model]] = [ProductionModel, MarketModel, CogShop1Asset]
V2_MODELS: list[type[Model]] = [
    ProductionModelDM,
    CogShopDataModel,
    BenchmarkMarketDataModel,
    DayAheadMarketDataModel,
    RKOMMarketDataModel,
    AFRRMarket,
    CapacityBidModel,
]

__all__ = [
    "ProductionModel",
    "MarketModel",
    "CogShop1Asset",
    "ProductionModelDM",
    "CogShopDataModel",
    "BenchmarkMarketDataModel",
    "DayAheadMarketDataModel",
    "RKOMMarketDataModel",
    "V1_MODELS",
    "V2_MODELS",
    "migration",
]
