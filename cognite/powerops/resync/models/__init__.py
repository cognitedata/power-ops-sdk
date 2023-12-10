from __future__ import annotations

from cognite.powerops.resync.models.base import Model

from . import migration
from .v1 import CogShop1Asset, MarketModel, ProductionModel
from .v2 import (
    BenchmarkMarketDataModel,
    CogShopDataModel,
    DayAheadMarketDataModel,
    PowerAssetModelDM,
    ProductionModelDM,
    RKOMMarketDataModel,
)

V1_MODELS: list[type[Model]] = [ProductionModel, MarketModel, CogShop1Asset]
V2_MODELS: list[type[Model]] = [ProductionModelDM, PowerAssetModelDM]

__all__ = [
    "ProductionModel",
    "MarketModel",
    "CogShop1Asset",
    "ProductionModelDM",
    "CogShopDataModel",
    "BenchmarkMarketDataModel",
    "DayAheadMarketDataModel",
    "RKOMMarketDataModel",
    "PowerAssetModelDM",
    "V1_MODELS",
    "V2_MODELS",
    "migration",
]
