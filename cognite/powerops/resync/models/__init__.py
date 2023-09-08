from .v1 import CogShop1Asset, MarketModel, ProductionModel
from .v2 import (
    BenchmarkMarketDataModel,
    CogShopDataModel,
    DayAheadMarketDataModel,
    ProductionModelDM,
    RKOMMarketDataModel,
)

V1_MODELS = [ProductionModel, MarketModel, CogShop1Asset]
V2_MODELS = [
    ProductionModelDM,
    CogShopDataModel,
    BenchmarkMarketDataModel,
    DayAheadMarketDataModel,
    RKOMMarketDataModel,
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
]
