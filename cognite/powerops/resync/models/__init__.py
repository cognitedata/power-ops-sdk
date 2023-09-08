from .v1 import ProductionModel, MarketModel, CogShop1Asset
from .v2 import (
    ProductionModelDM,
    CogShopDataModel,
    BenchmarkMarketDataModel,
    DayAheadMarketDataModel,
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
