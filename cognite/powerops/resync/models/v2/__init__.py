"""
This package contains the v2 version of the models in the resync package. All of these models
are based on Data Modeling in CDF.

* ProductionModel - This is the data model version of the ProductionModel in v1.
* Dayahead - This is the data model version of the Dayahead part of the MarketModel in v1.
* RKOM - This is the data model version of the RKOM part of the MarketModel in v1.
* Benchmarking - This is the data model version of the Benchmarking part of the MarketModel in v1.
* CogSHOP - This is the updated version of the CogShop1Asset in v1.
"""

from .capacity_bids import CapacityBidModel
from .cogshop import CogShopDataModel
from .graphql_schemas import GRAPHQL_MODELS
from .market_dm import AFRRMarket, BenchmarkMarketDataModel, DayAheadMarketDataModel, RKOMMarketDataModel
from .production_dm import ProductionModelDM

__all__ = [
    "GRAPHQL_MODELS",
    "CogShopDataModel",
    "ProductionModelDM",
    "DayAheadMarketDataModel",
    "RKOMMarketDataModel",
    "BenchmarkMarketDataModel",
    "AFRRMarket",
    "CapacityBidModel",
]
