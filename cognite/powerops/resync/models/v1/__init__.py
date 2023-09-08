"""
This package contains the v1 version of the models in the resync package. There are three legacy models:

* ProductionModel - This is an asset-based model for the physical assets in the production system.
* MarketModel - This is an asset-based model for the physical assets in the market system.
* CogShop1Asset - This is the first iteration of the data model for CogShop. It is expected to be deprecated soon.
"""
from .cogshop import CogShop1Asset
from .graphql_schemas import GRAPHQL_MODELS
from .market import (
    BenchmarkBid,
    BenchmarkProcess,
    DayAheadBid,
    DayAheadProcess,
    MarketModel,
    NordPoolMarket,
    RKOMBid,
    RKOMBidCombination,
    RKOMCombinationBid,
    RKOMMarket,
    RKOMPlants,
    RKOMProcess,
)
from .production import Generator, Plant, PriceArea, ProductionModel, Reservoir, Watercourse

__all__ = [
    "GRAPHQL_MODELS",
    "CogShop1Asset",
    "ProductionModel",
    "Generator",
    "Plant",
    "PriceArea",
    "Reservoir",
    "Watercourse",
    "MarketModel",
    "BenchmarkBid",
    "BenchmarkProcess",
    "DayAheadBid",
    "DayAheadProcess",
    "NordPoolMarket",
    "RKOMBid",
    "RKOMBidCombination",
    "RKOMCombinationBid",
    "RKOMMarket",
    "RKOMPlants",
    "RKOMProcess",
]
