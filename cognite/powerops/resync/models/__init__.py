from __future__ import annotations

from cognite.powerops.resync.models.base import Model

from .v1 import CogShop1Asset, MarketModel, ProductionModel
from .v2 import PowerAssetModelDM

V1_MODELS: list[type[Model]] = [ProductionModel, MarketModel, CogShop1Asset]
V2_MODELS: list[type[Model]] = [PowerAssetModelDM]

__all__ = ["ProductionModel", "MarketModel", "CogShop1Asset", "PowerAssetModelDM", "V1_MODELS", "V2_MODELS"]
