from __future__ import annotations

from cognite.powerops.resync import models
from cognite.powerops.resync.config._main import ReSyncConfig
from cognite.powerops.resync.models.base import AssetModel, DataModel, Model
from cognite.powerops.resync.models.v1.config_to_model import (
    to_cogshop_asset_model,
    to_market_asset_model,
    to_production_model,
)
from cognite.powerops.resync.models.v2.config_to_model import to_powerasset_model, to_production_data_model


def transform(config: ReSyncConfig, market_name: str, model_types: set[type[Model]]) -> list[Model]:
    all_models: list[Model] = []

    # The Production model is a prerequisite for the Market and CogShop models
    has_asset_model = any(issubclass(m, (AssetModel, models.CogShop1Asset)) for m in model_types)
    if has_asset_model:
        production_model = to_production_model(config.production)
        if models.ProductionModel in model_types:
            all_models.append(production_model)
        if models.MarketModel in model_types or models.CogShop1Asset in model_types:
            market_model = to_market_asset_model(config.market, production_model.price_areas, market_name)
            settings = config.settings
            if production_model.root_asset.external_id is None:
                raise ValueError("The production model must have an external_id")
            market_model.set_root_asset(
                settings.shop_service_url,
                settings.organization_subdomain,
                settings.tenant_id,
                production_model.root_asset.external_id,
            )
            if models.MarketModel in model_types:
                all_models.append(market_model)
            if models.CogShop1Asset in model_types:
                cogshop_model = to_cogshop_asset_model(
                    config.cogshop,
                    production_model.watercourses,
                    config.settings.shop_version,
                    market_model.dayahead_processes,
                    market_model.rkom_processes,
                )
                all_models.append(cogshop_model)

    has_data_model = any(
        issubclass(m, (DataModel, models.PowerAssetModelDM)) and not issubclass(m, models.CogShop1Asset)
        for m in model_types
    )
    if has_data_model:
        # The production model is a prerequisite for the CogShop and Market models
        production__data_model = to_production_data_model(config.production)
        if models.ProductionModelDM in model_types:
            all_models.append(production__data_model)
        if models.PowerAssetModelDM in model_types:
            power_asset_model = to_powerasset_model.to_asset_data_model(config.production)
            all_models.append(power_asset_model)  # type: ignore[arg-type]

    return all_models
