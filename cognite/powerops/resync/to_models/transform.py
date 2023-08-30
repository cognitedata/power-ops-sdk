from __future__ import annotations

from typing import cast, Type

from cognite.powerops.resync.config.resync_config import ReSyncConfig
from cognite.powerops.resync.models.base import AssetModel, DataModel, Model
from cognite.powerops.resync import models
from .to_cogshop_model import to_cogshop_asset_model, to_cogshop_data_model
from .to_market_model import to_benchmark_data_model, to_dayahead_data_model, to_market_asset_model, to_rkom_data_model
from .to_production_model import to_production_data_model, to_production_model


def transform(
    config: ReSyncConfig,
    market_name: str,
    model_types: set[Type[Model]],
) -> list[Model]:
    asset_models: list[AssetModel] = []
    data_models: list[DataModel] = []

    # The Production model is a prerequisite for the Market and CogShop models
    has_asset_model = any(issubclass(m, (AssetModel, models.CogShop1Asset)) for m in model_types)
    if has_asset_model:
        production_model = to_production_model(config.production)
        if models.ProductionModel in model_types:
            asset_models.append(production_model)
        if models.MarketModel in model_types:
            market_model = to_market_asset_model(config.market, production_model.price_areas, market_name)
            settings = config.settings
            market_model.set_root_asset(
                settings.shop_service_url,
                settings.organization_subdomain,
                settings.tenant_id,
                production_model.root_asset.external_id,
            )
            asset_models.append(market_model)
        if models.CogShop1Asset in model_types:
            cogshop_model = to_cogshop_asset_model(
                config.cogshop, production_model.watercourses, config.settings.shop_version
            )
            data_models.append(cogshop_model)

    has_data_model = any(issubclass(m, DataModel) for m in model_types)
    if has_data_model:
        # The production model is a prerequisite for the CogShop and Market models
        production__data_model = to_production_data_model(config.production)
        if models.ProductionModelDM in model_types:
            data_models.append(production__data_model)
        if models.CogShopDataModel in model_types:
            cogshop_data_model = to_cogshop_data_model(
                config.cogshop, config.production.watercourses, config.settings.shop_version
            )
            data_models.append(cogshop_data_model)
        if models.BenchmarkMarketDataModel in model_types or models.DayAheadMarketDataModel in model_types:
            benchmark_market_model = to_benchmark_data_model(config.market.benchmarks)
            if models.BenchmarkMarketDataModel in model_types:
                data_models.append(benchmark_market_model)
            if models.DayAheadMarketDataModel in model_types:
                dayahead_benchmark = benchmark_market_model.benchmarking[0]
                dayahead_bid = benchmark_market_model.bids[cast(str, dayahead_benchmark.bid)]
                day_ahead_market_model = to_dayahead_data_model(
                    config.market, dayahead_benchmark, dayahead_bid, production__data_model.price_areas
                )
                data_models.append(day_ahead_market_model)
        if models.RKOMMarketDataModel in model_types:
            rkom_market_model = to_rkom_data_model(config.market, market_name)
            data_models.append(rkom_market_model)

    all_models: list[Model] = cast(list[Model], asset_models) + cast(list[Model], data_models)
    return all_models
