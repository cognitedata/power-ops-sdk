from __future__ import annotations

from typing import cast

from cognite.powerops.cdf_labels import AssetLabel, RelationshipLabel
from cognite.powerops.resync.config.resource_collection import ResourceCollection
from cognite.powerops.resync.config.resync_config import ReSyncConfig
from cognite.powerops.resync.models.base import AssetModel, DataModel, Model

from .to_cogshop_model import to_cogshop_asset_model, to_cogshop_data_model
from .to_market_model import to_benchmark_data_model, to_dayahead_data_model, to_market_asset_model, to_rkom_data_model
from .to_production_model import to_production_data_model, to_production_model


def transform(
    config: ReSyncConfig,
    market_name: str,
    model_names: set[str],
) -> tuple[ResourceCollection, list[Model]]:
    asset_models: list[AssetModel] = []
    data_models: list[DataModel] = []

    # The Production model is a prerequisite for the Market and CogShop models
    has_asset_model = any("Asset" in m for m in model_names)
    if has_asset_model:
        production_model = to_production_model(config.production)
        if "ProductionAsset" in model_names:
            asset_models.append(production_model)
        if "MarketAsset" in model_names:
            market_model = to_market_asset_model(config.market, production_model.price_areas, market_name)
            settings = config.settings
            market_model.set_root_asset(
                settings.shop_service_url,
                settings.organization_subdomain,
                settings.tenant_id,
                production_model.root_asset.external_id,
            )
            asset_models.append(market_model)
        if "CogShopAsset" in model_names:
            cogshop_model = to_cogshop_asset_model(config.cogshop, production_model.watercourses)
            asset_models.append(cogshop_model)

    has_data_model = any("DataModel" in m for m in model_names)
    if has_data_model:
        # The production model is a prerequisite for the CogShop and Market models
        production__data_model = to_production_data_model(config.production)
        if "ProductionDataModel" in model_names:
            data_models.append(production__data_model)
        if "CogShopDataModel" in model_names:
            cogshop_data_model = to_cogshop_data_model(
                config.cogshop, config.production.watercourses, config.settings.shop_version
            )
            data_models.append(cogshop_data_model)
        if "BenchmarkMarketDataModel" in model_names or "DayAheadMarketDataModel" in model_names:
            benchmark_market_model = to_benchmark_data_model(config.market.benchmarks)
            if "BenchmarkMarketDataModel" in model_names:
                data_models.append(benchmark_market_model)
            if "DayAheadMarketDataModel" in model_names:
                dayahead_benchmark = benchmark_market_model.benchmarking[0]
                dayahead_bid = benchmark_market_model.bids[cast(str, dayahead_benchmark.bid)]
                day_ahead_market_model = to_dayahead_data_model(
                    config.market, dayahead_benchmark, dayahead_bid, production__data_model.price_areas
                )
                data_models.append(day_ahead_market_model)
        if "RKOMMarketDataModel" in model_names:
            rkom_market_model = to_rkom_data_model(config.market, market_name)
            data_models.append(rkom_market_model)

    collection = ResourceCollection()
    if has_asset_model:
        labels = AssetLabel.as_label_definitions() + RelationshipLabel.as_label_definitions()
        collection.add(labels)
    all_models: list[Model] = cast(list[Model], asset_models) + cast(list[Model], data_models)
    for model in all_models:
        collection.add(model.sequences())
        collection.add(model.files())

    for asset_model in asset_models:
        collection.add(asset_model.parent_assets())
        collection.add(asset_model.assets())
        collection.add(asset_model.relationships())
    for data_model in data_models:
        collection.add(data_model.instances())
    return collection, all_models
