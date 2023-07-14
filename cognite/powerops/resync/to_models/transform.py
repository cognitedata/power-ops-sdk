from __future__ import annotations

from cognite.powerops.cdf_labels import AssetLabel, RelationshipLabel
from cognite.powerops.resync.config.resource_collection import ResourceCollection
from cognite.powerops.resync.config.resync_config import ReSyncConfig
from cognite.powerops.resync.models._base import AssetModel, DataModel, Model

from .to_cogshop_model import to_cogshop_model
from .to_market_model import to_market_model
from .to_production_model import to_production_model


def transform(
    config: ReSyncConfig,
    market_name: str,
    models: set[str],
) -> ResourceCollection:
    asset_models: list[AssetModel] = []
    data_models: list[DataModel] = []
    production_model = to_production_model(config.production)
    if "ProductionAsset" in models:
        asset_models.append(production_model)
    if "MarketAsset" in models:
        market_model = to_market_model(config.market, production_model.price_areas, market_name)
        settings = config.settings
        market_model.set_root_asset(
            settings.shop_service_url,
            settings.organization_subdomain,
            settings.tenant_id,
            production_model.root_asset.external_id,
        )
        asset_models.append(market_model)
    if "CogShopAsset" in models:
        cogshop_model = to_cogshop_model(config.cogshop, production_model.watercourses, config.settings.shop_version)
        data_models.append(cogshop_model)

    # production_data_model = to_production_data_model(production_model)
    # market_data_model = to_market_data_model(market_model)

    labels = AssetLabel.as_label_definitions() + RelationshipLabel.as_label_definitions()
    collection = ResourceCollection()
    collection.add(labels)
    all_models: list[Model] = asset_models + data_models
    for model in all_models:
        collection.add(model.sequences())
        collection.add(model.files())

    for asset_model in asset_models:
        collection.add(asset_model.parent_assets())
        collection.add(asset_model.assets())
        collection.add(asset_model.relationships())
    for data_model in data_models:
        collection.add(data_model.instances())

    return collection
