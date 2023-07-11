from __future__ import annotations

from cognite.powerops.cdf_labels import AssetLabel, RelationshipLabel
from cognite.powerops.resync.config_classes.resource_collection import ResourceCollection
from cognite.powerops.resync.config_classes.resync_config import ReSyncConfig

from .to_cogshop_model import to_cogshop_model
from .to_core_model import to_core_model
from .to_market_model import to_market_model


def transform(
    config: ReSyncConfig,
    market_name: str,
) -> ResourceCollection:
    core_model = to_core_model(config.production)
    market_model = to_market_model(config.markets, core_model.price_areas, market_name)
    cogshop_model = to_cogshop_model(config.cogshop, core_model.watercourses, config.settings.shop_version)

    settings = config.settings
    market_model.set_root_asset(
        settings.shop_service_url,
        settings.organization_subdomain,
        settings.tenant_id,
        core_model.root_asset.external_id,
    )

    labels = AssetLabel.as_label_definitions() + RelationshipLabel.as_label_definitions()
    collection = ResourceCollection()
    collection.add(labels)

    for model in [core_model, market_model, cogshop_model]:
        collection.add(model.sequences())
        collection.add(model.files())
    for asset_model in [core_model, market_model]:
        collection.add(asset_model.parent_assets())
        collection.add(asset_model.assets())
        collection.add(asset_model.relationships())
    for data_model in [cogshop_model]:
        collection.add(data_model.instances())

    return collection
