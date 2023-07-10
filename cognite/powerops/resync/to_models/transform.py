from __future__ import annotations

from cognite.powerops.cdf_labels import AssetLabel, RelationshipLabel
from cognite.powerops.resync.config_classes.resource_collection import ResourceCollection
from cognite.powerops.resync.config_classes.resync_config import ReSyncConfig

from .to_cogshop_model import cogshop_to_cdf_resources
from .to_core_model import to_core_model
from .to_market_model import to_market_model


def transform(
    config: ReSyncConfig,
    market_name: str,
) -> ResourceCollection:
    core_model = to_core_model(config.core)
    market_model = to_market_model(config.markets, market_name, core_model.price_areas)

    settings = config.settings
    market_model.set_root_asset(
        settings.shop_service_url,
        settings.organization_subdomain,
        settings.tenant_id,
        core_model.root_asset.external_id,
    )

    collection = ResourceCollection()
    created, cogshop_model = cogshop_to_cdf_resources(
        config.core, config.settings.shop_version, config.cogshop, core_model.watercourses
    )
    collection.add(cogshop_model.sequences())
    collection.add(cogshop_model.instances())
    collection += created
    print(cogshop_model.model_dump())
    labels = AssetLabel.as_label_definitions() + RelationshipLabel.as_label_definitions()

    collection.add(labels)
    for model in [core_model, market_model]:
        collection.add(model.parent_assets())
        collection.add(model.assets())
        collection.add(model.relationships())
        collection.add(model.sequences())

    return collection
