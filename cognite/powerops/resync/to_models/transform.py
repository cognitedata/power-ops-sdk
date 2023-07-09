from __future__ import annotations

from typing import Callable

from cognite.powerops.cdf_labels import AssetLabel, RelationshipLabel
from cognite.powerops.resync.config_classes.resource_collection import ResourceCollection
from cognite.powerops.resync.config_classes.resync_config import ReSyncConfig
from cognite.powerops.resync.to_models.to_cogshop_model import cogshop_to_cdf_resources
from cognite.powerops.resync.to_models.to_core_model import to_core_model
from cognite.powerops.resync.to_models.to_market_model import market_to_cdf_resources


def transform(
    config: ReSyncConfig,
    market_name: str,
    echo: Callable[[str], None],
) -> ResourceCollection:
    settings = config.settings
    echo(f"Running bootstrap for data set {settings.data_set_external_id} in CDF project {settings.cdf_project}")

    core_model = to_core_model(config.core)

    market_model = market_to_cdf_resources(
        config.markets,
        market_name,
        core_model.price_areas,
    )
    market_model.set_root_asset(
        settings.shop_service_url,
        settings.organization_subdomain,
        settings.tenant_id,
        core_model.root_asset.external_id,
    )

    collection = ResourceCollection()
    labels = AssetLabel.as_label_definitions() + RelationshipLabel.as_label_definitions()

    collection.add(labels)
    for model in [core_model, market_model]:
        collection.add(model.parent_assets())
        collection.add(model.assets())
        collection.add(model.relationships())
        collection.add(model.sequences())

    collection += cogshop_to_cdf_resources(
        config.core, collection.shop_file_configs, config.settings.shop_version, config.watercourses_shop
    )

    return collection
