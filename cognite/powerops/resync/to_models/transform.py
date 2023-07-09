from __future__ import annotations

import datetime
from pathlib import Path
from typing import Callable
from uuid import uuid4

from cognite.client.data_classes import Event

from cognite.powerops.resync.config_classes.bootstrap_config import BootstrapConfig
from cognite.powerops.resync.config_classes.cdf_labels import AssetLabel, RelationshipLabel
from cognite.powerops.resync.config_classes.resource_collection import ResourceCollection
from cognite.powerops.resync.config_classes.skeleton_asset_hierarchy import create_skeleton_asset_hierarchy
from cognite.powerops.resync.to_models.to_cogshop_model import cogshop_to_cdf_resources
from cognite.powerops.resync.to_models.to_core_model import to_core_model
from cognite.powerops.resync.to_models.to_market_model import market_to_cdf_resources


def transform(
    config: BootstrapConfig,
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

    collection = ResourceCollection()
    # Create common CDF resources
    labels = AssetLabel.as_label_definitions() + RelationshipLabel.as_label_definitions()

    skeleton_assets = create_skeleton_asset_hierarchy(
        settings.shop_service_url, settings.organization_subdomain, settings.tenant_id
    )
    collection.add(skeleton_assets)
    collection.add(labels)
    for model in [core_model, market_model]:
        collection.add(model.assets())
        collection.add(model.relationships())
        collection.add(model.sequences())

    collection += cogshop_to_cdf_resources(
        config.core, collection.shop_file_configs, config.settings.shop_version, config.watercourses_shop
    )

    # Set hashes for Shop Files, needed for comparison
    for shop_config in collection.shop_file_configs.values():
        if shop_config.md5_hash is None:
            file_content = Path(shop_config.path).read_bytes()
            shop_config.set_md5_hash(file_content)

    # ! This should always stay at the bottom # TODO: consider wrapper
    collection.add(create_bootstrap_finished_event(echo))

    return collection


def create_bootstrap_finished_event(echo: Callable[[str], None]) -> Event:
    """Creating a POWEROPS_BOOTSTRAP_FINISHED Event in CDF to signal that bootstrap scripts have been ran"""
    current_time = int(datetime.datetime.utcnow().timestamp() * 1000)  # in milliseconds
    event = Event(
        start_time=current_time,
        end_time=current_time,
        external_id=f"POWEROPS_BOOTSTRAP_FINISHED_{str(uuid4())}",
        type="POWEROPS_BOOTSTRAP_FINISHED",
        subtype=None,
        source="PowerOps bootstrap",
        description="Manual run of bootstrap scripts finished",
    )
    echo(f"Created status event '{event.external_id}'")

    return event
