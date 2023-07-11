from datetime import datetime
from pathlib import Path
from typing import Callable
from uuid import uuid4

from cognite.client.data_classes import Event

from cognite.powerops.clients import get_powerops_client
from cognite.powerops.resync._logger import configure_debug_logging
from cognite.powerops.resync.config.resource_collection import ResourceCollection
from cognite.powerops.resync.config.resync_config import ReSyncConfig
from cognite.powerops.resync.to_models.transform import transform


def plan(path: Path, market: str, echo: Callable[[str], None] = None):
    echo = echo or print
    client = get_powerops_client()
    bootstrap_resources, config = _load_transform(market, path, client.cdf.config.project, echo)

    # 2.b - preview diff
    cdf_bootstrap_resources = bootstrap_resources.from_cdf(
        po_client=client, data_set_external_id=config.settings.data_set_external_id
    )

    echo(ResourceCollection.prettify_differences(bootstrap_resources.difference(cdf_bootstrap_resources)))


def apply(path: Path, market: str, echo: Callable[[str], None] = None):
    echo = echo or print
    client = get_powerops_client()
    collection, config = _load_transform(market, path, client.cdf.config.project, echo)

    # ! This should always stay at the bottom # TODO: consider wrapper
    collection.add(_create_bootstrap_finished_event(echo))

    # Step 3 - write bootstrap resources from diffs to CDF
    collection.write_to_cdf(
        client,
        config.settings.data_set_external_id,
        config.settings.overwrite_data,
    )


def _load_transform(market: str, path: Path, cdf_project: str, echo: Callable[[str], None]):
    # Step 1 - configure and validate config
    config = ReSyncConfig.from_yamls(path, cdf_project)
    configure_debug_logging(config.settings.debug_level)
    # Step 2 - transform from config to CDF resources and preview diffs
    echo(
        f"Running resync for data set {config.settings.data_set_external_id} "
        f"in CDF project {config.settings.cdf_project}"
    )
    bootstrap_resources = transform(config, market)
    return bootstrap_resources, config


def _create_bootstrap_finished_event(echo: Callable[[str], None]) -> Event:
    """Creating a POWEROPS_BOOTSTRAP_FINISHED Event in CDF to signal that bootstrap scripts have been ran"""
    current_time = int(datetime.utcnow().timestamp() * 1000)  # in milliseconds
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
