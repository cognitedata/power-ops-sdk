from pathlib import Path
from typing import Callable

from cognite.powerops.clients import get_powerops_client
from cognite.powerops.resync.config_classes.bootstrap_config import BootstrapConfig
from cognite.powerops.resync.config_classes.resource_collection import ResourceCollection
from cognite.powerops.resync.logger import configure_debug_logging
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
    bootstrap_resources, config = _load_transform(market, path, client.cdf.config.project, echo)

    # Step 3 - write bootstrap resources from diffs to CDF
    bootstrap_resources.write_to_cdf(
        client,
        config.settings.data_set_external_id,
        config.settings.overwrite_data,
        config.settings.skip_dm,
    )


def _load_transform(market: str, path: Path, cdf_project: str, echo: Callable[[str], None]):
    # Step 1 - configure and validate config
    config = BootstrapConfig.from_yamls(path, cdf_project)
    configure_debug_logging(config.settings.debug_level)
    # Step 2 - transform from config to CDF resources and preview diffs
    bootstrap_resources = transform(config, market, echo)
    return bootstrap_resources, config
