from pathlib import Path
from typing import Callable

from cognite.powerops.bootstrap.data_classes.bootstrap_config import BootstrapConfig
from cognite.powerops.bootstrap.logger import configure_debug_logging
from cognite.powerops.bootstrap.to_cdf_resources.core import _create_cdf_resources, _preview_resources_diff, transform
from cognite.powerops.clients import get_powerops_client


def plan(path: Path, market: str, echo: Callable[[str], None] = None):
    echo = echo or print
    client = get_powerops_client()
    bootstrap_resources, config = _load_transform(market, path, client.cdf.config.project, echo)

    # 2.b - preview diff
    _preview_resources_diff(
        client,
        bootstrap_resources,
        config.settings.data_set_external_id,
    )


def apply(path: Path, market: str, echo: Callable[[str], None] = None):
    echo = echo or print
    client = get_powerops_client()
    bootstrap_resources, config = _load_transform(market, path, client.cdf.config.project, echo)

    # Step 3 - write bootstrap resources from diffs to CDF
    _create_cdf_resources(
        client,
        bootstrap_resources,
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
