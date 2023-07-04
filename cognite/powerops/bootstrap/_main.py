from pathlib import Path

from cognite.powerops.bootstrap.bootstrap import (
    _create_cdf_resources,
    _load_config,
    _preview_resources_diff,
    _transform,
    validate_config,
)
from cognite.powerops.bootstrap.logger import configure_debug_logging


def plan(path: Path, market: str):
    # Step 1 - configure and validate config
    config = _load_config(path)
    config = validate_config(config)
    configure_debug_logging(config.constants.debug_level)

    # Step 2 - transform from config to CDF resources and preview diffs
    # 2.a - transform
    bootstrap_resources = _transform(
        config,
        path,
        market,
    )

    # 2.b - preview diff
    _preview_resources_diff(
        bootstrap_resources,
        config.constants.data_set_external_id,
    )


def apply(path: Path, market: str):
    # Step 1 - configure and validate config
    config = _load_config(path)
    config = validate_config(config)
    configure_debug_logging(config.constants.debug_level)

    # Step 2 - transform from config to CDF resources and preview diffs
    bootstrap_resources = _transform(
        config,
        path,
        market,
    )

    # Step 3 - write bootstrap resources from diffs to CDF
    _create_cdf_resources(
        bootstrap_resources,
        config.constants.data_set_external_id,
        config.constants.overwrite_data,
        config.constants.skip_dm,
    )
