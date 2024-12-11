from __future__ import annotations

import logging
from pathlib import Path

from cognite.powerops.client import PowerOpsClient
from cognite.powerops.resync.config_to_fdm import ResyncImporter
from cognite.powerops.resync.utils import get_data_model_write_classes

logger = logging.getLogger(__name__)


def pre_build(client_configuration: Path, configuration: Path) -> None:
    """Generates data model objects from a resync configuration and prints the plan.

    Initializes a ResyncImporter given the input configuration path. The importer is then used to generate data model
    objects from the configuration. The plan is then printed to the console.

    Args:
        configuration: Path to the resync configuration file.
    """
    client = PowerOpsClient.from_config(client_configuration)

    data_model_classes = get_data_model_write_classes(client.v1)

    resync_importer = ResyncImporter.from_yaml(configuration, data_model_classes, client.cdf)
    resync_data_model_objects, external_ids = resync_importer.to_toolkit_nodes_edges(client.cdf)

    logger.info(resync_data_model_objects)
    logger.info(f"Generated {len(resync_data_model_objects)} node objects")
    logger.info(f"External IDs: {external_ids}")
