from __future__ import annotations

import logging
from pathlib import Path

from cognite.powerops.client import PowerOpsClient
from cognite.powerops.resync.config_to_fdm import ResyncImporter
from cognite.powerops.resync.purge import ResyncPurge
from cognite.powerops.resync.utils import get_data_model_write_classes

logger = logging.getLogger("resync")


def pre_build(client_configuration: Path, configuration: Path) -> None:
    """Generates data model objects from a resync configuration and prints the plan.

    Initializes a ResyncImporter given the input configuration path. The importer is then used to generate data model
    objects from the configuration. The plan is then printed to the console.

    Args:
        client_configuration: Path to the CDF client configuration file.
        configuration: Path to the resync configuration file.
    """
    client = PowerOpsClient.from_config(client_configuration)

    data_model_classes = get_data_model_write_classes(client.v1)

    resync_importer = ResyncImporter.from_yaml(configuration, data_model_classes, client.cdf)
    resync_data_model_objects, external_ids = resync_importer.to_toolkit_nodes_edges(client.cdf)

    logger.info(resync_data_model_objects)
    logger.info(f"Generated {len(resync_data_model_objects)} node objects")
    logger.info(f"External IDs: {external_ids}")


def purge(client_configuration: Path, configuration: Path, dry_run: bool = False, verbose: bool = False) -> None:
    """Checks which nodes & edges are not included in the toolkit files to be deleted.

    Args:
        client_configuration: Path to the CDF client configuration file.
        configuration: Path to the resync configuration file.
        dry_run: Whether or not to actually delete the nodes/edges.
        verbose: Print out detailed information about nodes & edges to delete.
    """

    client = PowerOpsClient.from_config(client_configuration)

    resync_purge = ResyncPurge.from_yaml(configuration, client.cdf, dry_run, verbose)
    resync_purge.purge()
