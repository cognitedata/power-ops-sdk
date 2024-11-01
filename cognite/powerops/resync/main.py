from __future__ import annotations

import logging
from pathlib import Path

from cognite.powerops.client import PowerOpsClient
from cognite.powerops.resync.config_to_fdm import ResyncImporter
from cognite.powerops.resync.utils import check_all_linked_sources_exist, get_data_model_write_classes

logger = logging.getLogger(__name__)


def plan(client_configuration: Path, configuration: Path) -> None:
    """Generates data model objects from a resync configuration and prints the plan.

    Initializes a ResyncImporter given the input configuration path. The importer is then used to generate data model
    objects from the configuration. The plan is then printed to the console.

    Args:
        configuration: Path to the resync configuration file.
    """
    client = PowerOpsClient.from_config(client_configuration)

    data_model_classes = get_data_model_write_classes(client.v1)

    resync_importer = ResyncImporter.from_yaml(configuration, data_model_classes, client.cdf)
    resync_data_model_objects, external_ids = resync_importer.to_data_model()

    logger.info(resync_data_model_objects)
    logger.info(f"Generated {len(resync_data_model_objects)} objects")
    logger.info(f"External IDs: {external_ids}")


def apply(client_configuration: Path, configuration: Path, client: PowerOpsClient | None = None) -> None:
    """Generates data model objects from a resync configuration and upserts them to CDF.

    Initializes a ResyncImporter given the input configuration path. The importer is then used to generate data model
    objects from the configuration. The data model objects are then upserted to CDF using the provided client. If no
    client is provided, a client is created from the settings. If the configuration contains file configurations, the
    files are uploaded to CDF before the data model objects are upserted. The function also checks that all linked
    sources exist or are referenced in the data model objects being upserted.

    Args:
        configuration: Path to the resync configuration file.
        client: PowerOpsClient to use for upserting the data model objects. If not provided, a client is created from
            the settings.
    """
    client = client or PowerOpsClient.from_config(client_configuration)

    data_model_classes = get_data_model_write_classes(client.v1)

    resync_importer = ResyncImporter.from_yaml(configuration, data_model_classes, client.cdf)
    resync_data_model_objects, external_ids = resync_importer.to_data_model()

    if resync_importer.file_configuration:
        file_external_ids = resync_importer.file_configuration.upload_files_to_cdf(client.cdf)

    # TODO: check all linked sources exist
    check_all_linked_sources_exist(resync_data_model_objects, file_external_ids, [], external_ids)

    logger.info(resync_data_model_objects)
    client.v1.upsert(resync_data_model_objects, replace=resync_importer.overwrite_data)

    logger.info(f"Upserted {len(resync_data_model_objects)} objects")
