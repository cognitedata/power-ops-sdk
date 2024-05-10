from __future__ import annotations

from pathlib import Path

from rich import print

from cognite.powerops.client import PowerOpsClient
from cognite.powerops.resync.v1.config_to_fdm import ResyncImporter
from cognite.powerops.resync.v1.utils import check_all_linked_sources_exist, upload_files_to_cdf


def plan(configuration: Path) -> None:
    """Generates data model objects from a resync configuration and prints the plan.

    Initializes a ResyncImporter given the input configuration path. The importer is then used to generate data model
    objects from the configuration. The plan is then printed to the console.

    Args:
        configuration: Path to the resync configuration file.
    """
    resync_importer = ResyncImporter(configuration)
    resync_data_model_objects, external_ids = resync_importer.to_data_model()

    print(resync_data_model_objects)
    print(f"Generated {len(resync_data_model_objects)} bid configurations")
    print(f"External IDs: {external_ids}")


def apply(configuration: Path, client: PowerOpsClient | None = None) -> None:
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
    client = client or PowerOpsClient.from_settings()

    resync_importer = ResyncImporter(configuration)
    resync_data_model_objects, external_ids = resync_importer.to_data_model()

    if resync_importer.resync_configuration.file_configuration:
        file_external_ids = upload_files_to_cdf(
            client.cdf,
            resync_importer.resync_configuration.file_configuration,
            resync_importer.resync_configuration.data_set_id,
        )

    # TODO: check all linked sources exist
    check_all_linked_sources_exist(resync_data_model_objects, file_external_ids, [], external_ids)

    print(resync_data_model_objects)
    client.v1.upsert(resync_data_model_objects, replace=resync_importer.resync_configuration.overwrite_data)

    print(f"Upserted {len(resync_data_model_objects)} bid configurations")
