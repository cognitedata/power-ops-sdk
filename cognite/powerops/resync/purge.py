from __future__ import annotations

import logging
from typing import Optional
from pathlib import Path
from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import ViewId
from cognite.powerops.utils.serialization import load_yaml

logger = logging.getLogger(__name__)

class ResyncPurge:
    """Class for purging resync data from CDF."""
    toolkit_modules: list[Path]
    data_model_name: str = "config_ResyncConfiguration"
    dry_run: bool = False
    client: CogniteClient = None

    def __init__(
        self,
        toolkit_directory: list[Path],
        dry_run: bool,
        client: CogniteClient,
        data_model_name: Optional[str] = None,
    ):
        self.toolkit_directory = toolkit_directory
        if data_model_name:
            self.data_model_name = data_model_name
        self.dry_run = dry_run
        self.client = client

    @classmethod
    def from_yaml(
        cls,
        configuration_path: Path,
        cdf_client: CogniteClient,
        dry_run: bool = False,
    ) -> ResyncPurge:
        """Creates a ResyncImporter object from a resync configuration file.

        Args:
            configuration_path: Path to the resync configuration file.
            data_model_classes: A dictionary of all data model classes to be used for the resync configuration.

        Returns:
            A ResyncImporter object.
        """
        configuration = load_yaml(configuration_path, expected_return_type="dict")

        temp_toolkit_directory = configuration.get("toolkit_modules")
        if temp_toolkit_directory:
            if isinstance(temp_toolkit_directory, str):
                toolkit_directory = [Path(temp_toolkit_directory)]
            elif isinstance(temp_toolkit_directory, list):
                toolkit_directory = [Path(directory) for directory in temp_toolkit_directory]
            else:
                raise ValueError("toolkit_directory must be a string or a list of strings")
        else:
            raise ValueError("toolkit_directory is required in the configuration file")

        data_model_name = configuration.get("data_model_name")

        return cls(
            toolkit_directory=toolkit_directory,
            dry_run=dry_run,
            client=cdf_client,
            data_model_name=data_model_name,
        )

    def purge(self) -> None:
        """Purges resync data from CDF."""
        print("Purging resync data")

        view_ids = self.get_views_in_data_model()

        for view_id in view_ids:
            print(view_id)

        node_ids = self.get_toolkit_node_external_ids()

        if self.dry_run:
            print("Dry run mode enabled. Exiting without deleting any data.")
            return

        # Delete nodes


    def get_views_in_data_model(self) -> list[ViewId]:
        """Get all views in the data model."""

        return self.client.data_modeling.data_models.retrieve(("power_ops_core", self.data_model_name, "1"))[0].views

    def get_toolkit_node_external_ids(self) -> dict[str, list[str]]:
        """Get all node external IDs from the toolkit files."""


        for toolkit_directory in self.toolkit_directory:
            yaml_files = list(toolkit_directory.rglob('*.yaml')) + list(toolkit_directory.rglob('*.yml'))
            for file in yaml_files:
                nodes = load_yaml(file, expected_return_type="list")
                for node in nodes:
                    print(node)

        return {}