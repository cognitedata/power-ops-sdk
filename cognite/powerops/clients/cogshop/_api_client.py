from __future__ import annotations

import getpass
from pathlib import Path

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials

from ._api.commands_configs import CommandsConfigsAPI
from ._api.input_time_series_mappings import InputTimeSeriesMappingsAPI
from ._api.output_mappings import OutputMappingsAPI
from ._api.scenario_templates import ScenarioTemplatesAPI
from ._api.scenarios import ScenariosAPI
from ._api.value_transformations import ValueTransformationsAPI


class CogShopClient:
    """
    CogShopClient

    Generated with:
        pygen = 0.11.6
        cognite-sdk = 6.8.4
        pydantic = 2.0.2

    Data Model:
        space: power-ops
        externalId: cogshop
        version: 1
    """

    def __init__(self, config: ClientConfig | None = None):
        client = CogniteClient(config)
        self.commands_configs = CommandsConfigsAPI(client)
        self.input_time_series_mappings = InputTimeSeriesMappingsAPI(client)
        self.output_mappings = OutputMappingsAPI(client)
        self.scenarios = ScenariosAPI(client)
        self.scenario_templates = ScenarioTemplatesAPI(client)
        self.value_transformations = ValueTransformationsAPI(client)

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> CogShopClient:
        base_url = f"https://{cdf_cluster}.cognitedata.com/"
        credentials = OAuthClientCredentials(
            token_url=f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=[f"{base_url}.default"],
        )
        config = ClientConfig(
            project=project,
            credentials=credentials,
            client_name=getpass.getuser(),
            base_url=base_url,
        )

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str) -> CogShopClient:
        import toml

        return cls.azure_project(**toml.load(file_path)["cognite"])
