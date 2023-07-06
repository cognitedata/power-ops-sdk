from __future__ import annotations

import getpass
from pathlib import Path

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials

from ._api.cases import CasesAPI
from ._api.commands_configs import CommandsConfigsAPI
from ._api.file_refs import FileRefsAPI
from ._api.mappings import MappingsAPI
from ._api.model_templates import ModelTemplatesAPI
from ._api.processing_logs import ProcessingLogsAPI
from ._api.scenarios import ScenariosAPI
from ._api.transformations import TransformationsAPI


class CogShopClient:
    """
    CogShopClient

    Generated with:
        pygen = 0.11.4
        cognite-sdk = 6.5.8
        pydantic = 2.0.2

    Data Model:
        space: cogShop
        externalId: CogShop
        version: 1
    """

    def __init__(self, config: ClientConfig | None = None):
        client = CogniteClient(config)
        self.cases = CasesAPI(client)
        self.commands_configs = CommandsConfigsAPI(client)
        self.file_refs = FileRefsAPI(client)
        self.mappings = MappingsAPI(client)
        self.model_templates = ModelTemplatesAPI(client)
        self.processing_logs = ProcessingLogsAPI(client)
        self.scenarios = ScenariosAPI(client)
        self.transformations = TransformationsAPI(client)

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
