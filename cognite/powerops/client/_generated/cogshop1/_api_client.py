from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials

from cognite.powerops.client._generated.cogshop1._api.cases import CasesAPI
from cognite.powerops.client._generated.cogshop1._api.commands_configs import CommandsConfigsAPI
from cognite.powerops.client._generated.cogshop1._api.file_refs import FileRefsAPI
from cognite.powerops.client._generated.cogshop1._api.mappings import MappingsAPI
from cognite.powerops.client._generated.cogshop1._api.model_templates import ModelTemplatesAPI
from cognite.powerops.client._generated.cogshop1._api.processing_logs import ProcessingLogsAPI
from cognite.powerops.client._generated.cogshop1._api.scenarios import ScenariosAPI
from cognite.powerops.client._generated.cogshop1._api.transformations import TransformationsAPI


class CogShop1Client:
    """
    CogShop1Client

    Generated with:
        pygen = 0.17.7
        cognite-sdk = 6.21.1
        pydantic = 2.3.0

    Data Model:
        space: cogShop
        externalId: CogShop
        version: 2
    """

    def __init__(self, config_or_client: CogniteClient | ClientConfig):
        if isinstance(config_or_client, CogniteClient):
            client = config_or_client
        elif isinstance(config_or_client, ClientConfig):
            client = CogniteClient(config_or_client)
        else:
            raise ValueError(f"Expected CogniteClient or ClientConfig, got {type(config_or_client)}")
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
    ) -> CogShop1Client:
        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)
        config = ClientConfig.default(project, cdf_cluster, credentials)

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str, section: str | None = "cognite") -> CogShop1Client:
        import toml

        toml_content = toml.load(file_path)
        if section is not None:
            try:
                toml_content = toml_content[section]
            except KeyError as e:
                raise ValueError(f"Could not find section '{section}' in {file_path}") from e

        return cls.azure_project(**toml_content)
