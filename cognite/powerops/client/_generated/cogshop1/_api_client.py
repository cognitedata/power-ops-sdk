from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.credentials import OAuthClientCredentials

from ._api.case import CaseAPI
from ._api.commands_config import CommandsConfigAPI
from ._api.file_ref import FileRefAPI
from ._api.mapping import MappingAPI
from ._api.model_template import ModelTemplateAPI
from ._api.processing_log import ProcessingLogAPI
from ._api.scenario import ScenarioAPI
from ._api.transformation import TransformationAPI


class CogShop1Client:
    """
    CogShop1Client

    Generated with:
        pygen = 0.21.1
        cognite-sdk = 6.28.0
        pydantic = 2.4.2

    Data Model:
        space: cogShop
        externalId: CogShop
        version: 3
    """

    def __init__(self, config_or_client: CogniteClient | ClientConfig):
        if isinstance(config_or_client, CogniteClient):
            client = config_or_client
        elif isinstance(config_or_client, ClientConfig):
            client = CogniteClient(config_or_client)
        else:
            raise ValueError(f"Expected CogniteClient or ClientConfig, got {type(config_or_client)}")
        self.case = CaseAPI(client, dm.ViewId("cogShop", "Case", "f532a799a59e1b"))
        self.commands_config = CommandsConfigAPI(client, dm.ViewId("cogShop", "CommandsConfig", "1bada7c1b95263"))
        self.file_ref = FileRefAPI(client, dm.ViewId("cogShop", "FileRef", "7c508a562e95ca"))
        self.mapping = MappingAPI(client, dm.ViewId("cogShop", "Mapping", "fc518056c47cad"))
        self.model_template = ModelTemplateAPI(client, dm.ViewId("cogShop", "ModelTemplate", "7864e537bac36e"))
        self.processing_log = ProcessingLogAPI(client, dm.ViewId("cogShop", "ProcessingLog", "c4fa72c3c45945"))
        self.scenario = ScenarioAPI(client, dm.ViewId("cogShop", "Scenario", "dcb646ab5384db"))
        self.transformation = TransformationAPI(client, dm.ViewId("cogShop", "Transformation", "917a6a16276e22"))

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
