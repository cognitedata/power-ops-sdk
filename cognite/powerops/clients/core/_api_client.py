from __future__ import annotations

import getpass
from pathlib import Path

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials

from ._api.generators import GeneratorsAPI
from ._api.plants import PlantsAPI
from ._api.price_areas import PriceAreasAPI
from ._api.reservoirs import ReservoirsAPI
from ._api.watercourses import WatercoursesAPI


class CoreClient:
    """
    CoreClient

    Generated with:
        pygen = 0.11.4
        cognite-sdk = 6.5.8
        pydantic = 2.0.2

    Data Model:
        space: power-ops
        externalId: Core
        version: 1
    """

    def __init__(self, config: ClientConfig | None = None):
        client = CogniteClient(config)
        self.generators = GeneratorsAPI(client)
        self.plants = PlantsAPI(client)
        self.price_areas = PriceAreasAPI(client)
        self.reservoirs = ReservoirsAPI(client)
        self.watercourses = WatercoursesAPI(client)

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> CoreClient:
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
    def from_toml(cls, file_path: Path | str) -> CoreClient:
        import toml

        return cls.azure_project(**toml.load(file_path)["cognite"])
