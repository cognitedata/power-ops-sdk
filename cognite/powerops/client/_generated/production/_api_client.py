from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient, data_modeling as dm
from cognite.client.credentials import OAuthClientCredentials

from ._api.generator import GeneratorAPI
from ._api.plant import PlantAPI
from ._api.price_area import PriceAreaAPI
from ._api.reservoir import ReservoirAPI
from ._api.watercourse import WatercourseAPI
from ._api.watercourse_shop import WatercourseShopAPI
from . import data_classes


class ProductionModelAPI:
    """
    ProductionModelAPI

    Generated with:
        pygen = 0.33.0
        cognite-sdk = 7.8.6
        pydantic = 2.5.3

    Data Model:
        space: power-ops
        externalId: production
        version: 1
    """

    def __init__(self, config_or_client: CogniteClient | ClientConfig):
        if isinstance(config_or_client, CogniteClient):
            client = config_or_client
        elif isinstance(config_or_client, ClientConfig):
            client = CogniteClient(config_or_client)
        else:
            raise ValueError(f"Expected CogniteClient or ClientConfig, got {type(config_or_client)}")
        # The client name is used for aggregated logging of Pygen Usage
        client.config.client_name = "CognitePygen:0.33.0"

        view_by_read_class = {
            data_classes.Generator: dm.ViewId("power-ops", "Generator", "9178931bbaac71"),
            data_classes.Plant: dm.ViewId("power-ops", "Plant", "836dcb3f5da1df"),
            data_classes.PriceArea: dm.ViewId("power-ops", "PriceArea", "6849ae787cd368"),
            data_classes.Reservoir: dm.ViewId("power-ops", "Reservoir", "3c822b0c3d68f7"),
            data_classes.Watercourse: dm.ViewId("power-ops", "Watercourse", "96f5170f35ef70"),
            data_classes.WatercourseShop: dm.ViewId("power-ops", "WatercourseShop", "4b5321b1fccd06"),
        }

        self.generator = GeneratorAPI(client, view_by_read_class)
        self.plant = PlantAPI(client, view_by_read_class)
        self.price_area = PriceAreaAPI(client, view_by_read_class)
        self.reservoir = ReservoirAPI(client, view_by_read_class)
        self.watercourse = WatercourseAPI(client, view_by_read_class)
        self.watercourse_shop = WatercourseShopAPI(client, view_by_read_class)

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> ProductionModelAPI:
        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)
        config = ClientConfig.default(project, cdf_cluster, credentials)

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str, section: str | None = "cognite") -> ProductionModelAPI:
        import toml

        toml_content = toml.load(file_path)
        if section is not None:
            try:
                toml_content = toml_content[section]
            except KeyError as e:
                raise ValueError(f"Could not find section '{section}' in {file_path}") from e

        return cls.azure_project(**toml_content)
