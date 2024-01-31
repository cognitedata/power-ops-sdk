from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient, data_modeling as dm
from cognite.client.credentials import OAuthClientCredentials

from ._api.bid_method import BidMethodAPI
from ._api.generator import GeneratorAPI
from ._api.generator_efficiency_curve import GeneratorEfficiencyCurveAPI
from ._api.plant import PlantAPI
from ._api.price_area import PriceAreaAPI
from ._api.reservoir import ReservoirAPI
from ._api.turbine_efficiency_curve import TurbineEfficiencyCurveAPI
from ._api.watercourse import WatercourseAPI
from . import data_classes


class PowerAssetAPI:
    """
    PowerAssetAPI

    Generated with:
        pygen = 0.36.0
        cognite-sdk = 7.15.0
        pydantic = 2.5.3

    Data Model:
        space: power-ops-assets
        externalId: PowerAsset
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
        client.config.client_name = "CognitePygen:0.36.0"

        view_by_read_class = {
            data_classes.BidMethod: dm.ViewId("power-ops-shared", "BidMethod", "1"),
            data_classes.Generator: dm.ViewId("power-ops-assets", "Generator", "1"),
            data_classes.GeneratorEfficiencyCurve: dm.ViewId("power-ops-assets", "GeneratorEfficiencyCurve", "1"),
            data_classes.Plant: dm.ViewId("power-ops-assets", "Plant", "1"),
            data_classes.PriceArea: dm.ViewId("power-ops-assets", "PriceArea", "1"),
            data_classes.Reservoir: dm.ViewId("power-ops-assets", "Reservoir", "1"),
            data_classes.TurbineEfficiencyCurve: dm.ViewId("power-ops-assets", "TurbineEfficiencyCurve", "1"),
            data_classes.Watercourse: dm.ViewId("power-ops-assets", "Watercourse", "1"),
        }

        self.bid_method = BidMethodAPI(client, view_by_read_class)
        self.generator = GeneratorAPI(client, view_by_read_class)
        self.generator_efficiency_curve = GeneratorEfficiencyCurveAPI(client, view_by_read_class)
        self.plant = PlantAPI(client, view_by_read_class)
        self.price_area = PriceAreaAPI(client, view_by_read_class)
        self.reservoir = ReservoirAPI(client, view_by_read_class)
        self.turbine_efficiency_curve = TurbineEfficiencyCurveAPI(client, view_by_read_class)
        self.watercourse = WatercourseAPI(client, view_by_read_class)

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> PowerAssetAPI:
        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)
        config = ClientConfig.default(project, cdf_cluster, credentials)

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str, section: str | None = "cognite") -> PowerAssetAPI:
        import toml

        toml_content = toml.load(file_path)
        if section is not None:
            try:
                toml_content = toml_content[section]
            except KeyError as e:
                raise ValueError(f"Could not find section '{section}' in {file_path}") from e

        return cls.azure_project(**toml_content)

    def _repr_html_(self) -> str:
        return """<strong>PowerAssetAPI</strong> generated from data model ("power-ops-assets", "PowerAsset", "1")<br />
with the following APIs available<br />
&nbsp;&nbsp;&nbsp;&nbsp;.bid_method<br />
&nbsp;&nbsp;&nbsp;&nbsp;.generator<br />
&nbsp;&nbsp;&nbsp;&nbsp;.generator_efficiency_curve<br />
&nbsp;&nbsp;&nbsp;&nbsp;.plant<br />
&nbsp;&nbsp;&nbsp;&nbsp;.price_area<br />
&nbsp;&nbsp;&nbsp;&nbsp;.reservoir<br />
&nbsp;&nbsp;&nbsp;&nbsp;.turbine_efficiency_curve<br />
&nbsp;&nbsp;&nbsp;&nbsp;.watercourse<br />
"""
