from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient, data_modeling as dm
from cognite.client.credentials import OAuthClientCredentials

from ._api.alert import AlertAPI
from ._api.basic_bid_matrix import BasicBidMatrixAPI
from ._api.bid_document import BidDocumentAPI
from ._api.bid_matrix import BidMatrixAPI
from ._api.bid_method import BidMethodAPI
from ._api.multi_scenario_matrix import MultiScenarioMatrixAPI
from ._api.price_area import PriceAreaAPI
from ._api.shop_multi_scenario_method import SHOPMultiScenarioMethodAPI
from ._api.shop_price_scenario import SHOPPriceScenarioAPI
from ._api.shop_price_scenario_result import SHOPPriceScenarioResultAPI
from ._api.water_value_based_method import WaterValueBasedMethodAPI
from . import data_classes


class DayAheadBidAPI:
    """
    DayAheadBidAPI

    Generated with:
        pygen = 0.36.0
        cognite-sdk = 7.15.0
        pydantic = 2.5.3

    Data Model:
        space: power-ops-day-ahead-bid
        externalId: DayAheadBid
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
            data_classes.Alert: dm.ViewId("power-ops-shared", "Alert", "1"),
            data_classes.BasicBidMatrix: dm.ViewId("power-ops-day-ahead-bid", "BasicBidMatrix", "1"),
            data_classes.BidDocument: dm.ViewId("power-ops-day-ahead-bid", "BidDocument", "1"),
            data_classes.BidMatrix: dm.ViewId("power-ops-day-ahead-bid", "BidMatrix", "1"),
            data_classes.BidMethod: dm.ViewId("power-ops-day-ahead-bid", "BidMethod", "1"),
            data_classes.MultiScenarioMatrix: dm.ViewId("power-ops-day-ahead-bid", "MultiScenarioMatrix", "1"),
            data_classes.PriceArea: dm.ViewId("power-ops-day-ahead-bid", "PriceArea", "1"),
            data_classes.SHOPMultiScenarioMethod: dm.ViewId("power-ops-day-ahead-bid", "SHOPMultiScenarioMethod", "1"),
            data_classes.SHOPPriceScenario: dm.ViewId("power-ops-day-ahead-bid", "SHOPPriceScenario", "1"),
            data_classes.SHOPPriceScenarioResult: dm.ViewId("power-ops-day-ahead-bid", "SHOPPriceScenarioResult", "1"),
            data_classes.WaterValueBasedMethod: dm.ViewId("power-ops-day-ahead-bid", "WaterValueBasedMethod", "1"),
        }

        self.alert = AlertAPI(client, view_by_read_class)
        self.basic_bid_matrix = BasicBidMatrixAPI(client, view_by_read_class)
        self.bid_document = BidDocumentAPI(client, view_by_read_class)
        self.bid_matrix = BidMatrixAPI(client, view_by_read_class)
        self.bid_method = BidMethodAPI(client, view_by_read_class)
        self.multi_scenario_matrix = MultiScenarioMatrixAPI(client, view_by_read_class)
        self.price_area = PriceAreaAPI(client, view_by_read_class)
        self.shop_multi_scenario_method = SHOPMultiScenarioMethodAPI(client, view_by_read_class)
        self.shop_price_scenario = SHOPPriceScenarioAPI(client, view_by_read_class)
        self.shop_price_scenario_result = SHOPPriceScenarioResultAPI(client, view_by_read_class)
        self.water_value_based_method = WaterValueBasedMethodAPI(client, view_by_read_class)

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> DayAheadBidAPI:
        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)
        config = ClientConfig.default(project, cdf_cluster, credentials)

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str, section: str | None = "cognite") -> DayAheadBidAPI:
        import toml

        toml_content = toml.load(file_path)
        if section is not None:
            try:
                toml_content = toml_content[section]
            except KeyError as e:
                raise ValueError(f"Could not find section '{section}' in {file_path}") from e

        return cls.azure_project(**toml_content)

    def _repr_html_(self) -> str:
        return """<strong>DayAheadBidAPI</strong> generated from data model ("power-ops-day-ahead-bid", "DayAheadBid", "1")<br />
with the following APIs available<br />
&nbsp;&nbsp;&nbsp;&nbsp;.alert<br />
&nbsp;&nbsp;&nbsp;&nbsp;.basic_bid_matrix<br />
&nbsp;&nbsp;&nbsp;&nbsp;.bid_document<br />
&nbsp;&nbsp;&nbsp;&nbsp;.bid_matrix<br />
&nbsp;&nbsp;&nbsp;&nbsp;.bid_method<br />
&nbsp;&nbsp;&nbsp;&nbsp;.multi_scenario_matrix<br />
&nbsp;&nbsp;&nbsp;&nbsp;.price_area<br />
&nbsp;&nbsp;&nbsp;&nbsp;.shop_multi_scenario_method<br />
&nbsp;&nbsp;&nbsp;&nbsp;.shop_price_scenario<br />
&nbsp;&nbsp;&nbsp;&nbsp;.shop_price_scenario_result<br />
&nbsp;&nbsp;&nbsp;&nbsp;.water_value_based_method<br />
"""
