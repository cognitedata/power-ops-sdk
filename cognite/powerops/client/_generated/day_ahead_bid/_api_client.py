from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient, data_modeling as dm
from cognite.client.credentials import OAuthClientCredentials

from ._api.alert import AlertAPI
from ._api.bid_document import BidDocumentAPI
from ._api.bid_method import BidMethodAPI
from ._api.bid_table import BidTableAPI
from ._api.price_area import PriceAreaAPI
from ._api.shop_multi_scenario import SHOPMultiScenarioAPI
from ._api.shop_table import SHOPTableAPI
from ._api.water_value_based import WaterValueBasedAPI
from ._api.water_value_table import WaterValueTableAPI
from . import data_classes


class DayAheadBidAPI:
    """
    DayAheadBidAPI

    Generated with:
        pygen = 0.32.0
        cognite-sdk = 7.5.4
        pydantic = 2.5.2

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
        view_by_write_class = {
            data_classes.AlertApply: dm.ViewId("power-ops-shared", "Alert", "1"),
            data_classes.BidDocumentApply: dm.ViewId("power-ops-day-ahead-bid", "BidDocument", "1"),
            data_classes.BidMethodApply: dm.ViewId("power-ops-day-ahead-bid", "BidMethod", "1"),
            data_classes.BidTableApply: dm.ViewId("power-ops-day-ahead-bid", "BidTable", "1"),
            data_classes.PriceAreaApply: dm.ViewId("power-ops-day-ahead-bid", "PriceArea", "1"),
            data_classes.SHOPMultiScenarioApply: dm.ViewId("power-ops-day-ahead-bid", "SHOPMultiScenario", "1"),
            data_classes.SHOPTableApply: dm.ViewId("power-ops-day-ahead-bid", "SHOPTable", "1"),
            data_classes.WaterValueBasedApply: dm.ViewId("power-ops-day-ahead-bid", "WaterValueBased", "1"),
            data_classes.WaterValueTableApply: dm.ViewId("power-ops-day-ahead-bid", "WaterValueTable", "1"),
        }

        self.alert = AlertAPI(client, view_by_write_class)
        self.bid_document = BidDocumentAPI(client, view_by_write_class)
        self.bid_method = BidMethodAPI(client, view_by_write_class)
        self.bid_table = BidTableAPI(client, view_by_write_class)
        self.price_area = PriceAreaAPI(client, view_by_write_class)
        self.shop_multi_scenario = SHOPMultiScenarioAPI(client, view_by_write_class)
        self.shop_table = SHOPTableAPI(client, view_by_write_class)
        self.water_value_based = WaterValueBasedAPI(client, view_by_write_class)
        self.water_value_table = WaterValueTableAPI(client, view_by_write_class)

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
