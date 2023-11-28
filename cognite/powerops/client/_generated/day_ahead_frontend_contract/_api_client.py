from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient, data_modeling as dm
from cognite.client.credentials import OAuthClientCredentials

from ._api.alert import AlertAPI
from ._api.bid import BidAPI
from ._api.bid_method import BidMethodAPI
from ._api.bid_table import BidTableAPI
from ._api.market_price_area import MarketPriceAreaAPI
from ._api.production_price_pair import ProductionPricePairAPI
from ._api.shop import SHOPAPI
from ._api.shop_table import SHOPTableAPI
from ._api.water_value_based import WaterValueBasedAPI
from . import data_classes


class DayAheadFrontendContractAPI:
    """
    DayAheadFrontendContractAPI

    Generated with:
        pygen = 0.31.0
        cognite-sdk = 7.4.0
        pydantic = 2.5.2

    Data Model:
        space: dayAheadFrontendContractModel
        externalId: DayAheadFrontendContract
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
            data_classes.AlertApply: dm.ViewId("dayAheadFrontendContractModel", "Alert", "1"),
            data_classes.BidApply: dm.ViewId("dayAheadFrontendContractModel", "Bid", "1"),
            data_classes.BidMethodApply: dm.ViewId("dayAheadFrontendContractModel", "BidMethod", "1"),
            data_classes.BidTableApply: dm.ViewId("dayAheadFrontendContractModel", "BidTable", "1"),
            data_classes.MarketPriceAreaApply: dm.ViewId("dayAheadFrontendContractModel", "MarketPriceArea", "1"),
            data_classes.ProductionPricePairApply: dm.ViewId(
                "dayAheadFrontendContractModel", "ProductionPricePair", "1"
            ),
            data_classes.SHOPApply: dm.ViewId("dayAheadFrontendContractModel", "SHOP", "1"),
            data_classes.SHOPTableApply: dm.ViewId("dayAheadFrontendContractModel", "SHOPTable", "1"),
            data_classes.WaterValueBasedApply: dm.ViewId("dayAheadFrontendContractModel", "WaterValueBased", "1"),
        }

        self.alert = AlertAPI(client, view_by_write_class)
        self.bid = BidAPI(client, view_by_write_class)
        self.bid_method = BidMethodAPI(client, view_by_write_class)
        self.bid_table = BidTableAPI(client, view_by_write_class)
        self.market_price_area = MarketPriceAreaAPI(client, view_by_write_class)
        self.production_price_pair = ProductionPricePairAPI(client, view_by_write_class)
        self.shop = SHOPAPI(client, view_by_write_class)
        self.shop_table = SHOPTableAPI(client, view_by_write_class)
        self.water_value_based = WaterValueBasedAPI(client, view_by_write_class)

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> DayAheadFrontendContractAPI:
        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)
        config = ClientConfig.default(project, cdf_cluster, credentials)

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str, section: str | None = "cognite") -> DayAheadFrontendContractAPI:
        import toml

        toml_content = toml.load(file_path)
        if section is not None:
            try:
                toml_content = toml_content[section]
            except KeyError as e:
                raise ValueError(f"Could not find section '{section}' in {file_path}") from e

        return cls.azure_project(**toml_content)
