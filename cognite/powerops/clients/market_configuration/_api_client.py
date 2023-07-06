from __future__ import annotations

import getpass
from pathlib import Path

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials

from ._api.benchmark_bids import BenchmarkBidsAPI
from ._api.benchmarkings import BenchmarkingsAPI
from ._api.bids import BidsAPI
from ._api.date_transformations import DateTransformationsAPI
from ._api.day_ahead_bids import DayAheadBidsAPI
from ._api.day_ahead_process import DayAheadProcessAPI
from ._api.markets import MarketsAPI
from ._api.nord_pool_markets import NordPoolMarketsAPI
from ._api.production_plan_time_series import ProductionPlanTimeSeriesAPI
from ._api.rkom_bid_combinations import RKOMBidCombinationsAPI
from ._api.rkom_bids import RKOMBidsAPI
from ._api.rkom_markets import RKOMMarketsAPI
from ._api.rkom_process import RKOMProcessAPI
from ._api.shop_transformations import ShopTransformationsAPI


class MarketConfigClient:
    """
    MarketConfigClient

    Generated with:
        pygen = 0.11.4
        cognite-sdk = 6.5.8
        pydantic = 2.0.2

    Data Model:
        space: power-ops
        externalId: MarketConfiguration
        version: 1
    """

    def __init__(self, config: ClientConfig | None = None):
        client = CogniteClient(config)
        self.benchmark_bids = BenchmarkBidsAPI(client)
        self.benchmarkings = BenchmarkingsAPI(client)
        self.bids = BidsAPI(client)
        self.date_transformations = DateTransformationsAPI(client)
        self.day_ahead_bids = DayAheadBidsAPI(client)
        self.day_ahead_process = DayAheadProcessAPI(client)
        self.markets = MarketsAPI(client)
        self.nord_pool_markets = NordPoolMarketsAPI(client)
        self.production_plan_time_series = ProductionPlanTimeSeriesAPI(client)
        self.rkom_bids = RKOMBidsAPI(client)
        self.rkom_bid_combinations = RKOMBidCombinationsAPI(client)
        self.rkom_markets = RKOMMarketsAPI(client)
        self.rkom_process = RKOMProcessAPI(client)
        self.shop_transformations = ShopTransformationsAPI(client)

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> MarketConfigClient:
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
    def from_toml(cls, file_path: Path | str) -> MarketConfigClient:
        import toml

        return cls.azure_project(**toml.load(file_path)["cognite"])
