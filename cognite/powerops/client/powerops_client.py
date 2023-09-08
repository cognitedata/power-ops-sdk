from __future__ import annotations

from cognite.client import ClientConfig, CogniteClient

from ._generated._api_client import BenchmarkAPIs, CogShopAPIs, DayAheadAPIs, ProductionAPIs, RKOMMarketAPIs
from .data_set_api import DataSetsAPI
from ._generated.cogshop1 import CogShop1Client
from .shop import ShopClient
from cognite.powerops.utils.cdf import Settings, get_client_config


class PowerOpsClient:
    def __init__(self, read_dataset: str, write_dataset: str, cogshop_version: str, config: ClientConfig):
        self.cdf = CogniteClient(config)
        data_set_api = DataSetsAPI(self.cdf, read_dataset, write_dataset)

        self.production = ProductionAPIs(self.cdf)
        self.dayahead = DayAheadAPIs(self.cdf)
        self.rkom = RKOMMarketAPIs(self.cdf)
        self.benchmark = BenchmarkAPIs(self.cdf)
        self.cog_shop = CogShopAPIs(self.cdf)
        self.shop = ShopClient(self.cdf, self.cog_shop, data_set_api, cogshop_version)
        self.cog_shop1 = CogShop1Client(self.cdf)


def get_powerops_client(write_dataset: str | None = None) -> PowerOpsClient:
    settings = Settings(**{"powerops": {"write_dataset": write_dataset}})
    client_config = get_client_config(settings.cognite)

    return PowerOpsClient(
        settings.powerops.read_dataset,
        settings.powerops.write_dataset,
        settings.powerops.cogshop_version,
        config=client_config,
    )
