from __future__ import annotations

from cognite.client import ClientConfig, CogniteClient

from cognite.powerops.clients.cogshop import CogShopClient
from cognite.powerops.clients.core import CoreClient
from cognite.powerops.clients.data_set_api import DataSetsAPI
from cognite.powerops.clients.market_configuration import MarketConfigClient
from cognite.powerops.clients.shop import ShopClient
from cognite.powerops.utils.cdf import Settings, get_client_config


class PowerOpsClient:
    def __init__(
        self,
        read_dataset: str,
        write_dataset: str,
        cogshop_version: str,
        config: ClientConfig,
    ):
        self.cdf = CogniteClient(config)
        data_set_api = DataSetsAPI(self.cdf, read_dataset, write_dataset)

        self.core = CoreClient(config)
        self.market_configuration = MarketConfigClient(config)
        self.cog_shop = CogShopClient(config)
        self.shop = ShopClient(self.cdf, self.cog_shop, data_set_api, cogshop_version)


def get_powerops_client(write_dataset: str | None = None):
    settings = Settings(**{"powerops": {"write_dataset": write_dataset}})
    client_config = get_client_config(settings.cognite)
    return PowerOpsClient(
        settings.powerops.read_dataset,
        settings.powerops.write_dataset,
        settings.powerops.cogshop_version,
        config=client_config,
    )
