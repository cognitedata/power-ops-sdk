from functools import cached_property

from cognite.client import ClientConfig, CogniteClient

from cognite.powerops.client.api.asset_apis import (
    BenchmarkingConfigurationsAPI,
    BidConfigurationsAPI,
    GeneratorsAPI,
    MarketConfigurationsAPI,
    PlantAPI,
    PriceAreasAPI,
    ReservoirsAPI,
    RKOMBidCombinationConfiguration,
    RKOMBidConfigurationsAPI,
    WatercourseAPI,
)
from cognite.powerops.client.api.shop_api import ShopAPI
from cognite.powerops.client.config_client import ConfigurationClient
from cognite.powerops.client.dm_client import CogShopClient
from cognite.powerops.settings import settings
from cognite.powerops.utils.cdf_auth import get_client_config, get_cognite_client
from cognite.powerops.utils.cdf_utils import retrieve_dataset


class ConfigurationsClient:
    def __init__(self, read_dataset: str, write_dataset: str, client: CogniteClient):
        self.bids = BidConfigurationsAPI(client, read_dataset, write_dataset)
        self.rkom_bids = RKOMBidConfigurationsAPI(client, read_dataset, write_dataset)
        self.bechmarkings = BenchmarkingConfigurationsAPI(client, read_dataset, write_dataset)
        self.markets = MarketConfigurationsAPI(client, read_dataset, write_dataset)
        self.rkom_bid_combinations = RKOMBidCombinationConfiguration(client, read_dataset, write_dataset)


class PowerOpsClient:
    def __init__(
        self,
        read_dataset: str,
        write_dataset: str,
        cogshop_version: str,
        config: ClientConfig,
    ):
        self._read_dataset = read_dataset
        self._write_dataset = write_dataset
        self._cogshop_version = cogshop_version

        self.cdf = CogniteClient(config=config) if config else get_cognite_client()
        self.dm = CogShopClient(self.cdf.config)

        self.configurations = ConfigurationClient()

        self.shop = ShopAPI(po_client=self)

        self.configurations = ConfigurationsClient(read_dataset, write_dataset, self.cdf)
        self.generators = GeneratorsAPI(self.cdf, read_dataset, write_dataset)
        self.plants = PlantAPI(self.cdf, read_dataset, write_dataset)
        self.price_areas = PriceAreasAPI(self.cdf, read_dataset, write_dataset)
        self.reservoirs = ReservoirsAPI(self.cdf, read_dataset, write_dataset)
        self.watercourses = WatercourseAPI(self.cdf, read_dataset, write_dataset)

    @classmethod
    def from_settings(cls):
        return cls(
            read_dataset=settings.powerops.read_dataset,
            write_dataset=settings.powerops.write_dataset,
            cogshop_version=settings.powerops.cogshop_version,
            config=get_client_config(),
        )

    @cached_property
    def read_dataset_id(self) -> int:
        return retrieve_dataset(self.cdf, external_id=self._read_dataset).id

    @cached_property
    def write_dataset_id(self) -> int:
        return retrieve_dataset(self.cdf, external_id=self._write_dataset).id

    @property
    def cogshop_version(self) -> str:
        return self._cogshop_version
