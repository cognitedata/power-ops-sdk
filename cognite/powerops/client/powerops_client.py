from functools import cached_property
from typing import Optional

from cognite.client import ClientConfig, CogniteClient
from cognite.dm_clients.config import settings

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
from cognite.powerops.client.api.dm_apis import CaseAPI, CommandsAPI, MappingAPI, ScenarioAPI, TransformationAPI
from cognite.powerops.client.api.shop_api import ShopAPI
from cognite.powerops.client.config_client import ConfigurationClient
from cognite.powerops.client.dm.client import get_power_ops_dm_client
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
        read_dataset: Optional[str] = None,
        write_dataset: Optional[str] = None,
        cogshop_version: Optional[str] = None,
        config: Optional[ClientConfig] = None,
        space_id: Optional[str] = None,
        data_model: Optional[str] = None,
        schema_version: Optional[int] = None,
    ):
        self._read_dataset = read_dataset or settings.powerops.read_dataset
        self._write_dataset = write_dataset or settings.powerops.write_dataset
        self._cogshop_version = cogshop_version or settings.powerops.get("cogshop_version", "")

        self.dm = get_power_ops_dm_client(
            config=config, space_id=space_id, data_model=data_model, schema_version=schema_version
        )
        self.cdf = self.dm._client

        self.configurations = ConfigurationClient()

        self.shop = ShopAPI(po_client=self)

        self.configurations = ConfigurationsClient(read_dataset, write_dataset, self.cdf)
        self.generators = GeneratorsAPI(self.cdf, read_dataset, write_dataset)
        self.plants = PlantAPI(self.cdf, read_dataset, write_dataset)
        self.price_areas = PriceAreasAPI(self.cdf, read_dataset, write_dataset)
        self.reservoirs = ReservoirsAPI(self.cdf, read_dataset, write_dataset)
        self.watercourses = WatercourseAPI(self.cdf, read_dataset, write_dataset)

        self.cases = CaseAPI(self.dm)
        self.commands = CommandsAPI(self.dm)
        self.scenarios = ScenarioAPI(self.dm)
        self.mappings = MappingAPI(self.dm)
        self.transformations = TransformationAPI(self.dm)

    @cached_property
    def read_dataset_id(self) -> int:
        return retrieve_dataset(self.cdf, external_id=self._read_dataset).id

    @cached_property
    def write_dataset_id(self) -> int:
        return retrieve_dataset(self.cdf, external_id=self._write_dataset).id

    @property
    def cogshop_version(self) -> str:
        return self._cogshop_version
