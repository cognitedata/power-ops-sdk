from typing import Optional

from cognite.client import ClientConfig, CogniteClient

from cognite.powerops.client.asset_apis import (
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
from cognite.powerops.client.config_client import ConfigurationClient
from cognite.powerops.client.dm.client import get_power_ops_dm_client
from cognite.powerops.client.dm_apis import CaseAPI, CommandsAPI, MappingAPI, ScenarioAPI, TransformationAPI
from cognite.powerops.client.shop_api import ShopAPI


class ConfigurationsClient:
    def __init__(self, read_dataset: str, write_dataset: str, client: CogniteClient):
        self.bids = BidConfigurationsAPI(client, read_dataset, write_dataset)
        self.rkom_bids = RKOMBidConfigurationsAPI(client, read_dataset, write_dataset)
        self.bechmarkings = BenchmarkingConfigurationsAPI(client, read_dataset, write_dataset)
        self.markets = MarketConfigurationsAPI(client, read_dataset, write_dataset)
        self.rkom_bid_combinations = RKOMBidCombinationConfiguration(client, read_dataset, write_dataset)


class PowerOpsClient:
    def __init__(self, read_dataset: str, write_dataset: str, config: Optional[ClientConfig] = None):
        self.dm = get_power_ops_dm_client(config=config)
        self.core = CogniteClient(config)

        self.configurations = ConfigurationClient()

        self.shop = ShopAPI(po_client=self)

        self.configurations = ConfigurationsClient(read_dataset, write_dataset, self.core)
        self.generators = GeneratorsAPI(self.core, read_dataset, write_dataset)
        self.plants = PlantAPI(self.core, read_dataset, write_dataset)
        self.price_areas = PriceAreasAPI(self.core, read_dataset, write_dataset)
        self.reservoirs = ReservoirsAPI(self.core, read_dataset, write_dataset)
        self.watercourses = WatercourseAPI(self.core, read_dataset, write_dataset)

        self.cases = CaseAPI(self.dm)
        self.commands = CommandsAPI(self.dm)
        self.scenarios = ScenarioAPI(self.dm)
        self.mappings = MappingAPI(self.dm)
        self.transformations = TransformationAPI(self.dm)
