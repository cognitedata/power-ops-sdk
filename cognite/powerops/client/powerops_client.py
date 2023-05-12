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
    ROOMBidConfigurationsAPI,
    WatercourseAPI,
)
from cognite.powerops.client.config_client import ConfigurationClient
from cognite.powerops.client.mapping_client import MappingClient
from cognite.powerops.client.transformation_client import TransformationClient
from cognite.powerops.config import BootstrapConfig


class SHOPAPI:
    def __init__(self):
        ...

    def run(
        self, external_id: str, configuration: BootstrapConfig, shop_version: str
    ) -> dict:  # TODO is BootstrapConfig correct?
        """Create a ShopRun event and a DM Case"""


class ConfigurationsClient:
    def __init__(self, read_dataset: str, write_dataset: str, client: CogniteClient):
        self.bids = BidConfigurationsAPI(client, read_dataset, write_dataset)
        self.rkom_bids = ROOMBidConfigurationsAPI(client, read_dataset, write_dataset)
        self.bechmarkings = BenchmarkingConfigurationsAPI(client, read_dataset, write_dataset)
        self.markets = MarketConfigurationsAPI(client, read_dataset, write_dataset)
        self.rkom_bid_combinations = RKOMBidCombinationConfiguration(client, read_dataset, write_dataset)


class PowerOpsClient:
    def __init__(self, read_dataset: str, write_dataset: str, config: Optional[ClientConfig] = None):
        self.core = CogniteClient(config)
        self.configurations = ConfigurationClient()

        self.configurations = ConfigurationsClient(read_dataset, write_dataset, self.core)
        self.generators = GeneratorsAPI(self.core, read_dataset, write_dataset)
        self.plants = PlantAPI(self.core, read_dataset, write_dataset)
        self.price_areas = PriceAreasAPI(self.core, read_dataset, write_dataset)
        self.reservoirs = ReservoirsAPI(self.core, read_dataset, write_dataset)
        self.watercourses = WatercourseAPI(self.core, read_dataset, write_dataset)

        self.shop = SHOPAPI()

        self.mappings = MappingClient()
        self.transformations = TransformationClient()
        ...

        # low-level clients:
        # self._dm = get_power_ops_dm_client()  # manage DM items (instances) directly
        # self._cdf = self._dm._client  # CogniteClient plus DM v3 client (Nodes, Edges, Spaces, etc.)
