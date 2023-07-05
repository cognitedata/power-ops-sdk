from functools import cached_property

from cognite.client import ClientConfig, CogniteClient

from cognite.powerops.clients.cogshop import CogShopClient
from cognite.powerops.clients.core import CoreClient
from cognite.powerops.clients.market_configuration import MarketConfigClient
from cognite.powerops.clients.shop import ShopClient


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

        self.cdf = CogniteClient(config)
        self.core = CoreClient(config)
        self.market_configuration = MarketConfigClient(config)
        self.cog_shop = CogShopClient(config)
        self.shop = ShopClient(self.cog_shop)

    @cached_property
    def read_dataset_id(self) -> int:
        return self._retrieve_with_raise_if_none(self._write_dataset).id

    @cached_property
    def write_dataset_id(self) -> int:
        return self._retrieve_with_raise_if_none(self._write_dataset).id

    def _retrieve_with_raise_if_none(self, dataset):
        dataset = self.cdf.data_sets.retrieve(dataset)
        if dataset is None:
            raise ValueError(f"Dataset {dataset} not found.")
        return dataset

    @property
    def cogshop_version(self) -> str:
        return self._cogshop_version
