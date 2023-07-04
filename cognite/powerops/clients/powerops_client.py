from functools import cached_property

from cognite.client import ClientConfig, CogniteClient

from cognite.powerops.bootstrap.utils import get_client_config
from cognite.powerops.bootstrap.utils.cdf_utils import retrieve_dataset
from cognite.powerops.client.api.shop_api import ShopAPI
from cognite.powerops.client.config_client import ConfigurationClient
from cognite.powerops.client.dm_client import CogShopClient
from cognite.powerops.utils.settings import settings


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
        self.dm = CogShopClient(config)

        self.configurations = ConfigurationClient()

        self.shop = ShopAPI(po_client=self)

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
