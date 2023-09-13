from __future__ import annotations

from cognite.client import ClientConfig, CogniteClient

from cognite.powerops.utils.cdf import Settings, get_client_config

from ._generated._api_client import BenchmarkAPIs, CogShopAPIs, DayAheadAPIs, ProductionAPIs, RKOMMarketAPIs
from ._generated.cogshop1 import CogShop1Client
from .data_set_api import DataSetsAPI
from .shop.shop_run_api import SHOPRunAPI


class PowerOpsClient:
    def __init__(
        self,
        read_dataset: str,
        write_dataset: str,
        cogshop_version: str,
        config: ClientConfig,
        monitor_dataset: str | None = None,
    ):
        self.cdf = CogniteClient(config)
        self.datasets = DataSetsAPI(self.cdf, read_dataset, write_dataset, monitor_dataset)
        self.production = ProductionAPIs(self.cdf)
        self.dayahead = DayAheadAPIs(self.cdf)
        self.rkom = RKOMMarketAPIs(self.cdf)
        self.benchmark = BenchmarkAPIs(self.cdf)
        self.cog_shop = CogShopAPIs(self.cdf)
        self.cog_shop1 = CogShop1Client(self.cdf)
        self.shop = SHOPRunAPI(self.cdf, self.datasets.write_dataset_id, cogshop_version)

    @classmethod
    def from_settings(cls, settings: Settings | None = None) -> PowerOpsClient:
        """
        Create a PowerOpsClient from a Settings object.


        Args:
            settings: The settings object. If no settings object is provided, the settings object is loaded from
                the environment. When loading the Settings object from the environment,
                the environment variable `SETTINGS_FILES` is used to
                specify which files to load. The default value is `settings.toml;.secrets.toml`.

        Returns:
            A PowerOpsClient object.
        """
        settings = settings or Settings()

        client_config = get_client_config(settings.cognite)

        return PowerOpsClient(
            settings.powerops.read_dataset,
            settings.powerops.write_dataset,
            settings.powerops.cogshop_version,
            config=client_config,
            monitor_dataset=settings.powerops.monitor_dataset,
        )
