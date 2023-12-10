from __future__ import annotations

from cognite.client import ClientConfig, CogniteClient

from cognite.powerops.utils.cdf import Settings, get_client_config

from ._generated._api_client import ProductionAPIs
from ._generated.affr_bid import AFRRBidAPI
from ._generated.assets import PowerAssetAPI
from ._generated.cogshop1 import CogShop1Client
from ._generated.day_ahead_bid import DayAheadBidAPI
from .data_set_api import DataSetsAPI
from .shop.api.dayahead_trigger_api import DayaheadTriggerAPI
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
        self.cogshop_version = cogshop_version
        self.datasets = DataSetsAPI(self.cdf, read_dataset, write_dataset, monitor_dataset)
        self.cog_shop1 = CogShop1Client(self.cdf)
        self.assets = PowerAssetAPI(self.cdf)
        self.afrr_bid = AFRRBidAPI(self.cdf)
        self.production = ProductionAPIs(self.cdf)
        self.day_ahead_bid = DayAheadBidAPI(self.cdf)
        self.shop = SHOPRunAPI(self.cdf, self.datasets.write_dataset_id, cogshop_version)
        self.workflow = DayaheadTriggerAPI(self.cdf, self.datasets.write_dataset_id, cogshop_version)

    @classmethod
    def from_settings(
        cls,
        settings: Settings | None = None,
        *,
        config: ClientConfig | None = None,
        read_dataset: str | None = None,
        write_dataset: str | None = None,
        cogshop_version: str | None = None,
        monitor_dataset: str | None = None,
    ) -> PowerOpsClient:
        """
        Create a PowerOpsClient from a Settings object.


        Args:
            settings: The settings object. If no settings object is provided, the settings object is loaded from
                the environment. When loading the Settings object from the environment,
                the environment variable `SETTINGS_FILES` is used to
                specify which files to load. The default value is `settings.toml;.secrets.toml`.
            config: The client config object. Optional, by default it is loaded from the settings object.
            read_dataset: externalId of read data set. Optional, by default loaded from the settings object.
            write_dataset: externalId of write data set. Optional, by default loaded from the settings object.
            monitor_dataset: externalId of monitor data set. Optional, by default loaded from the settings object.
            cogshop_version: tag for the "cog-shop" Docker image. Optional, by default loaded from the settings object.

        Returns:
            A PowerOpsClient object.
        """
        settings = settings or Settings()

        client_config = config if config is not None else get_client_config(settings.cognite)

        return PowerOpsClient(
            config=client_config,
            read_dataset=read_dataset if read_dataset is not None else settings.powerops.read_dataset,
            write_dataset=write_dataset if write_dataset is not None else settings.powerops.write_dataset,
            monitor_dataset=monitor_dataset if monitor_dataset is not None else settings.powerops.monitor_dataset,
            cogshop_version=cogshop_version if cogshop_version is not None else settings.powerops.cogshop_version,
        )
