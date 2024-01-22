from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient

from cognite.powerops.utils.cdf import Settings, get_client_config
from cognite.powerops.utils.serialization import read_toml_file

from ._generated.afrr_bid import AFRRBidAPI
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
        self.day_ahead_bid = DayAheadBidAPI(self.cdf)
        self.shop = SHOPRunAPI(self.cdf, self.datasets.write_dataset_id, cogshop_version)
        self.workflow = DayaheadTriggerAPI(self.cdf, self.datasets.write_dataset_id, cogshop_version)

    def _apis(self) -> dict[str, str]:
        return {
            "cdf": "The regular Cognite Client",
            "cog_shop1": "The CogSHOP client, this is used by cogshop",
            "assets": "The PowerOps Assets model. For example, plants, generators etc",
            "afrr_bid": "The AFRR bid model, the model used to represent AFRR bids",
            "production": "(will be deprecated) The production model",
            "day_ahead_bid": "The day ahead bid model, the model used to represent day-ahead bids",
            "shop": "The shop model, this is used to trigger individual SHOP runs",
            "workflow": "The workflow model, this is used to trigger set of SHOP runs",
        }

    def _repr_html_(self) -> str:
        return (
            "<strong>PowerOpsClient:</strong><ul>"
            + "".join([f"<li><strong><em>.{k}</em></strong>: {v}</li>" for k, v in self._apis().items()])
            + "</ul>"
        )

    def __str__(self):
        return f"PowerOpsClient with {', '.join(map(lambda a: '.' + a, self._apis().keys()))} APIs"

    @classmethod
    def from_client(cls, client: CogniteClient) -> PowerOpsClient:
        """
        Create a PowerOpsClient from a CogniteClient object.

        This uses default values for the read and write data sets, cogshop version and monitor data set.

        Args:
            client: The CogniteClient object.

        Returns:
            A PowerOpsClient object.
        """
        return PowerOpsClient(
            config=client.config,
            read_dataset="uc:000:powerops",
            write_dataset="uc:000:powerops",
            cogshop_version="CogShop2-20231030T120815Z",
            monitor_dataset="uc:po:monitoring",
        )

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

    @classmethod
    def from_toml(cls, toml_file: str | Path) -> PowerOpsClient:
        """
        Create a PowerOpsClient from a TOML file.

        Args:
            toml_file: Path to the TOML file.

        Returns:
            A PowerOpsClient object.
        """
        content = read_toml_file(toml_file)
        return cls.from_settings(Settings.model_validate(content))
