from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient

from cognite.powerops.client.shop.cogshop_api import CogShopAPI
from cognite.powerops.utils.cdf import Settings, get_client_config
from cognite.powerops.utils.serialization import read_toml_file
from cognite.pygen.utils.external_id_factories import ExternalIdFactory

from ._generated.afrr_bid import AFRRBidAPI
from ._generated.assets import PowerAssetAPI
from ._generated.cogshop1 import CogShop1Client
from ._generated.day_ahead_bid import DayAheadBidAPI
from ._generated.v1 import PowerOpsModelsV1Client
from ._generated.v1.data_classes._core import DomainModelWrite
from .data_set_api import DataSetsAPI
from .shop.dayahead_trigger_api import DayaheadTriggerAPI
from .shop.shop_run_api import SHOPRunAPI

# max_domain = max_total (255) - uuid (32) + separator (1)  noqa: ERA001
_MAX_DOMAIN_LENGTH = 233


class PowerOpsClient:
    def __init__(
        self,
        read_dataset: str,
        write_dataset: str,
        config: ClientConfig,
        monitor_dataset: str | None = None,
    ):
        self.cdf = CogniteClient(config)
        self.datasets = DataSetsAPI(self.cdf, read_dataset, write_dataset, monitor_dataset)
        self.cog_shop1 = CogShop1Client(self.cdf)
        self.assets = PowerAssetAPI(self.cdf)
        self.afrr_bid = AFRRBidAPI(self.cdf)
        self.day_ahead_bid = DayAheadBidAPI(self.cdf)
        self.shop = SHOPRunAPI(self.cdf, self.datasets.write_dataset_id)
        self.workflow = DayaheadTriggerAPI(self.cdf, self.datasets.write_dataset_id)

        self.cogshop = CogShopAPI(self.cdf)
        self.v1 = PowerOpsModelsV1Client(self.cdf)

        DomainModelWrite.external_id_factory = ExternalIdFactory.create_external_id_factory(
            prefix_ext_id_factory=ExternalIdFactory(
                ExternalIdFactory.domain_name_factory(),
                shorten_length=_MAX_DOMAIN_LENGTH,
            ),
            override_external_id=False,
        )

    def _apis(self) -> dict[str, str]:
        return {
            "cdf": "The regular Cognite Client",
            "cog_shop1": " (Deprecated, use cogshop instead) The CogSHOP client, this is used by cogshop",
            "assets": "(Deprecated) The PowerOps Assets model. For example, plants, generators etc",
            "afrr_bid": "(Deprecated) The AFRR bid model, the model used to represent AFRR bids",
            "day_ahead_bid": "(Deprecated) The day ahead bid model, the model used to represent day-ahead bids",
            "shop": "(Deprecated, use cogshop instead) The shop model, this is used to trigger individual SHOP runs",
            "workflow": "(Deprecated) The workflow model, this is used to trigger set of SHOP runs",
            "cogshop": "The CogShop client, this is used to trigger SHOP runs",
            "v1": "The PowerOps Data Models client, this is used to interact with the PowerOps Models API"
            " Will be moved to top level in the future",
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
    def from_client(
        cls,
        client: CogniteClient,
        *,
        read_dataset: str | None = None,
        write_dataset: str | None = None,
        monitor_dataset: str | None = None,
    ) -> PowerOpsClient:
        """
        Create a PowerOpsClient from a CogniteClient object.

        This uses default values for the read and write data sets, cogshop version and monitor data set.

        Args:
            client: The CogniteClient object.
            read_dataset: externalId of read data set. Optional, by default loaded from the settings object.
            write_dataset: externalId of write data set. Optional, by default loaded from the settings object.
            monitor_dataset: externalId of monitor data set. Optional, by default loaded from the settings object.

        Returns:
            A PowerOpsClient object.
        """
        return cls.from_settings(
            config=client.config,
            read_dataset=read_dataset,
            write_dataset=write_dataset,
            monitor_dataset=monitor_dataset,
        )

    @classmethod
    def from_settings(
        cls,
        settings: Settings | None = None,
        *,
        config: ClientConfig | None = None,
        read_dataset: str | None = None,
        write_dataset: str | None = None,
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

        Returns:
            A PowerOpsClient object.
        """
        settings = settings or Settings()

        client_config = config if config is not None else get_client_config(settings.cognite)

        return PowerOpsClient(
            config=client_config,
            read_dataset=(read_dataset if read_dataset is not None else settings.powerops.read_dataset),
            write_dataset=(write_dataset if write_dataset is not None else settings.powerops.write_dataset),
            monitor_dataset=(monitor_dataset if monitor_dataset is not None else settings.powerops.monitor_dataset),
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
        return cls.from_settings(settings=Settings.model_validate(content))
