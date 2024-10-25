from __future__ import annotations

import os
from pathlib import Path
from string import Template

import yaml
import logging
from dotenv import load_dotenv

from cognite.client import CogniteClient, global_config

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

logger = logging.getLogger(__name__)

class PowerOpsClient:
    def __init__(
        self,
        # read_dataset: str,
        # write_dataset: str,
        client: CogniteClient,
        # monitor_dataset: str | None = None,
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
    def from_config(
        cls,
        config_path: Path | str,
    ) -> PowerOpsClient:
        """
        Create a PowerOpsClient from a configuration file.

        Args:
            config_path: The path to a yaml configuration file.

        Returns:
            A PowerOpsClient object.
        """

        if isinstance(config_path, str):
            config_path = Path(config_path)

        # Read in yaml file and fetch all environment variables referenced in the file
        env_sub_template = Template(config_path.read_text())
        all_identifiers = env_sub_template.get_identifiers()

        load_dotenv()
        env_dict = dict(os.environ)

        missing_env_vars = set(all_identifiers) - set(env_dict.keys())
        if missing_env_vars:
            raise ValueError(f"Missing environment variables: {missing_env_vars}")

        # Substitute environment variables in the file string
        file_env_parsed = env_sub_template.substitute(env_dict)

        # Load yaml file string into a dictionary to parse global and client configurations
        cognite_config = yaml.safe_load(file_env_parsed)

        # If you want to set a global configuration it must be done before creating the client
        global_config.apply_settings(cognite_config["global"])
        client = CogniteClient.load(cognite_config["client"])

        power_ops_config = cognite_config["powerops"]

        return cls(
            client=client,
        )