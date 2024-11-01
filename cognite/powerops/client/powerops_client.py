from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from string import Template

import yaml
from cognite.client import CogniteClient, global_config
from dotenv import load_dotenv

from cognite.powerops.client.shop.cogshop_api import CogShopAPI
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
        client: CogniteClient,
        read_dataset: str,
        write_dataset: str,
        monitor_dataset: str | None = None,
    ):
        self.cdf = client
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

        # Read in yaml file
        env_sub_template = Template(config_path.read_text())

        load_dotenv()
        env_dict = dict(os.environ)

        if sys.version >= "3.11":  # Template.get_identifiers() is only available in Python 3.11 and later
            #  Fetch all environment variables referenced in the file to check if they are set
            all_identifiers = env_sub_template.get_identifiers()
            missing_env_vars = set(all_identifiers) - set(env_dict.keys())
            if missing_env_vars:
                raise ValueError(f"Missing environment variables: {missing_env_vars}")

        # Substitute environment variables in the file string
        file_env_parsed = env_sub_template.substitute(env_dict)

        # Load yaml file string into a dictionary to parse global and client configurations
        cognite_config = yaml.safe_load(file_env_parsed)

        # If you want to set a global configuration it must be done before creating the client
        if "global" in cognite_config:
            global_config.apply_settings(cognite_config["global"])

        client = CogniteClient.load(cognite_config["client"])

        power_ops_config = cognite_config.get("power_ops", {})

        return cls(client=client, **power_ops_config)
