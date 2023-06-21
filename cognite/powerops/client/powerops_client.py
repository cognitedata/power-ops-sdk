from __future__ import annotations

import getpass
from functools import cached_property
from pathlib import Path
from typing import Optional

import yaml
from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials

from cognite.powerops.client.api.asset_apis import (
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

# from cognite.powerops.client.api.dm_apis import CaseAPI, CommandsAPI, MappingAPI, ScenarioAPI, TransformationAPI
from cognite.powerops.client.api.shop_api import ShopAPI
from cognite.powerops.client.config_client import ConfigurationClient
from cognite.powerops.utils.cdf_utils import retrieve_dataset


class ConfigurationsClient:
    def __init__(self, read_dataset: str, write_dataset: str, client: CogniteClient):
        self.bids = BidConfigurationsAPI(client, read_dataset, write_dataset)
        self.rkom_bids = RKOMBidConfigurationsAPI(client, read_dataset, write_dataset)
        self.bechmarkings = BenchmarkingConfigurationsAPI(client, read_dataset, write_dataset)
        self.markets = MarketConfigurationsAPI(client, read_dataset, write_dataset)
        self.rkom_bid_combinations = RKOMBidCombinationConfiguration(client, read_dataset, write_dataset)


class PowerOpsClient:
    def __init__(
        self,
        read_dataset: Optional[str] = None,
        write_dataset: Optional[str] = None,
        cogshop_version: Optional[str] = None,
        config: Optional[ClientConfig] = None,
        space_id: Optional[str] = None,
        data_model: Optional[str] = None,
        schema_version: Optional[int] = None,
    ):
        self._read_dataset = read_dataset
        self._write_dataset = write_dataset
        self._cogshop_version = cogshop_version

        # self.dm = get_power_ops_dm_client(
        #     config=config, space_id=space_id, data_model=data_model, schema_version=schema_version
        # )
        self.client = CogniteClient(config)

        self.configurations = ConfigurationClient()

        self.shop = ShopAPI(po_client=self)

        self.configurations = ConfigurationsClient(read_dataset, write_dataset, self.client)
        self.generators = GeneratorsAPI(self.client, read_dataset, write_dataset)
        self.plants = PlantAPI(self.client, read_dataset, write_dataset)
        self.price_areas = PriceAreasAPI(self.client, read_dataset, write_dataset)
        self.reservoirs = ReservoirsAPI(self.client, read_dataset, write_dataset)
        self.watercourses = WatercourseAPI(self.client, read_dataset, write_dataset)

        # self.cases = CaseAPI(self.dm)
        # self.commands = CommandsAPI(self.dm)
        # self.scenarios = ScenarioAPI(self.dm)
        # self.mappings = MappingAPI(self.dm)
        # self.transformations = TransformationAPI(self.dm)

    @cached_property
    def read_dataset_id(self) -> int:
        return retrieve_dataset(self.client, external_id=self._read_dataset).id

    @cached_property
    def write_dataset_id(self) -> int:
        return retrieve_dataset(self.client, external_id=self._write_dataset).id

    @property
    def cogshop_version(self) -> str:
        return self._cogshop_version

    @classmethod
    def from_file(cls, file_path: Path) -> PowerOpsClient:
        if file_path.suffix != ".yaml":
            raise ValueError(f"Unsupported file type {file_path.suffix}")
        with file_path.open("r") as file:
            content = yaml.safe_load(file)

        credentials = OAuthClientCredentials(
            token_url=f'https://login.microsoftonline.com/{content["client"]["tenant_id"]}/oauth2/v2.0/token',
            client_id=content["client"]["client_id"],
            client_secret=content["client"]["client_secret"],
            scopes=[f'https://{content["client"]["cdf_cluster"]}.cognitedata.com/.default'],
        )
        # credentials = OAuthInteractive(authority_url=f'https://login.microsoftonline.com/{content["client"]["tenant_id"]}',
        #                                client_id=content["client"]["client_id"],
        #     scopes=[f'https://{content["client"]["cdf_cluster"]}.cognitedata.com/.default'])
        config = ClientConfig(
            project=content["client"]["cognite_project"],
            credentials=credentials,
            client_name=getpass.getuser(),
            base_url=f'https://{content["client"]["cdf_cluster"]}.cognitedata.com/',
        )
        return cls(**content["datasets"], **content["shop"], **content["data_model"], config=config)
