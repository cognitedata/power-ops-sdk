import logging
from pathlib import Path
from typing import Optional, Sequence, Union

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, LabelDefinition


from cognite.powerops.bootstrap import get_shop_service_url
from cognite.powerops.config import BootstrapConfig, GeneratorTimeSeriesMapping, PlantTimeSeriesMapping, Watercourse, WatercourseConfig
from cognite.powerops.client.api.asset_apis import (
    BenchmarkingConfigurationsAPI,
    BidConfigurationsAPI,
    MarketConfigurationsAPI,
    RKOMBidCombinationConfiguration,
    RKOMBidConfigurationsAPI,
)
from cognite.powerops.data_classes.reservoir import Reservoir

from cognite.powerops.data_classes.shop_file_config import ShopFileConfigs
from cognite.powerops.utils.labels import create_labels
from cognite.powerops.utils.powerops_asset_hierarchy import create_skeleton_asset_hierarchy
from cognite.powerops.utils.serializer import load_yaml

logger = logging.getLogger(__name__)


class ShoConfigCommonAPI:
    def __init__(self,
                 cdf: CogniteClient,
                 read_dataset: str,
                 write_dataset: str,
                 ):
        self.cdf = cdf
        self.read_dataset = read_dataset
        self.write_dataset = write_dataset

    def list_labels(self) -> list[LabelDefinition]:
        return self.cdf.labels.list(data_set_external_ids=[self.read_dataset])

    def generate_all_label_definitions(self) -> list[LabelDefinition]:
        return create_labels()

    def create_labels(self, label_definitions: Union[LabelDefinition, Sequence[LabelDefinition]],):
        self.cdf.labels.create(label_definitions)

    def generate_and_create_labels(self):
        self.create_labels(self.generate_all_label_definitions())

    def generate_asset_hierarchy(
        self,
        shop_service_url: str,
        organization_subdomain: str,
        tenant_id: str,
    ) -> list[Asset]:
        return create_skeleton_asset_hierarchy(
            shop_service_url, organization_subdomain, tenant_id,
        )

    def create_asset_hierarchy(self, assets: Sequence[Asset]):
        self.cdf.assets.create(assets)

    def generate_and_create_asset_hierarchy(
        self,
        shop_service_url: str,
        organization_subdomain: str,
        tenant_id: str,
    ):
        self.create_asset_hierarchy(
            self.generate_asset_hierarchy(
                shop_service_url, organization_subdomain, tenant_id,
            )
        )


class WatercourseConfigAPI:
    def __init__(
        self,
        cdf: CogniteClient,
        read_dataset: str,
        write_dataset: str,
    ):
        self.cdf = cdf
        self.read_dataset = read_dataset
        self.write_dataset = write_dataset

    def create_watercourse_asset(self, watercourse: Watercourse) -> Asset:
        watercourse_asset = watercourse.to_asset()
        self.cdf.assets.create(watercourse_asset)
        return watercourse_asset

    def create_reservoir_assets(
            self, 
            shop_case_reservoirs: dict,
            watercourse_config:WatercourseConfig, 
            ) -> list[Asset]:
        reservoirs = Reservoir.from_shop_case_reservoirs(
            shop_case_reservoirs,
            watercourse_config,
            )
        reservoir_assets = [r.asset() for r in reservoirs]
        self.cdf.assets.create(reservoir_assets)
        return reservoir_assets
    
    def create_generator_assets(
            self, generators, generator_time_series_mappings, ):
        pass


    def _from_bootstrap_config(
        self,
        watercourse_config: WatercourseConfig,
        plant_time_series_mappings: Optional[list[PlantTimeSeriesMapping]],
        generator_time_series_mappings: Optional[list[GeneratorTimeSeriesMapping]],
    ):
        """
        Copied/modified from `_transform` in utils
        Creates the resources, option to not create left for later (plan/apply)
        """
        watercourse = Watercourse(
            name=watercourse_config.name,
            shop_penalty_limit=watercourse_config.shop_penalty_limit,
        )
        _ = self.create_watercourse_asset(watercourse)

        shop_case = load_yaml(
            Path(watercourse_config.yaml_raw_path), clean_data=True)
        reservoirs = shop_case["model"]["reservoir"]
        _ = self.create_reservoir_assets(reservoirs, watercourse_config)

        generators = shop_case["model"]["generator"]
        


class ShopConfigurationAPI:
    """No option to preview changes before applying them."""

    def _init__(
        self,
        cdf: CogniteClient,
        read_dataset: str,
        write_dataset: str,

    ):
        self.bids = BidConfigurationsAPI(cdf, read_dataset, write_dataset)
        self.rkom_bids = RKOMBidConfigurationsAPI(
            cdf, read_dataset, write_dataset)

        self.bechmarkings = BenchmarkingConfigurationsAPI(
            cdf, read_dataset, write_dataset)

        self.markets = MarketConfigurationsAPI(
            cdf, read_dataset, write_dataset)

        self.rkom_bid_combinations = RKOMBidCombinationConfiguration(
            cdf, read_dataset, write_dataset)

        # --------------------

        self.common = ShoConfigCommonAPI(
            cdf, read_dataset, write_dataset)

        self.watercourses = WatercourseConfigAPI(
            cdf, read_dataset, write_dataset)

    def upsert(self, configuration: BootstrapConfig, path: Path):
        cognite_project = configuration.cdf.dict()["COGNITE_PROJECT"]

        consts = configuration.constants

        watercourse_directories = {}
        for w in configuration.watercourses:
            w.set_shop_yaml_paths(path)
            watercourse_directories[w.name] = "/".join(
                (path/w.directory).parts)

        shop_files_config_list = ShopFileConfigs.from_yaml(
            path).watercourses_shop
        print(
            f"Running bootstrap for data set {consts.data_set_external_id} in CDF project" + f"{cognite_project}")

        # Is needed in asset hierarchy-- so i can add it to the resource's apis configs?
        self.common.generate_and_create_labels()
        self.common.generate_and_create_asset_hierarchy(
            shop_service_url=get_shop_service_url(cognite_project),
            organization_subdomain=consts.organization_subdomain,
            tenant_id=consts.tenant_id
        )

        for watercourse_config in configuration.watercourses:
            self.watercourses._from_bootstrap_config(
                watercourse_configs=watercourse_config,
                plant_time_series_mappings=configuration.plant_time_series_mappings,
                generator_time_series_mappings=configuration.generator_time_series_mappings,
            )
