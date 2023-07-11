from __future__ import annotations

import itertools
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic_core.core_schema import ValidationInfo

from cognite.powerops.resync._settings import Settings
from cognite.powerops.resync.config_classes.cogshop.shop_file_config import ShopFileConfig
from cognite.powerops.resync.config_classes.market import BenchmarkingConfig, PriceScenario
from cognite.powerops.resync.config_classes.market.dayahead import BidMatrixGeneratorConfig, BidProcessConfig
from cognite.powerops.resync.config_classes.market.market import MarketConfig
from cognite.powerops.resync.config_classes.market.rkom import (
    RKOMBidCombinationConfig,
    RKOMBidProcessConfig,
    RkomMarketConfig,
)
from cognite.powerops.resync.config_classes.production.generator import GeneratorTimeSeriesMapping
from cognite.powerops.resync.config_classes.production.plant import PlantTimeSeriesMapping
from cognite.powerops.resync.config_classes.production.watercourse import WatercourseConfig
from cognite.powerops.resync.config_classes.shared import TimeSeriesMapping
from cognite.powerops.resync.utils.serializer import load_yaml


class MarketConfigs(BaseModel):
    market: MarketConfig
    benchmarks: list[BenchmarkingConfig]
    price_scenario_by_id: dict[str, PriceScenario]

    bidprocess: list[BidProcessConfig]
    bidmatrix_generators: list[BidMatrixGeneratorConfig]

    rkom_bid_process: list[RKOMBidProcessConfig]
    rkom_bid_combination: Optional[list[RKOMBidCombinationConfig]] = None
    rkom_market: Optional[RkomMarketConfig] = None

    @field_validator("price_scenario_by_id")
    def no_duplicated_scenarios(cls, value: dict[str, PriceScenario]):
        scenarios_by_hash = defaultdict(list)
        for id_, s in value.items():
            scenarios_by_hash[hash(s.model_dump_json(exclude={"name"}))].append((id_, s))
        if duplicated := [s for s in scenarios_by_hash.values() if len(s) > 1]:
            sep = ") |\n\t\t\t ("
            raise ValueError(
                f"Scenarios  "
                f"({sep.join([', '.join([id_ for id_, _ in duplicate_set]) for duplicate_set in duplicated])}) "
                f"\nare duplicated."
            )
        return value

    @field_validator("rkom_bid_combination", mode="after")
    def valid_process_external_id(cls, value: list[RKOMBidCombinationConfig], values: ValidationInfo):
        valid_ids = {process_config.external_id for process_config in values.data["rkom_bid_process"]}
        for combination in value:
            for external_id_to_validate in combination.rkom_bid_config_external_ids:
                if external_id_to_validate not in valid_ids:
                    raise ValueError(
                        f"Reference to rkom bid process config in rkom_bid_combination yaml is wrong for "
                        f"{external_id_to_validate}. "
                        f"Possible references are: {[config.external_id for config in values['rkom_bid_process']]}"
                    )
        return value

    @field_validator("bidprocess", mode="after")
    def one_default_per_price_area(cls, value: list[BidProcessConfig]):
        default_configs = Counter(
            (bid_config.price_area_name for bid_config in value if bid_config.is_default_config_for_price_area)
        )

        if price_areas_with_multiple_default_configs := [
            price_area for price_area, count in default_configs.items() if count > 1
        ]:
            raise ValueError(
                f"Multiple default bid configs for price areas: {price_areas_with_multiple_default_configs}"
            )
        return value


class ProductionConfigs(BaseModel):
    dayahead_price_timeseries: Dict[str, str]
    watercourses: list[WatercourseConfig]
    generator_time_series_mappings: list[GeneratorTimeSeriesMapping] = None
    plant_time_series_mappings: list[PlantTimeSeriesMapping] = None


class CogShopConfigs(BaseModel):
    time_series_mappings: list[TimeSeriesMapping]
    watercourses_shop: list[ShopFileConfig]


class ReSyncConfig(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    settings: Settings = Field(alias="constants")
    production: ProductionConfigs
    markets: MarketConfigs
    cogshop: CogShopConfigs

    @classmethod
    def from_yamls(cls, config_dir_path: Path, cdf_project: str) -> "ReSyncConfig":
        configs: dict[str, dict[str, Any]] = {"markets": {}, "production": {}, "cogshop": {}}
        market_keys = set(MarketConfigs.model_fields)
        core_keys = set(ProductionConfigs.model_fields)
        cogshop_keys = set(CogShopConfigs.model_fields)
        watercourse_directory_by_name = {}
        for field_name in itertools.chain(
            cls.model_fields, MarketConfigs.model_fields, ProductionConfigs.model_fields, CogShopConfigs.model_fields
        ):
            if (config_file_path := config_dir_path / f"{field_name}.yaml").exists():
                content = load_yaml(config_file_path, encoding="utf-8")
                if field_name in market_keys:
                    configs["markets"][field_name] = content
                elif field_name in core_keys:
                    if field_name == "watercourses":
                        # Setting complete paths
                        for watercourse in content:
                            if all(key in watercourse for key in ["directory", "model_raw"]):
                                watercourse["yaml_raw_path"] = (
                                    config_dir_path / watercourse["directory"] / watercourse["model_raw"]
                                )
                            if all(key in watercourse for key in ["directory", "model_processed"]):
                                watercourse["yaml_processed_path"] = (
                                    config_dir_path / watercourse["directory"] / watercourse["model_processed"]
                                )
                            if all(key in watercourse for key in ["directory", "model_raw"]):
                                watercourse_directory_by_name[watercourse["name"]] = watercourse["directory"]
                    configs["production"][field_name] = content
                elif field_name in cogshop_keys:
                    configs["cogshop"][field_name] = content
                else:
                    configs[field_name] = content

        # Completing the paths for the shop files
        for shop_file in configs["cogshop"].get("watercourses_shop", []):
            if (watercourse_name := shop_file.get("watercourse_name")) and (
                directory := watercourse_directory_by_name.get(watercourse_name)
            ):
                if "file_path" in shop_file:
                    shop_file["file_path"] = config_dir_path / directory / shop_file["file_path"]
                # For backwards compatibility
                if "path" in shop_file:
                    shop_file["path"] = config_dir_path / directory / shop_file["path"]

        # Todo Hack to get cdf project into settings.
        if "settings" in configs:
            configs["settings"]["cdf_project"] = cdf_project
        elif "constants" in configs:
            # For backwards compatibility
            configs["constants"]["cdf_project"] = cdf_project

        return cls(**configs)
