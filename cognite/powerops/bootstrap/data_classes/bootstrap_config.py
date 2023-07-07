from __future__ import annotations

import itertools
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic_core.core_schema import ValidationInfo

from cognite.powerops.bootstrap._settings import Settings
from cognite.powerops.bootstrap.data_classes.core.generator import GeneratorTimeSeriesMapping
from cognite.powerops.bootstrap.data_classes.core.plant import PlantTimeSeriesMapping
from cognite.powerops.bootstrap.data_classes.core.watercourse import WatercourseConfig
from cognite.powerops.bootstrap.data_classes.marked_configuration import BenchmarkingConfig, PriceScenario
from cognite.powerops.bootstrap.data_classes.marked_configuration.dayahead import (
    BidMatrixGeneratorConfig,
    BidProcessConfig,
)
from cognite.powerops.bootstrap.data_classes.marked_configuration.market import MarketConfig
from cognite.powerops.bootstrap.data_classes.marked_configuration.rkom import (
    RKOMBidCombinationConfig,
    RKOMBidProcessConfig,
    RkomMarketConfig,
)
from cognite.powerops.bootstrap.data_classes.shared import TimeSeriesMapping
from cognite.powerops.bootstrap.data_classes.shop_file_config import ShopFileConfig
from cognite.powerops.bootstrap.utils.serializer import load_yaml


class MarketConfigs(BaseModel):
    market: MarketConfig
    benchmarks: list[BenchmarkingConfig]
    price_scenario_by_id: dict[str, PriceScenario]

    bidprocess: list[BidProcessConfig]
    bidmatrix_generators: list[BidMatrixGeneratorConfig]
    dayahead_price_timeseries: Dict[str, str]

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


class CoreConfigs(BaseModel):
    source_path: Path
    watercourses: list[WatercourseConfig]
    time_series_mappings: list[TimeSeriesMapping]
    generator_time_series_mappings: list[GeneratorTimeSeriesMapping] = None
    plant_time_series_mappings: list[PlantTimeSeriesMapping] = None

    @property
    def watercourse_directories(self) -> dict[str, str]:
        return {w.name: str(self.source_path / w.directory) for w in self.watercourses}


class BootstrapConfig(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    settings: Settings = Field(alias="constants")
    core: CoreConfigs
    markets: MarketConfigs
    watercourses_shop: list[ShopFileConfig]

    @classmethod
    def from_yamls(cls, config_dir_path: Path, cdf_project: str) -> "BootstrapConfig":
        configs = {"markets": {}, "core": {"source_path": config_dir_path}}
        market_keys = set(MarketConfigs.model_fields)
        core_keys = set(CoreConfigs.model_fields)
        for field_name in itertools.chain(cls.model_fields, MarketConfigs.model_fields, CoreConfigs.model_fields):
            if (config_file_path := config_dir_path / f"{field_name}.yaml").exists():
                content = load_yaml(config_file_path, encoding="utf-8")
                if field_name in market_keys:
                    configs["markets"][field_name] = content
                elif field_name in core_keys:
                    configs["core"][field_name] = content
                else:
                    configs[field_name] = content
        # Todo Hack to get cdf project into settings.
        if "settings" in configs:
            configs["settings"]["cdf_project"] = cdf_project
        elif "constants" in configs:
            # For backwards compatiblitity
            configs["constants"]["cdf_project"] = cdf_project
        config = cls(**configs)
        for watercourse in config.core.watercourses:
            watercourse.set_shop_yaml_paths(config.core.source_path)
        return config
