from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Dict, Optional

from pydantic import BaseModel, validator

from cognite.powerops.bootstrap.data_classes.common import CommonConstants
from cognite.powerops.bootstrap.data_classes.core.generator import GeneratorTimeSeriesMapping
from cognite.powerops.bootstrap.data_classes.core.plant import PlantTimeSeriesMapping
from cognite.powerops.bootstrap.data_classes.core.watercourse import WatercourseConfig
from cognite.powerops.bootstrap.data_classes.marked_configuration import (
    BenchmarkingConfig,
    BidMatrixGeneratorConfig,
    BidProcessConfig,
    MarketConfig,
    PriceScenario,
    RKOMBidCombinationConfig,
    RKOMBidProcessConfig,
    RkomMarketConfig,
)
from cognite.powerops.bootstrap.data_classes.time_series_mapping import TimeSeriesMapping
from cognite.powerops.bootstrap.utils.serializer import load_yaml

# TODO: Put this in a configgen module?


class BootstrapConfig(BaseModel):
    constants: CommonConstants
    benchmarks: list[BenchmarkingConfig]
    price_scenario_by_id: dict[str, PriceScenario]
    bidprocess: list[BidProcessConfig]
    bidmatrix_generators: list[BidMatrixGeneratorConfig]
    dayahead_price_timeseries: Dict[str, str]
    market: MarketConfig
    watercourses: list[WatercourseConfig]
    time_series_mappings: list[TimeSeriesMapping]
    rkom_bid_process: list[RKOMBidProcessConfig]
    rkom_bid_combination: Optional[list[RKOMBidCombinationConfig]] = None
    rkom_market: Optional[RkomMarketConfig] = None
    plant_time_series_mappings: list[PlantTimeSeriesMapping] = None
    generator_time_series_mappings: list[GeneratorTimeSeriesMapping] = None

    @validator("price_scenario_by_id")
    def no_duplicated_scenarios(cls, value: dict[str, PriceScenario]):
        scenarios_by_hash = defaultdict(list)
        for id_, s in value.items():
            scenarios_by_hash[hash(s.json(exclude={"name"}))].append((id_, s))
        if duplicated := [s for s in scenarios_by_hash.values() if len(s) > 1]:
            sep = ") |\n\t\t\t ("
            raise ValueError(
                f"Scenarios  "
                f"({sep.join([', '.join([id_ for id_, _ in duplicate_set]) for duplicate_set in duplicated])}) "
                f"\nare duplicated."
            )
        return value

    @validator("rkom_bid_combination", each_item=True)
    def valid_process_external_id(cls, value, values: dict):
        valid_ids = {process_config.external_id for process_config in values["rkom_bid_process"]}
        for external_id_to_validate in value.rkom_bid_config_external_ids:
            if external_id_to_validate not in valid_ids:
                raise ValueError(
                    f"Reference to rkom bid process config in rkom_bid_combination yaml is wrong for "
                    f"{external_id_to_validate}. "
                    f"Possible references are: {[config.external_id for config in values['rkom_bid_process']]}"
                )
        return value

    @classmethod
    def from_yamls(cls, config_dir_path: Path) -> "BootstrapConfig":
        configs = {}
        for field_name in cls.__fields__:
            if (config_file_path := config_dir_path / f"{field_name}.yaml").exists():
                configs[field_name] = load_yaml(config_file_path, encoding="utf-8")
        return cls(**configs)

    def validate_bid_configs(self):
        """Validate the bid configs in the bootstrap config. Per now only ensure there is at most one default config
        per price area"""

        default_configs = defaultdict(int)
        for bid_config in self.bidprocess:
            if bid_config.is_default_config_for_price_area:
                default_configs[bid_config.price_area_name] += 1

        if price_areas_with_multiple_default_configs := [
            price_area for price_area, count in default_configs.items() if count > 1
        ]:
            raise ValueError(
                f"Multiple default bid configs for price areas: {price_areas_with_multiple_default_configs}"
            )
