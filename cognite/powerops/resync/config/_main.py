from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Literal, Optional, overload

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_core.core_schema import FieldValidationInfo
from typing_extensions import Self

from cognite.powerops.utils.serialization import load_yaml

from ._settings import Settings
from ._shared import TimeSeriesMapping
from .cogshop.shop_file_config import ShopFileConfig
from .market import (
    BenchmarkingConfig,
    BidMatrixGeneratorConfig,
    BidProcessConfig,
    Market,
    PriceScenario,
    RKOMBidCombinationConfig,
    RKOMBidProcessConfig,
    RkomMarketConfig,
)
from .production import GeneratorTimeSeriesMapping, PlantTimeSeriesMapping, WatercourseConfig


class Config(BaseModel):
    @classmethod
    def load_yamls(cls, config_dir_path: Path) -> dict[str, Any]:
        """
        Loads all yaml files in the config directory and returns a dictionary with the file name as key and the
        content as value.

        Note the config_dir_path can be nested, all yaml files in the directory and subdirectories will be loaded.

        Parameters
        ----------
        config_dir_path: Path
            The path to the config directory

        Returns
        -------
            Dictionary with all yaml files loaded.
        """
        config: dict[str, Any] = {}
        field_names = set(cls.model_fields)
        for file_path in config_dir_path.glob("**/*.yaml"):
            if file_path.stem not in field_names:
                continue
            config[file_path.stem] = load_yaml(file_path)
        return config


class MarketConfig(Config):
    market: Market
    benchmarks: list[BenchmarkingConfig]
    price_scenario_by_id: dict[str, PriceScenario]

    bidprocess: list[BidProcessConfig]
    bidmatrix_generators: list[BidMatrixGeneratorConfig]

    rkom_bid_process: list[RKOMBidProcessConfig]
    rkom_bid_combination: Optional[list[RKOMBidCombinationConfig]] = None
    rkom_market: RkomMarketConfig = Field(default_factory=RkomMarketConfig.default)

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
    def valid_process_external_id(cls, value: list[RKOMBidCombinationConfig], values: FieldValidationInfo):
        valid_ids = {process_config.external_id for process_config in values.data["rkom_bid_process"]}
        for combination in value:
            for external_id_to_validate in combination.rkom_bid_config_external_ids:
                if external_id_to_validate not in valid_ids:
                    raise ValueError(
                        f"Reference to rkom bid process config in rkom_bid_combination yaml is wrong for "
                        f"{external_id_to_validate}. "
                        f"Possible references are: {[config.external_id for config in values.data['rkom_bid_process']]}"
                    )
        return value

    @field_validator("bidprocess", mode="after")
    def one_default_per_price_area(cls, value: list[BidProcessConfig]):
        default_configs = Counter(
            bid_config.price_area_name for bid_config in value if bid_config.is_default_config_for_price_area
        )

        if price_areas_with_multiple_default_configs := [
            price_area for price_area, count in default_configs.items() if count > 1
        ]:
            raise ValueError(
                f"Multiple default bid configs for price areas: {price_areas_with_multiple_default_configs}"
            )
        return value


class ProductionConfig(Config):
    dayahead_price_timeseries: dict[str, str]
    watercourses: list[WatercourseConfig]
    generator_time_series_mappings: Optional[list[GeneratorTimeSeriesMapping]] = None
    plant_time_series_mappings: Optional[list[PlantTimeSeriesMapping]] = None

    @classmethod
    @overload
    def load_yamls(cls, config_dir_path: Path, instantiate: Literal[False] = False) -> dict[str, Any]:
        ...

    @classmethod
    @overload
    def load_yamls(cls, config_dir_path: Path, instantiate: Literal[True]) -> ProductionConfig:
        ...

    @classmethod
    def load_yamls(cls, config_dir_path: Path, instantiate: bool = False) -> dict[str, Any] | ProductionConfig:
        config: dict[str, Any] = {}
        for field_name in cls.model_fields:
            if not (config_file_path := config_dir_path / f"{field_name}.yaml").exists():
                continue
            content = load_yaml(config_file_path)
            if field_name == "watercourses":
                for watercourse in content:
                    # Complete the paths for the yaml files
                    if all(key in watercourse for key in ["directory", "model_raw"]):
                        watercourse["yaml_raw_path"] = (
                            config_dir_path / watercourse["directory"] / watercourse["model_raw"]
                        )
                        watercourse["shop_model_template"] = load_yaml(watercourse["yaml_raw_path"], encoding="utf-8")
                    if all(key in watercourse for key in ["directory", "model_processed"]):
                        watercourse["yaml_processed_path"] = (
                            config_dir_path / watercourse["directory"] / watercourse["model_processed"]
                        )
            config[field_name] = content
        if instantiate:
            return cls(**config)
        return config


class CogShopConfig(Config):
    time_series_mappings: Optional[list[TimeSeriesMapping]] = None
    watercourses_shop: list[ShopFileConfig]


class ReSyncConfig(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    settings: Settings = Field(alias="constants")
    production: ProductionConfig
    market: MarketConfig
    cogshop: CogShopConfig

    @classmethod
    def from_yamls(cls, config_dir_path: Path, cdf_project: str) -> ReSyncConfig:
        configs: dict[str, Any] = {}
        for field in cls.model_fields:
            if (config_file_path := config_dir_path / f"{field}.yaml").exists():
                configs[field] = load_yaml(config_file_path, encoding="utf-8")
            elif (
                (config_subdir_path := config_dir_path / field).exists()
                and (class_ := cls.model_fields[field].annotation) is not None
                and issubclass(class_, Config)
            ):
                configs[field] = class_.load_yamls(config_subdir_path)

        watercourse_directory_by_name = {}
        for watercourse in configs.get("production", {}).get("watercourses", []):
            if "directory" in watercourse and "name" in watercourse:
                watercourse_directory_by_name[watercourse["name"]] = watercourse["directory"]

        # Completing the paths for the shop files
        for shop_file in configs.get("cogshop", {}).get("watercourses_shop", []):
            if (watercourse_name := shop_file.get("watercourse_name")) and (
                directory := watercourse_directory_by_name.get(watercourse_name)
            ):
                if "file_path" in shop_file:
                    shop_file["file_path"] = config_dir_path / "production" / directory / shop_file["file_path"]
                # For backwards compatibility
                if "path" in shop_file:
                    shop_file["path"] = config_dir_path / "production" / directory / shop_file["path"]

        # Todo Hack to get cdf project into settings.
        if "settings" in configs:
            configs["settings"]["cdf_project"] = cdf_project
        elif "constants" in configs:
            # For backwards compatibility
            configs["constants"]["cdf_project"] = cdf_project

        return cls(**configs)

    @model_validator(mode="after")
    def object_exists_in_shop_template_model(self) -> Self:
        production = self.production
        time_series_mappings = self.cogshop.time_series_mappings or []
        # TODO Fix the assumption that timeseries mappings and watercourses are in the same order
        invalid_mappings_by_watercourse: dict[str, list[str]] = defaultdict(list)
        seen: set[tuple[str, str]] = set()
        for watercourse, timeseries_mapping in zip(production.watercourses, time_series_mappings):
            model = watercourse.shop_model_template["model"]
            valid_mappings = _get_valid_shop_objects(model)
            for mapping in timeseries_mapping:
                entry = (mapping.object_type.lower(), mapping.object_name.lower())
                if entry not in valid_mappings and entry not in seen:
                    invalid_mappings_by_watercourse[watercourse.name].append(
                        f"{mapping.object_type}.{mapping.object_name}"
                    )
                    seen.add(entry)
        if invalid_mappings_by_watercourse:
            raise ValueError(
                "Timeseries mapping for watercourses not found in respective SHOP files: "
                f"{dict(invalid_mappings_by_watercourse)}"
            )

        return self


def _get_valid_shop_objects(model: dict[str, Any]) -> set[tuple[str, str]]:
    valid_mappings: set[tuple[str, str]] = set()
    for object_type, objects in model.items():
        if not isinstance(objects, dict):
            raise ValueError("Invalid SHOP yaml")
        for object_name, attributes in objects.items():
            if not isinstance(attributes, dict):
                raise ValueError("Invalid SHOP yaml")
            valid_mappings.add((str(object_type).lower(), str(object_name).lower()))
    return valid_mappings
