from __future__ import annotations

from functools import cached_property
from pathlib import Path
from typing import Any, ClassVar, Optional, cast

from pydantic import BaseModel, ConfigDict, Field

from cognite.powerops.resync.config._shared import TimeSeriesMapping
from cognite.powerops.utils.serialization import load_yaml

from .generator import Generator


class Watercourse(BaseModel):
    name: str
    shop_penalty_limit: int = 42000


class WatercourseConfig(Watercourse):
    """
    Represents the configuration for a Watercourse

    Attributes:
        version: The version of the watercourse configuration.

    """

    model_config: ClassVar[ConfigDict] = ConfigDict(protected_namespaces=tuple())
    version: str
    market_to_price_area: dict[str, str]
    directory: str
    model_raw: str
    # ------------------------------------------------------------------
    shop_model_template: dict[str, Any]
    yaml_raw_path: Path
    yaml_processed_path: Path  # TODO: not used here
    yaml_mapping_path: str = ""
    model_processed: str  # TODO: not used here
    model_mapping: Optional[str] = None
    write_back_model_file: Optional[bool] = True
    tco_paths: Optional[list[str]] = None  # TODO: not used here - HEV specific
    rrs_ids: Optional[list[str]] = None
    hardcoded_mapping: Optional[TimeSeriesMapping] = None  # TODO: not used here
    hist_flow_timeseries: Optional[dict[str, str]] = None  # TODO: not used here
    # ------------------------------------------------------------------
    production_obligation_ts_ext_ids: list[str] = Field(default_factory=list)
    plant_display_names_and_order: dict[str, tuple[str, int]] = Field(default_factory=dict)
    reservoir_display_names_and_order: dict[str, tuple[str, int]] = Field(default_factory=dict)
    water_value_based_method_time_series_csv_filename: Optional[str] = None
    generators: list[Generator] = Field(default_factory=list)

    @classmethod
    def from_yaml(cls, yaml_path: Path) -> list[WatercourseConfig]:
        return [cls(**watercourse_raw) for watercourse_raw in load_yaml(yaml_path)]

    @cached_property
    def valid_shop_objects(self) -> set[tuple[str, str]]:
        if "model" not in self.shop_model_template:
            raise ValueError("Invalid SHOP yaml: Missing 'model' key")
        model = cast(dict[str, Any], self.shop_model_template["model"])
        valid_mappings: set[tuple[str, str]] = set()
        for object_type, objects in model.items():
            if not isinstance(objects, dict):
                raise ValueError("Invalid SHOP yaml")
            for object_name, attributes in objects.items():
                if not isinstance(attributes, dict):
                    raise ValueError("Invalid SHOP yaml")
                valid_mappings.add((str(object_type).lower(), str(object_name).lower()))
        return valid_mappings
