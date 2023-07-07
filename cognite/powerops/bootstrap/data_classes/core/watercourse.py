from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from cognite.powerops.bootstrap.data_classes.shared import TimeSeriesMapping
from cognite.powerops.bootstrap.utils.serializer import load_yaml


class Watercourse(BaseModel):
    name: str
    shop_penalty_limit: int = 42000


class WatercourseConfig(Watercourse):
    """
    Represents the configuration for a Watercourse

    Attributes:
        version: The version of the watercourse configuration.

    """

    version: str
    market_to_price_area: Dict[str, str]
    directory: str
    model_raw: str
    # ------------------------------------------------------------------
    yaml_raw_path: str = ""
    yaml_processed_path: str = ""  # TODO: not used here
    yaml_mapping_path: str = ""
    model_processed: str  # TODO: not used here
    model_mapping: Optional[str] = None
    tco_paths: Optional[List[str]] = None  # TODO: not used here - HEV specific
    rrs_ids: Optional[List[str]] = None
    hardcoded_mapping: Optional[TimeSeriesMapping] = None  # TODO: not used here
    hist_flow_timeseries: Optional[Dict[str, str]] = None  # TODO: not used here
    # ------------------------------------------------------------------
    production_obligation_ts_ext_ids: Optional[List[str]] = None
    plant_display_names_and_order: Dict[str, tuple[str, int]] = Field(default_factory=dict)
    reservoir_display_names_and_order: Dict[str, tuple[str, int]] = Field(default_factory=dict)
    water_value_based_method_time_series_csv_filename: Optional[str] = None

    @classmethod
    def from_yaml(cls, yaml_path: Path) -> list["WatercourseConfig"]:
        return [cls(**watercourse_raw) for watercourse_raw in load_yaml(yaml_path)]

    def load_plants_by_price_area(self, path: Path) -> dict[str, list]:
        content = load_yaml(path / self.directory / self.model_raw, clean_data=True)
        plants_by_prod_area = defaultdict(list)
        for plant_name, plant_attributes in content["model"]["plant"].items():
            prod_area = str(list(plant_attributes["prod_area"].values())[0])
            price_area = self.market_to_price_area[prod_area]
            plants_by_prod_area[price_area].append(plant_name)
        return plants_by_prod_area

    def plant_display_name(self, plant: str) -> Optional[str]:
        try:
            return self.plant_display_names_and_order[plant][0]  # type: ignore
        except (KeyError, TypeError):
            print(f"[WARNING] No display name for plant: {plant}")
            return None

    def plant_ordering_key(self, plant: str) -> Optional[int]:
        try:
            return self.plant_display_names_and_order[plant][1]  # type: ignore
        except (KeyError, TypeError):
            print(f"[WARNING] No ordering key for plant: {plant}")
            return None

    def reservoir_display_name(self, reservoir: str) -> Optional[str]:
        return self.reservoir_display_names_and_order[reservoir][0] if self.reservoir_display_names_and_order else None

    def reservoir_ordering_key(self, reservoir: str) -> Optional[int]:
        return self.reservoir_display_names_and_order[reservoir][1] if self.reservoir_display_names_and_order else None

    def set_shop_yaml_paths(self, path):
        self.yaml_raw_path = f"{path}/{self.directory}/{self.model_raw}"  # TODO: create these as properties
        self.yaml_processed_path = f"{path}/{self.directory}/{self.model_processed}"
        self.yaml_mapping_path = f"{path}/{self.directory}/{self.model_mapping}"
