from __future__ import annotations

from pathlib import Path
from typing import ClassVar, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from cognite.powerops.resync.config.shared import TimeSeriesMapping
from cognite.powerops.resync.utils.serializer import load_yaml


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
    market_to_price_area: Dict[str, str]
    directory: str
    model_raw: str
    # ------------------------------------------------------------------
    yaml_raw_path: Path
    yaml_processed_path: Path  # TODO: not used here
    yaml_mapping_path: str = ""
    model_processed: str  # TODO: not used here
    model_mapping: Optional[str] = None
    tco_paths: Optional[List[str]] = None  # TODO: not used here - HEV specific
    rrs_ids: Optional[List[str]] = None
    hardcoded_mapping: Optional[TimeSeriesMapping] = None  # TODO: not used here
    hist_flow_timeseries: Optional[Dict[str, str]] = None  # TODO: not used here
    # ------------------------------------------------------------------
    production_obligation_ts_ext_ids: List[str] = Field(default_factory=list)
    plant_display_names_and_order: Dict[str, tuple[str, int]] = Field(default_factory=dict)
    reservoir_display_names_and_order: Dict[str, tuple[str, int]] = Field(default_factory=dict)
    water_value_based_method_time_series_csv_filename: Optional[str] = None

    @classmethod
    def from_yaml(cls, yaml_path: Path) -> list["WatercourseConfig"]:
        return [cls(**watercourse_raw) for watercourse_raw in load_yaml(yaml_path)]
