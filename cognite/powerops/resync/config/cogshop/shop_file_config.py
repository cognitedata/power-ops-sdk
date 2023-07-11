from __future__ import annotations

from pathlib import Path
from typing import ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field


class ShopFileConfig(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(populate_by_name=True)
    watercourse_name: str
    # Alias for backwards compatibility
    file_path: Path = Field(alias="path")
    cogshop_file_type: Literal[
        "case",
        "model",
        "log",
        "commands",
        "extra_data",
        "water_value_cut_file_reservoir_mapping",
        "water_value_cut_file",
        "module_series",
    ]

    @property
    def external_id(self) -> str:
        return f"SHOP_{self.watercourse_name}_{self.file_path.stem}"
