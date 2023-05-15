from __future__ import annotations

from hashlib import md5
from pathlib import Path
from typing import Literal, Optional

from cognite.client.data_classes import FileMetadata
from pydantic import BaseModel

from cognite.powerops.utils.serializer import load_yaml


class ShopFileConfig(BaseModel):
    watercourse_name: str
    path: Optional[str] = None
    cogshop_file_type: Literal[
        "case",
        "model",
        "log",
        "commands",
        "extra_data",
        "water_value_cut_file_reservoir_mapping",
        "water_value_cut_file",
    ]
    md5_hash: Optional[str] = None

    @property
    def external_id(self) -> str:
        return f"SHOP_{self.watercourse_name}_{self.cogshop_file_type}"

    @property
    def metadata(self) -> dict[str, str]:
        shop_type = self.cogshop_file_type
        if shop_type == "model":
            shop_type = "case"
        if self.md5_hash is None:
            raise AttributeError("The md5 hash is not set")
        return {
            "shop:type": shop_type,
            "shop:watercourse": self.watercourse_name,
            "md5_hash": self.md5_hash,
        }

    def set_md5_hash(self, file_content: bytes) -> None:
        self.md5_hash = md5(file_content).hexdigest()

    def set_full_path(self, directory: str) -> None:
        self.path = f"{directory}/{self.path}"

    @classmethod
    def from_file_meta(cls, file_meta: FileMetadata) -> "ShopFileConfig":
        return cls(
            path=None,
            watercourse_name=file_meta.metadata["shop:watercourse"],
            cogshop_file_type=file_meta.metadata["shop:type"] if file_meta.metadata["shop:type"] != "case" else "model",
            md5_hash=file_meta.metadata.get("md5_hash"),
        )


class ShopFileConfigs(BaseModel):
    watercourses_shop: list[ShopFileConfig]

    @classmethod
    def from_yaml(cls, config_dir_path: Path) -> "ShopFileConfigs":
        configs = {}
        for field_name in cls.__fields__:
            if (config_file_path := config_dir_path / f"{field_name}.yaml").exists():
                configs[field_name] = load_yaml(config_file_path, encoding="utf-8")
        return cls(**configs)
