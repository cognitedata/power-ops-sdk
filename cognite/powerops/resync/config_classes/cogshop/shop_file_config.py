from __future__ import annotations

from hashlib import md5
from pathlib import Path
from typing import ClassVar, Literal, Optional

from cognite.client.data_classes import FileMetadata
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
    md5_hash: Optional[str] = None

    @property
    def external_id(self) -> str:
        return f"SHOP_{self.watercourse_name}_{self.file_path.stem}"

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
            "shop:file_name": self.file_path.stem,
            "md5_hash": self.md5_hash,
        }

    def set_md5_hash(self, file_content: bytes) -> None:
        self.md5_hash = md5(file_content.replace(b"\r\n", b"\n")).hexdigest()

    def set_full_path(self, directory: str) -> None:
        self.file_path = Path(directory) / self.file_path

    @classmethod
    def from_file_meta(cls, file_meta: FileMetadata) -> "ShopFileConfig":
        cogshop_file_type = file_meta.metadata["shop:type"] if file_meta.metadata["shop:type"] != "case" else "model"
        return cls(
            path=Path(file_meta.metadata.get("shop:file_name") or cogshop_file_type),
            watercourse_name=file_meta.metadata["shop:watercourse"],
            cogshop_file_type=cogshop_file_type,
            md5_hash=file_meta.metadata.get("md5_hash"),
        )
