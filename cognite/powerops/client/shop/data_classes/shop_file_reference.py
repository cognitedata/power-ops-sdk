from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from cognite.client.data_classes import FileMetadata
from typing_extensions import Self

from cognite.powerops.utils.identifiers import external_id_to_name

try:
    from enum import StrEnum
except ImportError:
    from strenum import StrEnum


class SHOPFileType(StrEnum):
    ASCII = "ascii"
    YAML = "yaml"


@dataclass
class SHOPFileReference:
    external_id: str
    file_type: SHOPFileType

    @property
    def name(self) -> str:
        return external_id_to_name(self.external_id)

    @classmethod
    def load(cls, data: dict[str, Any]) -> Self:
        return cls(external_id=data["external_id"], file_type=data["file_type"])

    def dump(self) -> dict[str, Any]:
        return {"external_id": self.external_id, "file_type": self.file_type}

    def as_cdf_file_metadata(self) -> FileMetadata:
        return FileMetadata(
            external_id=self.external_id,
            name=self.name,
            metadata={"shop:file_type": self.file_type},  # TODO metadata incomplete!
        )
