from __future__ import annotations

import abc
import logging
import os
from pathlib import Path
from typing import Generic, TypeVar, Union

import yaml
from cognite.client.data_classes import FileMetadata

logger = logging.getLogger(__name__)


FileContentTypeT = TypeVar("FileContentTypeT", bound=Union[str, dict])


class ShopResultFile(abc.ABC, Generic[FileContentTypeT]):
    """Base class for handling a results file from Shop."""

    def __init__(self, content: FileContentTypeT, file_metadata: FileMetadata = None, encoding="utf-8") -> None:
        self._file_metadata = file_metadata
        self._encoding = encoding
        super().__init__()
        self.data: FileContentTypeT = content

    @property
    def encoding(self) -> str:
        return self._encoding

    @property
    def external_id(self):
        return self._file_metadata.external_id

    @property
    def name(self):
        return self._file_metadata.name

    def save_to_disk(self, dir_path: str = "") -> str:
        """Save file to local filesystem."""
        file_path = os.path.join(dir_path or os.getcwd(), self._file_metadata.external_id)
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        with open(file_path, "w", encoding=self.encoding) as out_file:
            out_file.write(self.file_content)
        return file_path

    @property
    def file_content(self) -> str:
        """Data encoded for writing to local filesystem."""
        raise NotImplementedError()


class ShopLogFile(ShopResultFile[str]):
    """Plain text result file (for SHOP messages and CPlex logs)."""

    @property
    def file_content(self) -> str:
        return self.data


class ShopYamlFile(ShopResultFile[dict]):
    """Yaml-formatted results file (for post_run.yaml file created by SHOP)."""

    @property
    def file_content(self) -> str:
        return yaml.safe_dump(self.data, sort_keys=False)
