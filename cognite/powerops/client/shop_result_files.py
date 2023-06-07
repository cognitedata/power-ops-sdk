from __future__ import annotations

import abc
import logging
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Generic, Optional, Sequence, TextIO, TypeVar, Union

import yaml
from cognite.client.data_classes import FileMetadata

from cognite.powerops.utils.cdf_utils import retrieve_relationships_from_source_ext_id
from cognite.powerops.utils.dotget import DotDict

if TYPE_CHECKING:
    from cognite.powerops import PowerOpsClient

logger = logging.getLogger(__name__)


FileContentTypeT = TypeVar("FileContentTypeT", bound=Union[str, dict])


class ShopResultFile(abc.ABC, Generic[FileContentTypeT]):
    """Base class for handling a results file from Shop."""

    def __init__(self, po_client: PowerOpsClient, file_metadata: FileMetadata = None, encoding="utf-8") -> None:
        self._po_client = po_client
        self._file_metadata = file_metadata
        self._encoding = encoding
        super().__init__()
        self.data: FileContentTypeT = self._download()

    @property
    def encoding(self) -> str:
        return self._encoding

    @property
    def external_id(self):
        return self._file_metadata.external_id

    @property
    def name(self):
        return self._file_metadata.name

    def _download(self) -> FileContentTypeT:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = self._po_client.shop.files.download(self, tmp_dir)
            with open(tmp_path, "r", encoding=self.encoding) as downloaded_file:
                return self._parse_file(downloaded_file)

    def _parse_file(self, file: TextIO) -> FileContentTypeT:
        """Read downloaded file and return data."""
        raise NotImplementedError()

    def save(self, dir_path: str = "") -> str:
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

    def _parse_file(self, file: TextIO) -> str:
        return file.read()

    @property
    def file_content(self) -> str:
        return self.data


class ShopYamlFile(ShopResultFile[dict], DotDict):
    """Yaml-formatted results file (for post_run.yaml file created by SHOP)."""

    def _parse_file(self, file: TextIO) -> dict:
        return yaml.safe_load(file)

    @property
    def file_content(self) -> str:
        return yaml.safe_dump(self.data, sort_keys=False)


class ShopFilesAPI:
    def __init__(self, po_client: PowerOpsClient) -> None:
        self._po_client = po_client

    def retrieve_related_meta(
        self, source_external_id: str, label_ext_id: Optional[Union[str, Sequence[str]]] = None
    ) -> Sequence[FileMetadata]:
        relationships = retrieve_relationships_from_source_ext_id(
            self._po_client.cdf,
            source_ext_id=source_external_id,
            label_ext_id=label_ext_id,
            target_types=["file"],
        )
        if not relationships:
            return []
        return self._po_client.cdf.files.retrieve_multiple(
            external_ids=[rel.target_external_id for rel in relationships],
            ignore_unknown_ids=True,
        )

    def retrieve(self, file_metadata: FileMetadata, shop_file_type: ShopResultFile) -> Optional[ShopResultFile]:
        try:
            shop_file = shop_file_type(self._po_client, file_metadata)  # TODO add encoding to metametadata!
        except Exception as exc:
            logger.error(f"Cannot retrieve result file: {file_metadata.external_id}\n{exc}")
            shop_file = None
        return shop_file

    def download(self, shop_file: ShopResultFile, dir_path: str) -> str:
        file_path = os.path.join(dir_path, shop_file.external_id)
        self._po_client.cdf.files.download_to_path(path=file_path, external_id=shop_file.external_id)
        return file_path
