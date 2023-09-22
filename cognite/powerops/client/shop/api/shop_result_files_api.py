from __future__ import annotations

import contextlib
import logging
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Generic, NewType, Optional, TextIO, TypeVar, Union, cast

import yaml
from cognite.client import CogniteClient
from cognite.client.data_classes import FileMetadata

from cognite.powerops.client.shop.data_classes import ShopLogFile, ShopResultFile, ShopYamlFile
from cognite.powerops.utils.cdf.calls import retrieve_relationships_from_source_ext_id

logger = logging.getLogger(__name__)


ShopResultFileT = TypeVar("ShopResultFileT", bound=ShopResultFile)
EncodingT = NewType("EncodingT", str)


class ShopFilesAPI:
    def __init__(self, client: CogniteClient) -> None:
        self._client = client
        self.log_files = ShopLogFilesAPI(self)
        self.yaml_files = ShopYamlFilesAPI(self)

    def download_to_disk(self, shop_file_id: str, dir_path: Path) -> None:
        """
        Download a file from CDF to local filesystem.

        Args:
            shop_file_id: The external id of the file to download.
            dir_path: The location to download the file to.

        """
        self._client.files.download_to_path(path=dir_path / shop_file_id, external_id=shop_file_id)

    def retrieve_related_files_metadata(
        self, source_external_id: str, label_ext_id: Optional[Union[str, Sequence[str]]] = None
    ) -> Sequence[FileMetadata]:
        """
        Retrieve metadata of files that have a relationship to specified source (externalId).
        Optionally restrict the results by relationship labels.

        Args:
            source_external_id: The external id of the source.
            label_ext_id: The external id of the relationship label(s) to filter by.

        Returns:
            Sequence[FileMetadata]: The metadata of the related files.

        """
        if relationships := retrieve_relationships_from_source_ext_id(
            self._client, source_ext_id=source_external_id, label_ext_id=label_ext_id, target_types=["file"]
        ):
            return self._client.files.retrieve_multiple(
                external_ids=[rel.target_external_id for rel in relationships], ignore_unknown_ids=True
            )
        else:
            return []


class ShopResultFilesAPI(Generic[ShopResultFileT]):
    def __init__(self, shop_files: ShopFilesAPI) -> None:
        self._shop_files = shop_files

    def retrieve(self, file_metadata: FileMetadata) -> ShopResultFileT:
        raise NotImplementedError()

    def _parse_file(self, file: TextIO) -> Any:
        raise NotImplementedError()

    def _download(self, file_metadata: FileMetadata) -> tuple[Any, EncodingT]:
        """Download the file, parse it and return the content and encoding used to open the file."""
        encoding: EncodingT = file_metadata.metadata.get("encoding", "utf-8")
        with tempfile.TemporaryDirectory() as tmp_dir:
            self._shop_files.download_to_disk(file_metadata.external_id, Path(tmp_dir))
            tmp_path = Path(tmp_dir) / file_metadata.external_id
            content_and_encoding: Optional[tuple[Any, EncodingT]]
            try:
                with tmp_path.open(encoding=encoding) as downloaded_file:
                    content_and_encoding = self._parse_file(downloaded_file), encoding
            except UnicodeDecodeError:
                content_and_encoding = self._download_w_wrong_encoding(encoding, tmp_path)
                if content_and_encoding is None:
                    raise
            return content_and_encoding

    def _download_w_wrong_encoding(self, encoding: EncodingT, tmp_path: Path) -> Optional[tuple[Any, EncodingT]]:
        """
        Some files are uploaded to CDF with latin-1 encoding, most with utf-8.
        When utf-8 fails, we try latin-1.
        """
        if encoding != "latin1":
            encoding = cast(EncodingT, "latin-1")
            with tmp_path.open(encoding=encoding) as downloaded_file:
                with contextlib.suppress(ValueError, TypeError):
                    return self._parse_file(downloaded_file), encoding
        return None


class ShopLogFilesAPI(ShopResultFilesAPI[ShopLogFile]):
    def retrieve(self, file_metadata: FileMetadata) -> ShopLogFile:
        content, encoding = self._download(file_metadata)
        return ShopLogFile(content, file_metadata, encoding)

    def _parse_file(self, file: TextIO) -> str:
        return file.read()


class ShopYamlFilesAPI(ShopResultFilesAPI[ShopYamlFile]):
    def retrieve(self, file_metadata: FileMetadata) -> ShopYamlFile:
        content, encoding = self._download(file_metadata)
        return ShopYamlFile(content, file_metadata, encoding)

    def _parse_file(self, file: TextIO) -> dict:
        return yaml.safe_load(file)
