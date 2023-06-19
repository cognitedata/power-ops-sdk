from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, NewType, Optional, Sequence, TextIO, Tuple, TypeVar, Union, cast

import yaml
from cognite.client.data_classes import FileMetadata

from cognite.powerops.client.data_classes import ShopLogFile, ShopResultFile, ShopYamlFile
from cognite.powerops.utils.cdf_utils import retrieve_relationships_from_source_ext_id

if TYPE_CHECKING:
    from cognite.powerops import PowerOpsClient

logger = logging.getLogger(__name__)


ShopResultFileT = TypeVar("ShopResultFileT", bound=ShopResultFile)
EncodingT = NewType("EncodingT", str)


class ShopFilesAPI:
    def __init__(self, po_client: PowerOpsClient) -> None:
        self._po_client = po_client
        self.log_files = ShopLogFilesAPI(po_client)
        self.yaml_files = ShopYamlFilesAPI(po_client)

    def download_to_disk(self, shop_file_id: str, dir_path: Path) -> None:
        """Download a file from CDF to local filesystem."""
        self._po_client.cdf.files.download_to_path(path=dir_path / shop_file_id, external_id=shop_file_id)

    def retrieve_related_files_metadata(
        self, source_external_id: str, label_ext_id: Optional[Union[str, Sequence[str]]] = None
    ) -> Sequence[FileMetadata]:
        """
        Retrieve metadata of files that have a relationship to specified source (externalId).
        Optionally restrict the results by relationship labels.
        """
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


class ShopResultFilesAPI(Generic[ShopResultFileT]):
    def __init__(self, po_client: PowerOpsClient) -> None:
        self._po_client = po_client

    def retrieve(self, file_metadata: FileMetadata) -> ShopResultFileT:
        raise NotImplementedError()

    def _parse_file(self, file: TextIO) -> Any:
        raise NotImplementedError()

    def _download(self, file_metadata: FileMetadata) -> Tuple[Any, EncodingT]:
        """Download the file, parse it and return the content and encoding used to open the file."""
        encoding: EncodingT = file_metadata.metadata.get("encoding", "utf-8")
        with tempfile.TemporaryDirectory() as tmp_dir:
            self._po_client.shop.files.download_to_disk(file_metadata.external_id, Path(tmp_dir))
            tmp_path = Path(tmp_dir) / file_metadata.external_id
            content_and_encoding: Optional[Tuple[Any, EncodingT]]
            try:
                with open(tmp_path, "r", encoding=encoding) as downloaded_file:
                    content_and_encoding = self._parse_file(downloaded_file), encoding
            except UnicodeDecodeError:
                content_and_encoding = self._download_w_wrong_encoding(encoding, tmp_path)
                if content_and_encoding is None:
                    raise
            return content_and_encoding

    def _download_w_wrong_encoding(self, encoding: EncodingT, tmp_path: Path) -> Optional[Tuple[Any, EncodingT]]:
        """
        Some files are uploaded to CDF with latin-1 encoding, most with utf-8.
        When utf-8 fails, we try latin-1.
        """
        if encoding != "latin1":
            encoding = cast(EncodingT, "latin-1")
            with open(tmp_path, "r", encoding=encoding) as downloaded_file:
                try:
                    return self._parse_file(downloaded_file), encoding
                except (ValueError, TypeError):
                    pass
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
