from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Sequence, Union

from cognite.client.data_classes import FileMetadata

from cognite.powerops.utils.cdf_utils import retrieve_relationships_from_source_ext_id

if TYPE_CHECKING:
    from cognite.powerops import PowerOpsClient

logger = logging.getLogger(__name__)


class ShopFilesAPI:
    def __init__(self, po_client: PowerOpsClient) -> None:
        self._po_client = po_client

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

    def download_to_disk(self, shop_file_id: str, dir_path: Path) -> None:
        """Download a file from CDF to local filesystem."""
        self._po_client.cdf.files.download_to_path(path=dir_path / shop_file_id, external_id=shop_file_id)
