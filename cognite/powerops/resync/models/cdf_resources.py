from __future__ import annotations

from abc import ABC, abstractclassmethod, abstractmethod
from typing import Any, ClassVar, Optional

import pandas as pd
from cognite.client import CogniteClient
from cognite.client.data_classes import FileMetadata, Sequence
from pydantic import BaseModel, ConfigDict

_READ_ONLY_FIELDS = [
    "created_time",
    "last_updated_time",
    "lastUpdatedTime",
    "createdTime",
    "uploaded_time",
    "uploadedTime",
]


class _CDFResource(BaseModel, ABC):
    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True)

    @abstractmethod
    def _dump(self, camel_case: bool = False) -> dict[str, Any]:
        ...

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        dump = self._dump(camel_case)
        for read_only_field in _READ_ONLY_FIELDS:
            dump.pop(read_only_field, None)
        return dump

    @abstractclassmethod
    def from_cdf(
        cls,
        client: CogniteClient,
        resource_ext_id: str,
        fetch_content: bool = False,
    ) -> _CDFResource:
        ...


class CDFSequence(_CDFResource):
    sequence: Sequence
    content: Optional[pd.DataFrame]

    def __repr__(self) -> str:
        return f"CDFSequence(external_id={self.external_id})"

    def __str__(self) -> str:
        return self.__repr__()

    @property
    def external_id(self):
        return self.sequence.external_id

    def _set_data_set_id(self, data_set_id: int):
        self.sequence.data_set_id = data_set_id

    data_set_id = property(None, fset=_set_data_set_id)

    def _dump(self, camel_case: bool = False) -> dict[str, Any]:
        return self.sequence.dump(camel_case=camel_case)

    @classmethod
    def from_cdf(
        cls,
        client: CogniteClient,
        resource_ext_id: str,
        fetch_content: bool = False,
    ) -> CDFSequence:
        sequence = client.sequences.retrieve(external_id=resource_ext_id)
        if fetch_content:
            # limit defaults to 100, might not be an issue
            content = client.sequences.data.retrieve_dataframe(
                external_id=resource_ext_id,
                start=0,
                end=None,
            )
        else:
            content = None
        return cls(sequence=sequence, content=content)


class CDFFile(_CDFResource):
    meta: FileMetadata
    content: Optional[bytes] = None

    def __repr__(self) -> str:
        return f"CDFFile(external_id={self.external_id})"

    def __str__(self) -> str:
        return self.__repr__()

    @property
    def external_id(self):
        return self.meta.external_id

    def _dump(self, camel_case: bool = False) -> dict[str, Any]:
        return self.meta.dump(camel_case=camel_case)

    @classmethod
    def from_cdf(
        cls,
        client: CogniteClient,
        resource_ext_id: str,
        fetch_content: bool = False,
    ) -> CDFFile:
        meta = client.files.retrieve(external_id=resource_ext_id)
        if fetch_content:
            content = client.files.download_bytes(external_id=resource_ext_id)
        else:
            content = None
        return cls(meta=meta, content=content)
