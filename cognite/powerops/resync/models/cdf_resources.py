from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar, Optional

import pandas as pd
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


class CDFSequence(_CDFResource):
    sequence: Sequence
    content: pd.DataFrame

    @property
    def external_id(self):
        return self.sequence.external_id

    def _set_data_set_id(self, data_set_id: int):
        self.sequence.data_set_id = data_set_id

    data_set_id = property(None, fset=_set_data_set_id)

    def _dump(self, camel_case: bool = False) -> dict[str, Any]:
        return self.sequence.dump(camel_case=camel_case)


class CDFFile(_CDFResource):
    meta: FileMetadata
    content: Optional[bytes] = None

    @property
    def external_id(self):
        return self.meta.external_id

    def _dump(self, camel_case: bool = False) -> dict[str, Any]:
        return self.meta.dump(camel_case=camel_case)
