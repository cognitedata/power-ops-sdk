from __future__ import annotations

from abc import ABC
from typing import ClassVar, Union

import pandas as pd
from cognite.client.data_classes import FileMetadata, Sequence, SequenceData
from pydantic import BaseModel, ConfigDict


class _CDFResource(BaseModel, ABC):
    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True)


class CDFSequence(_CDFResource):
    sequence: Sequence
    content: Union[SequenceData, pd.DataFrame]

    @property
    def external_id(self):
        return self.sequence.external_id

    def _set_data_set_id(self, data_set_id: int):
        self.sequence.data_set_id = data_set_id

    data_set_id = property(None, fset=_set_data_set_id)


class CDFFile(_CDFResource):
    meta: FileMetadata
    content: bytes

    @property
    def external_id(self):
        return self.meta.external_id
