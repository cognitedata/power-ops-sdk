from __future__ import annotations

from typing import ClassVar, Union

import pandas as pd
from cognite.client.data_classes import FileMetadata, Sequence, SequenceData
from pydantic import BaseModel, ConfigDict


class CDFResource(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True)


class CDFSequence(CDFResource):
    sequence: Sequence
    content: Union[SequenceData, pd.DataFrame]

    @property
    def external_id(self):
        return self.sequence.external_id


class CDFFile(CDFResource):
    meta: FileMetadata
    content: bytes

    @property
    def external_id(self):
        return self.meta.external_id
