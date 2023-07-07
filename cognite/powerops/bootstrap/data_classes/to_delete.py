from __future__ import annotations

from typing import Union

import pandas as pd
from pydantic import BaseModel


class SequenceRows(BaseModel):
    rows: list[tuple[int, list[str]]]
    columns_external_ids: list[str]

    def to_pandas(self) -> pd.DataFrame:
        return pd.DataFrame(
            data=[row[1] for row in self.rows],
            index=[row[0] for row in self.rows],
            columns=self.columns_external_ids,
        )


class SequenceContent(BaseModel):
    sequence_external_id: str
    data: Union[pd.DataFrame, SequenceRows]

    class Config:
        arbitrary_types_allowed = True

    def dump(self) -> dict:
        return {
            self.sequence_external_id: self.data.to_dict()
            if isinstance(self.data, pd.DataFrame)
            else self.data.to_pandas().to_dict()
        }
