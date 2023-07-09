from __future__ import annotations

import pandas as pd
from pydantic import BaseModel


class SequenceContent(BaseModel):
    sequence_external_id: str
    data: pd.DataFrame

    class Config:
        arbitrary_types_allowed = True

    def dump(self) -> dict:
        return {self.sequence_external_id: self.data.to_dict()}
