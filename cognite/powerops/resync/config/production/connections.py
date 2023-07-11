from __future__ import annotations

from pydantic import BaseModel, Field

from cognite.powerops.resync.models._base import AssetType


class Connection(BaseModel):
    connection_type: str
    from_: str = Field(alias="from")
    from_type: str
    to: str
    to_type: str

    def to_from_any(self, from_: dict[str, AssetType], to: AssetType) -> AssetType | None:
        if self.to != to.name or self.to_type != to.type_:
            return None
        f = from_.get(self.from_)
        if f and self.from_type == f.type_:
            return f
        return None

    def from_to_any(self, from_: AssetType, to: dict[str, AssetType]) -> AssetType | None:
        if self.from_ != from_.name or self.from_type != from_.type_:
            return None
        f = to.get(self.to)

        if f and self.to_type == f.type_:
            return f
        return None
