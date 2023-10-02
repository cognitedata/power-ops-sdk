from __future__ import annotations

from typing import Optional, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class Connection(BaseModel):
    connection_type: str
    from_: str = Field(alias="from")
    from_type: Optional[str] = None
    to: str
    to_type: Optional[str] = None

    def to_from_any(self, from_: dict[str, T], to_name: str, to_type: str) -> T | None:
        if self.to != to_name or self.to_type != to_type:
            return None
        f = from_.get(self.from_)
        if f:
            return f
        return None

    def from_to_any(self, from_name: str, from_type: str, to: dict[str, T]) -> T | None:
        if self.from_ != from_name or self.from_type != from_type:
            return None
        f = to.get(self.to)

        if f:
            return f
        return None
