from __future__ import annotations

from pydantic import Field, field_validator
from typing_extensions import TypeAlias

from cognite.powerops.resync.models.base import Model, CDFFile

ExternalID: TypeAlias = str


class CogShopCore(Model):
    shop_files: list[CDFFile] = Field(default_factory=list)

    @field_validator("shop_files", mode="after")
    def ordering(cls, value: list[CDFFile]) -> list[CDFFile]:
        return sorted(value, key=lambda x: x.external_id)

    def standardize(self) -> None:
        self.shop_files = self.ordering(self.shop_files)
