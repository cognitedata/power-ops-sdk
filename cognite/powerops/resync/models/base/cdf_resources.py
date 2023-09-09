from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from hashlib import sha256
from typing import Any, ClassVar, Optional

import pandas as pd
from cognite.client.data_classes import FileMetadata, Sequence
from cognite.client.data_classes._base import CogniteResource
from cognite.client.data_classes.data_modeling.ids import AbstractDataclass
from pydantic import BaseModel, ConfigDict, model_serializer, model_validator
from typing_extensions import Self

from cognite.powerops.utils.serialization import remove_read_only_fields


class CDFResource(BaseModel, ABC):
    content_key_hash: ClassVar[str] = "sha256_hash"
    model_config: ClassVar[ConfigDict] = ConfigDict(arbitrary_types_allowed=True)

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        return self.dump(camel_case=False)

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        return remove_read_only_fields(self._dump(camel_case))

    @abstractmethod
    def _dump(self, camel_case: bool = False) -> dict[str, Any]:
        raise NotImplementedError()

    @property
    def cdf_resource(self) -> CogniteResource:
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"{type(self).__name}({self.external_id=})"

    def __str__(self) -> str:
        return repr(self)


class CDFSequence(CDFResource):
    sequence: Sequence
    content: Optional[pd.DataFrame] = None

    @property
    def cdf_resource(self) -> Sequence:
        return self.sequence

    @model_validator(mode="before")
    def parse_dict(cls, value):
        if isinstance(value, dict) and "sequence" not in value:
            return {"sequence": Sequence._load(value)}
        return value

    @property
    def external_id(self):
        return self.sequence.external_id

    def dump(self, camel_case: bool = False) -> dict[str, Any]:
        output = super().dump(camel_case)
        if (columns := output.get("columns")) and isinstance(columns, list):
            for no, column in enumerate(columns):
                output["columns"][no] = remove_read_only_fields(column)
        return output

    @classmethod
    def calculate_hash(cls, content: pd.DataFrame) -> str:
        return sha256(pd.util.hash_pandas_object(content, index=True).values, usedforsecurity=False).hexdigest()

    def _dump(self, camel_case: bool = False) -> dict[str, Any]:
        if self.content is not None:
            sha256_hash = self.calculate_hash(self.content)
        elif self.sequence.metadata and self.sequence.metadata.get(self.content_key_hash) is not None:
            sha256_hash = self.sequence.metadata[self.content_key_hash]
        else:
            sha256_hash = "Missing Content"

        if self.sequence.metadata:
            self.sequence.metadata[self.content_key_hash] = sha256_hash
        else:
            self.sequence.metadata = {self.content_key_hash: sha256_hash}
        return self.sequence.dump(camel_case=camel_case)

    @classmethod
    def _load(cls, data: dict[str, Any]) -> Self:
        return cls(sequence=Sequence._load(data))


class CDFFile(CDFResource):
    meta: FileMetadata
    content: Optional[bytes] = None

    @property
    def cdf_resource(self) -> FileMetadata:
        return self.meta

    @model_validator(mode="before")
    def parse_dict(cls, value):
        if isinstance(value, dict) and "meta" not in value:
            return {"meta": FileMetadata._load(value)}
        return value

    @property
    def external_id(self):
        return self.meta.external_id

    @classmethod
    def calculate_hash(cls, content: bytes) -> str:
        # The replacement is used to ensure that the hash is the same on Windows and Linux
        return sha256(content.replace(b"\r\n", b"\n"), usedforsecurity=False).hexdigest()

    def _dump(self, camel_case: bool = False) -> dict[str, Any]:
        if self.content is None and (self.meta.metadata or {}).get(self.content_key_hash) is None:
            sha256_hash = "Missing Content"
        elif self.content is not None:
            sha256_hash = self.calculate_hash(self.content)
        else:
            sha256_hash = self.meta.metadata[self.content_key_hash]
        if self.meta.metadata:
            self.meta.metadata[self.content_key_hash] = sha256_hash
        else:
            self.meta.metadata = {self.content_key_hash: sha256_hash}
        return self.meta.dump(camel_case=camel_case)

    @classmethod
    def _load(cls, data: dict[str, Any]) -> Self:
        return cls(meta=FileMetadata._load(data))


# The extra class is because the AbstractDataClass does allow it
# subclasses to be instantiated. This is a private implementation detail in the Python-SDK, but it is useful
# for us here in the adapter methods of the CDF API.
@dataclass(frozen=True)
class _Dummy(AbstractDataclass):
    ...


@dataclass(frozen=True)
class SpaceId(_Dummy):
    space: str

    @property
    def external_id(self) -> str:
        return self.space

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {"space": self.space}
