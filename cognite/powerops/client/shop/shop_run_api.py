from __future__ import annotations

from collections.abc import Sequence
from typing import ClassVar

from cognite.client import CogniteClient

from .shop_run import SHOPRun, SHOPRunList

DEFAULT_READ_LIMIT = 25


class SHOPRunAPI:
    _cdf_event_type: ClassVar[str] = "POWEROPS_PROCESS_REQUESTED"
    _cdf_event_subtype: ClassVar[str] = "POWEROPS_SHOP_RUN"

    def __init__(self, client: CogniteClient):
        self._cdf = client

    def trigger(self) -> SHOPRun:
        raise NotImplementedError()

    def list(self, watercourse: str | list[str] | None = None, limit: int = DEFAULT_READ_LIMIT) -> SHOPRunList:
        raise NotImplementedError()

    def retrieve(self, external_id: str | Sequence[str]) -> SHOPRun | SHOPRunList:
        raise NotImplementedError()
