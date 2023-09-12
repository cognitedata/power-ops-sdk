from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client.data_classes import filters

from .shop_run import SHOPRun, ShopRunEvent, SHOPRunList

DEFAULT_READ_LIMIT = 25


class SHOPRunAPI:
    def __init__(self, client: CogniteClient):
        self._cdf = client

    def trigger(self) -> SHOPRun:
        raise NotImplementedError()

    def list(self, watercourse: str | list[str] | None = None, limit: int = DEFAULT_READ_LIMIT) -> SHOPRunList:
        """List the filtered SHOP runs.

        Args:
            watercourse: The watercourse to filter on.
            limit: The maximum number of SHOP runs to return. D

        Returns:
            A list of SHOP runs.
        """

        is_type = filters.Equals("type", ShopRunEvent.event_type)
        is_subtype = filters.Equals("subtype", ShopRunEvent.event_subtype)
        extra_filters = []
        if watercourse:
            watercourses = watercourse if isinstance(watercourse, list) else [watercourse]
            is_watercourse = filters.ContainsAny(["metadata", ShopRunEvent.watercourse], [watercourses])
            extra_filters.append(is_watercourse)

        selected = filters.And(is_type, is_subtype, *extra_filters)
        events = self._cdf.events.filter(selected, limit=limit)

        return SHOPRunList.load(events)

    @overload
    def retrieve(self, external_id: str) -> SHOPRun:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> SHOPRunList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> SHOPRun | SHOPRunList:
        raise NotImplementedError()
