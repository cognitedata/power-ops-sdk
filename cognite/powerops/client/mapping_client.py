from __future__ import annotations

from cognite.powerops.client.dm.schema import Mapping


class MappingClient:
    def retrieve(self, external_id: str) -> Mapping:  # TODO or TimeSeriesMappingEntry?
        ...

    def update(self, external_id: str) -> Mapping:  # TODO or TimeSeriesMappingEntry?
        ...

    def delete(self, external_id: str) -> None:
        ...

    def visualize(self, mapping: Mapping):
        """Visualize a timeseries with (optional) transformations"""
