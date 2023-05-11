from __future__ import annotations

from cognite.powerops.client.dm.schema import Transformation


class TransformationClient:
    def retrieve(self, external_id: str) -> Transformation:
        ...

    def update(self, external_id: str) -> Transformation:
        ...

    def delete(self, external_id: str) -> None:
        ...
