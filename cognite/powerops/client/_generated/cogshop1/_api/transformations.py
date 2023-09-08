from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from cognite.powerops.client._generated.cogshop1._api._core import TypeAPI
from cognite.powerops.client._generated.cogshop1.data_classes import (
    Transformation,
    TransformationApply,
    TransformationList,
)


class TransformationsAPI(TypeAPI[Transformation, TransformationApply, TransformationList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("cogShop", "Transformation", "15ce1f14efe2dc"),
            class_type=Transformation,
            class_apply_type=TransformationApply,
            class_list=TransformationList,
        )

    def apply(self, transformation: TransformationApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = transformation.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(TransformationApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(TransformationApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Transformation:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> TransformationList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Transformation | TransformationList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> TransformationList:
        return self._list(limit=limit)
