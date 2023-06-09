from __future__ import annotations

from typing import Sequence, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import INSTANCES_LIST_LIMIT_DEFAULT

from cognite.powerops.clients.cogshop.data_classes import Transformation, TransformationApply, TransformationList

from ._core import TypeAPI


class TransformationsAPI(TypeAPI[Transformation, TransformationApply, TransformationList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("cogShop", "Transformation", "e18830dc469f30"),
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

    def list(self, limit: int = INSTANCES_LIST_LIMIT_DEFAULT) -> TransformationList:
        return self._list(limit=limit)
