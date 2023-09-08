from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from cognite.powerops.client._generated._api._core import TypeAPI
from cognite.powerops.client._generated.data_classes import (
    BidMatrixGenerator,
    BidMatrixGeneratorApply,
    BidMatrixGeneratorList,
)


class BidMatrixGeneratorsAPI(TypeAPI[BidMatrixGenerator, BidMatrixGeneratorApply, BidMatrixGeneratorList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "BidMatrixGenerator", "98145498689f24"),
            class_type=BidMatrixGenerator,
            class_apply_type=BidMatrixGeneratorApply,
            class_list=BidMatrixGeneratorList,
        )

    def apply(self, bid_matrix_generator: BidMatrixGeneratorApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = bid_matrix_generator.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(BidMatrixGeneratorApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(BidMatrixGeneratorApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> BidMatrixGenerator:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> BidMatrixGeneratorList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> BidMatrixGenerator | BidMatrixGeneratorList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> BidMatrixGeneratorList:
        return self._list(limit=limit)
