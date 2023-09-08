from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from cognite.powerops.client._generated._api._core import TypeAPI
from cognite.powerops.client._generated.data_classes import Generator, GeneratorApply, GeneratorList


class GeneratorsAPI(TypeAPI[Generator, GeneratorApply, GeneratorList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "Generator", "9178931bbaac71"),
            class_type=Generator,
            class_apply_type=GeneratorApply,
            class_list=GeneratorList,
        )

    def apply(self, generator: GeneratorApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = generator.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(GeneratorApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(GeneratorApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Generator:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> GeneratorList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> Generator | GeneratorList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> GeneratorList:
        return self._list(limit=limit)
