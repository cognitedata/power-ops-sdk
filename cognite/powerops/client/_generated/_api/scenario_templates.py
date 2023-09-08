from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._constants import DEFAULT_LIMIT_READ

from cognite.powerops.client._generated._api._core import TypeAPI
from cognite.powerops.client._generated.data_classes import (
    ScenarioTemplate,
    ScenarioTemplateApply,
    ScenarioTemplateList,
)


class ScenarioTemplatesAPI(TypeAPI[ScenarioTemplate, ScenarioTemplateApply, ScenarioTemplateList]):
    def __init__(self, client: CogniteClient):
        super().__init__(
            client=client,
            sources=dm.ViewId("power-ops", "ScenarioTemplate", "77579c65a8cdf9"),
            class_type=ScenarioTemplate,
            class_apply_type=ScenarioTemplateApply,
            class_list=ScenarioTemplateList,
        )

    def apply(self, scenario_template: ScenarioTemplateApply, replace: bool = False) -> dm.InstancesApplyResult:
        instances = scenario_template.to_instances_apply()
        return self._client.data_modeling.instances.apply(nodes=instances.nodes, edges=instances.edges, replace=replace)

    def delete(self, external_id: str | Sequence[str]) -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(ScenarioTemplateApply.space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(ScenarioTemplateApply.space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> ScenarioTemplate:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ScenarioTemplateList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> ScenarioTemplate | ScenarioTemplateList:
        if isinstance(external_id, str):
            return self._retrieve((self.sources.space, external_id))
        else:
            return self._retrieve([(self.sources.space, ext_id) for ext_id in external_id])

    def list(self, limit: int = DEFAULT_LIMIT_READ) -> ScenarioTemplateList:
        return self._list(limit=limit)
