from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.data_classes import (
    ScenarioTemplate,
    ScenarioTemplateApply,
    ScenarioTemplateApplyList,
    ScenarioTemplateList,
)

from ._core import DEFAULT_LIMIT_READ, TypeAPI


class ScenarioTemplateAPI(TypeAPI[ScenarioTemplate, ScenarioTemplateApply, ScenarioTemplateList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=ScenarioTemplate,
            class_apply_type=ScenarioTemplateApply,
            class_list=ScenarioTemplateList,
        )
        self.view_id = view_id

    def apply(
        self, scenario_template: ScenarioTemplateApply | Sequence[ScenarioTemplateApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(scenario_template, ScenarioTemplateApply):
            instances = scenario_template.to_instances_apply()
        else:
            instances = ScenarioTemplateApplyList(scenario_template).to_instances_apply()
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

    def list(
        self,
        watercourse: str | list[str] | None = None,
        watercourse_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        template_version: str | list[str] | None = None,
        template_version_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ScenarioTemplateList:
        filter_ = _create_filter(
            self.view_id,
            watercourse,
            watercourse_prefix,
            shop_version,
            shop_version_prefix,
            template_version,
            template_version_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    watercourse: str | list[str] | None = None,
    watercourse_prefix: str | None = None,
    shop_version: str | list[str] | None = None,
    shop_version_prefix: str | None = None,
    template_version: str | list[str] | None = None,
    template_version_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if watercourse and isinstance(watercourse, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("watercourse"), value=watercourse))
    if watercourse and isinstance(watercourse, list):
        filters.append(dm.filters.In(view_id.as_property_ref("watercourse"), values=watercourse))
    if watercourse_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("watercourse"), value=watercourse_prefix))
    if shop_version and isinstance(shop_version, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopVersion"), value=shop_version))
    if shop_version and isinstance(shop_version, list):
        filters.append(dm.filters.In(view_id.as_property_ref("shopVersion"), values=shop_version))
    if shop_version_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("shopVersion"), value=shop_version_prefix))
    if template_version and isinstance(template_version, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("templateVersion"), value=template_version))
    if template_version and isinstance(template_version, list):
        filters.append(dm.filters.In(view_id.as_property_ref("templateVersion"), values=template_version))
    if template_version_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("templateVersion"), value=template_version_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
