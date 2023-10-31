from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.data_classes import (
    ScenarioTemplate,
    ScenarioTemplateApply,
    ScenarioTemplateApplyList,
    ScenarioTemplateFields,
    ScenarioTemplateList,
    ScenarioTemplateTextFields,
)
from cognite.powerops.client._generated.data_classes._scenario_template import _SCENARIOTEMPLATE_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, Aggregations, TypeAPI


class ScenarioTemplateAPI(TypeAPI[ScenarioTemplate, ScenarioTemplateApply, ScenarioTemplateList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=ScenarioTemplate,
            class_apply_type=ScenarioTemplateApply,
            class_list=ScenarioTemplateList,
        )
        self._view_id = view_id

    def apply(
        self, scenario_template: ScenarioTemplateApply | Sequence[ScenarioTemplateApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(scenario_template, ScenarioTemplateApply):
            instances = scenario_template.to_instances_apply()
        else:
            instances = ScenarioTemplateApplyList(scenario_template).to_instances_apply()
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space="power-ops") -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> ScenarioTemplate:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> ScenarioTemplateList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> ScenarioTemplate | ScenarioTemplateList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: ScenarioTemplateTextFields | Sequence[ScenarioTemplateTextFields] | None = None,
        watercourse: str | list[str] | None = None,
        watercourse_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        template_version: str | list[str] | None = None,
        template_version_prefix: str | None = None,
        base_mapping: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        output_definitions: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ScenarioTemplateList:
        filter_ = _create_filter(
            self._view_id,
            watercourse,
            watercourse_prefix,
            shop_version,
            shop_version_prefix,
            template_version,
            template_version_prefix,
            base_mapping,
            output_definitions,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _SCENARIOTEMPLATE_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ScenarioTemplateFields | Sequence[ScenarioTemplateFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: ScenarioTemplateTextFields | Sequence[ScenarioTemplateTextFields] | None = None,
        watercourse: str | list[str] | None = None,
        watercourse_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        template_version: str | list[str] | None = None,
        template_version_prefix: str | None = None,
        base_mapping: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        output_definitions: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ScenarioTemplateFields | Sequence[ScenarioTemplateFields] | None = None,
        group_by: ScenarioTemplateFields | Sequence[ScenarioTemplateFields] = None,
        query: str | None = None,
        search_properties: ScenarioTemplateTextFields | Sequence[ScenarioTemplateTextFields] | None = None,
        watercourse: str | list[str] | None = None,
        watercourse_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        template_version: str | list[str] | None = None,
        template_version_prefix: str | None = None,
        base_mapping: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        output_definitions: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList:
        ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ScenarioTemplateFields | Sequence[ScenarioTemplateFields] | None = None,
        group_by: ScenarioTemplateFields | Sequence[ScenarioTemplateFields] | None = None,
        query: str | None = None,
        search_property: ScenarioTemplateTextFields | Sequence[ScenarioTemplateTextFields] | None = None,
        watercourse: str | list[str] | None = None,
        watercourse_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        template_version: str | list[str] | None = None,
        template_version_prefix: str | None = None,
        base_mapping: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        output_definitions: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            watercourse,
            watercourse_prefix,
            shop_version,
            shop_version_prefix,
            template_version,
            template_version_prefix,
            base_mapping,
            output_definitions,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _SCENARIOTEMPLATE_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: ScenarioTemplateFields,
        interval: float,
        query: str | None = None,
        search_property: ScenarioTemplateTextFields | Sequence[ScenarioTemplateTextFields] | None = None,
        watercourse: str | list[str] | None = None,
        watercourse_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        template_version: str | list[str] | None = None,
        template_version_prefix: str | None = None,
        base_mapping: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        output_definitions: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            watercourse,
            watercourse_prefix,
            shop_version,
            shop_version_prefix,
            template_version,
            template_version_prefix,
            base_mapping,
            output_definitions,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _SCENARIOTEMPLATE_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        watercourse: str | list[str] | None = None,
        watercourse_prefix: str | None = None,
        shop_version: str | list[str] | None = None,
        shop_version_prefix: str | None = None,
        template_version: str | list[str] | None = None,
        template_version_prefix: str | None = None,
        base_mapping: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        output_definitions: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ScenarioTemplateList:
        filter_ = _create_filter(
            self._view_id,
            watercourse,
            watercourse_prefix,
            shop_version,
            shop_version_prefix,
            template_version,
            template_version_prefix,
            base_mapping,
            output_definitions,
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
    base_mapping: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    output_definitions: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
    if base_mapping and isinstance(base_mapping, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("baseMapping"), value={"space": "power-ops", "externalId": base_mapping}
            )
        )
    if base_mapping and isinstance(base_mapping, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("baseMapping"), value={"space": base_mapping[0], "externalId": base_mapping[1]}
            )
        )
    if base_mapping and isinstance(base_mapping, list) and isinstance(base_mapping[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("baseMapping"),
                values=[{"space": "power-ops", "externalId": item} for item in base_mapping],
            )
        )
    if base_mapping and isinstance(base_mapping, list) and isinstance(base_mapping[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("baseMapping"),
                values=[{"space": item[0], "externalId": item[1]} for item in base_mapping],
            )
        )
    if output_definitions and isinstance(output_definitions, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("outputDefinitions"),
                value={"space": "power-ops", "externalId": output_definitions},
            )
        )
    if output_definitions and isinstance(output_definitions, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("outputDefinitions"),
                value={"space": output_definitions[0], "externalId": output_definitions[1]},
            )
        )
    if output_definitions and isinstance(output_definitions, list) and isinstance(output_definitions[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("outputDefinitions"),
                values=[{"space": "power-ops", "externalId": item} for item in output_definitions],
            )
        )
    if output_definitions and isinstance(output_definitions, list) and isinstance(output_definitions[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("outputDefinitions"),
                values=[{"space": item[0], "externalId": item[1]} for item in output_definitions],
            )
        )
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
