from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.data_classes import (
    OutputMapping,
    OutputMappingApply,
    OutputMappingApplyList,
    OutputMappingFields,
    OutputMappingList,
    OutputMappingTextFields,
)
from cognite.powerops.client._generated.data_classes._output_mapping import _OUTPUTMAPPING_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, Aggregations, TypeAPI


class OutputMappingAPI(TypeAPI[OutputMapping, OutputMappingApply, OutputMappingList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=OutputMapping,
            class_apply_type=OutputMappingApply,
            class_list=OutputMappingList,
        )
        self._view_id = view_id

    def apply(
        self, output_mapping: OutputMappingApply | Sequence[OutputMappingApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(output_mapping, OutputMappingApply):
            instances = output_mapping.to_instances_apply()
        else:
            instances = OutputMappingApplyList(output_mapping).to_instances_apply()
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
    def retrieve(self, external_id: str) -> OutputMapping:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> OutputMappingList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> OutputMapping | OutputMappingList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: OutputMappingTextFields | Sequence[OutputMappingTextFields] | None = None,
        shop_object_type: str | list[str] | None = None,
        shop_object_type_prefix: str | None = None,
        shop_attribute_name: str | list[str] | None = None,
        shop_attribute_name_prefix: str | None = None,
        cdf_attribute_name: str | list[str] | None = None,
        cdf_attribute_name_prefix: str | None = None,
        unit: str | list[str] | None = None,
        unit_prefix: str | None = None,
        is_step: bool | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> OutputMappingList:
        filter_ = _create_filter(
            self._view_id,
            shop_object_type,
            shop_object_type_prefix,
            shop_attribute_name,
            shop_attribute_name_prefix,
            cdf_attribute_name,
            cdf_attribute_name_prefix,
            unit,
            unit_prefix,
            is_step,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _OUTPUTMAPPING_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: OutputMappingFields | Sequence[OutputMappingFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: OutputMappingTextFields | Sequence[OutputMappingTextFields] | None = None,
        shop_object_type: str | list[str] | None = None,
        shop_object_type_prefix: str | None = None,
        shop_attribute_name: str | list[str] | None = None,
        shop_attribute_name_prefix: str | None = None,
        cdf_attribute_name: str | list[str] | None = None,
        cdf_attribute_name_prefix: str | None = None,
        unit: str | list[str] | None = None,
        unit_prefix: str | None = None,
        is_step: bool | None = None,
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
        property: OutputMappingFields | Sequence[OutputMappingFields] | None = None,
        group_by: OutputMappingFields | Sequence[OutputMappingFields] = None,
        query: str | None = None,
        search_properties: OutputMappingTextFields | Sequence[OutputMappingTextFields] | None = None,
        shop_object_type: str | list[str] | None = None,
        shop_object_type_prefix: str | None = None,
        shop_attribute_name: str | list[str] | None = None,
        shop_attribute_name_prefix: str | None = None,
        cdf_attribute_name: str | list[str] | None = None,
        cdf_attribute_name_prefix: str | None = None,
        unit: str | list[str] | None = None,
        unit_prefix: str | None = None,
        is_step: bool | None = None,
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
        property: OutputMappingFields | Sequence[OutputMappingFields] | None = None,
        group_by: OutputMappingFields | Sequence[OutputMappingFields] | None = None,
        query: str | None = None,
        search_property: OutputMappingTextFields | Sequence[OutputMappingTextFields] | None = None,
        shop_object_type: str | list[str] | None = None,
        shop_object_type_prefix: str | None = None,
        shop_attribute_name: str | list[str] | None = None,
        shop_attribute_name_prefix: str | None = None,
        cdf_attribute_name: str | list[str] | None = None,
        cdf_attribute_name_prefix: str | None = None,
        unit: str | list[str] | None = None,
        unit_prefix: str | None = None,
        is_step: bool | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            shop_object_type,
            shop_object_type_prefix,
            shop_attribute_name,
            shop_attribute_name_prefix,
            cdf_attribute_name,
            cdf_attribute_name_prefix,
            unit,
            unit_prefix,
            is_step,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _OUTPUTMAPPING_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: OutputMappingFields,
        interval: float,
        query: str | None = None,
        search_property: OutputMappingTextFields | Sequence[OutputMappingTextFields] | None = None,
        shop_object_type: str | list[str] | None = None,
        shop_object_type_prefix: str | None = None,
        shop_attribute_name: str | list[str] | None = None,
        shop_attribute_name_prefix: str | None = None,
        cdf_attribute_name: str | list[str] | None = None,
        cdf_attribute_name_prefix: str | None = None,
        unit: str | list[str] | None = None,
        unit_prefix: str | None = None,
        is_step: bool | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            shop_object_type,
            shop_object_type_prefix,
            shop_attribute_name,
            shop_attribute_name_prefix,
            cdf_attribute_name,
            cdf_attribute_name_prefix,
            unit,
            unit_prefix,
            is_step,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _OUTPUTMAPPING_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        shop_object_type: str | list[str] | None = None,
        shop_object_type_prefix: str | None = None,
        shop_attribute_name: str | list[str] | None = None,
        shop_attribute_name_prefix: str | None = None,
        cdf_attribute_name: str | list[str] | None = None,
        cdf_attribute_name_prefix: str | None = None,
        unit: str | list[str] | None = None,
        unit_prefix: str | None = None,
        is_step: bool | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> OutputMappingList:
        filter_ = _create_filter(
            self._view_id,
            shop_object_type,
            shop_object_type_prefix,
            shop_attribute_name,
            shop_attribute_name_prefix,
            cdf_attribute_name,
            cdf_attribute_name_prefix,
            unit,
            unit_prefix,
            is_step,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    shop_object_type: str | list[str] | None = None,
    shop_object_type_prefix: str | None = None,
    shop_attribute_name: str | list[str] | None = None,
    shop_attribute_name_prefix: str | None = None,
    cdf_attribute_name: str | list[str] | None = None,
    cdf_attribute_name_prefix: str | None = None,
    unit: str | list[str] | None = None,
    unit_prefix: str | None = None,
    is_step: bool | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if shop_object_type and isinstance(shop_object_type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopObjectType"), value=shop_object_type))
    if shop_object_type and isinstance(shop_object_type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("shopObjectType"), values=shop_object_type))
    if shop_object_type_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("shopObjectType"), value=shop_object_type_prefix))
    if shop_attribute_name and isinstance(shop_attribute_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("shopAttributeName"), value=shop_attribute_name))
    if shop_attribute_name and isinstance(shop_attribute_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("shopAttributeName"), values=shop_attribute_name))
    if shop_attribute_name_prefix:
        filters.append(
            dm.filters.Prefix(view_id.as_property_ref("shopAttributeName"), value=shop_attribute_name_prefix)
        )
    if cdf_attribute_name and isinstance(cdf_attribute_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("cdfAttributeName"), value=cdf_attribute_name))
    if cdf_attribute_name and isinstance(cdf_attribute_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("cdfAttributeName"), values=cdf_attribute_name))
    if cdf_attribute_name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("cdfAttributeName"), value=cdf_attribute_name_prefix))
    if unit and isinstance(unit, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("unit"), value=unit))
    if unit and isinstance(unit, list):
        filters.append(dm.filters.In(view_id.as_property_ref("unit"), values=unit))
    if unit_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("unit"), value=unit_prefix))
    if is_step and isinstance(is_step, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("isStep"), value=is_step))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
