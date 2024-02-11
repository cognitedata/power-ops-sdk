from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.cogshop1.data_classes import (
    Transformation,
    TransformationApply,
    TransformationApplyList,
    TransformationFields,
    TransformationList,
    TransformationTextFields,
)
from cognite.powerops.client._generated.cogshop1.data_classes._transformation import _TRANSFORMATION_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, Aggregations, TypeAPI


class TransformationAPI(TypeAPI[Transformation, TransformationApply, TransformationList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Transformation,
            class_apply_type=TransformationApply,
            class_list=TransformationList,
        )
        self._view_id = view_id

    def apply(
        self, transformation: TransformationApply | Sequence[TransformationApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(transformation, TransformationApply):
            instances = transformation.to_instances_apply()
        else:
            instances = TransformationApplyList(transformation).to_instances_apply()
        return self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )

    def delete(self, external_id: str | Sequence[str], space="cogShop") -> dm.InstancesDeleteResult:
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @overload
    def retrieve(self, external_id: str) -> Transformation: ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> TransformationList: ...

    def retrieve(self, external_id: str | Sequence[str]) -> Transformation | TransformationList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: TransformationTextFields | Sequence[TransformationTextFields] | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        arguments: str | list[str] | None = None,
        arguments_prefix: str | None = None,
        min_order: int | None = None,
        max_order: int | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> TransformationList:
        filter_ = _create_filter(
            self._view_id,
            method,
            method_prefix,
            arguments,
            arguments_prefix,
            min_order,
            max_order,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _TRANSFORMATION_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: TransformationFields | Sequence[TransformationFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: TransformationTextFields | Sequence[TransformationTextFields] | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        arguments: str | list[str] | None = None,
        arguments_prefix: str | None = None,
        min_order: int | None = None,
        max_order: int | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]: ...

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: TransformationFields | Sequence[TransformationFields] | None = None,
        group_by: TransformationFields | Sequence[TransformationFields] = None,
        query: str | None = None,
        search_properties: TransformationTextFields | Sequence[TransformationTextFields] | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        arguments: str | list[str] | None = None,
        arguments_prefix: str | None = None,
        min_order: int | None = None,
        max_order: int | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList: ...

    def aggregate(
        self,
        aggregate: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: TransformationFields | Sequence[TransformationFields] | None = None,
        group_by: TransformationFields | Sequence[TransformationFields] | None = None,
        query: str | None = None,
        search_property: TransformationTextFields | Sequence[TransformationTextFields] | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        arguments: str | list[str] | None = None,
        arguments_prefix: str | None = None,
        min_order: int | None = None,
        max_order: int | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            method,
            method_prefix,
            arguments,
            arguments_prefix,
            min_order,
            max_order,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _TRANSFORMATION_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: TransformationFields,
        interval: float,
        query: str | None = None,
        search_property: TransformationTextFields | Sequence[TransformationTextFields] | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        arguments: str | list[str] | None = None,
        arguments_prefix: str | None = None,
        min_order: int | None = None,
        max_order: int | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            method,
            method_prefix,
            arguments,
            arguments_prefix,
            min_order,
            max_order,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _TRANSFORMATION_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        arguments: str | list[str] | None = None,
        arguments_prefix: str | None = None,
        min_order: int | None = None,
        max_order: int | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> TransformationList:
        filter_ = _create_filter(
            self._view_id,
            method,
            method_prefix,
            arguments,
            arguments_prefix,
            min_order,
            max_order,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    method: str | list[str] | None = None,
    method_prefix: str | None = None,
    arguments: str | list[str] | None = None,
    arguments_prefix: str | None = None,
    min_order: int | None = None,
    max_order: int | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if method and isinstance(method, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("method"), value=method))
    if method and isinstance(method, list):
        filters.append(dm.filters.In(view_id.as_property_ref("method"), values=method))
    if method_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("method"), value=method_prefix))
    if arguments and isinstance(arguments, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("arguments"), value=arguments))
    if arguments and isinstance(arguments, list):
        filters.append(dm.filters.In(view_id.as_property_ref("arguments"), values=arguments))
    if arguments_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("arguments"), value=arguments_prefix))
    if min_order or max_order:
        filters.append(dm.filters.Range(view_id.as_property_ref("order"), gte=min_order, lte=max_order))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
