from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.data_classes import (
    DateTransformation,
    DateTransformationApply,
    DateTransformationApplyList,
    DateTransformationFields,
    DateTransformationList,
    DateTransformationTextFields,
)
from cognite.powerops.client._generated.data_classes._date_transformation import _DATETRANSFORMATION_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, Aggregations, TypeAPI


class DateTransformationAPI(TypeAPI[DateTransformation, DateTransformationApply, DateTransformationList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=DateTransformation,
            class_apply_type=DateTransformationApply,
            class_list=DateTransformationList,
        )
        self._view_id = view_id

    def apply(
        self, date_transformation: DateTransformationApply | Sequence[DateTransformationApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(date_transformation, DateTransformationApply):
            instances = date_transformation.to_instances_apply()
        else:
            instances = DateTransformationApplyList(date_transformation).to_instances_apply()
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
    def retrieve(self, external_id: str) -> DateTransformation:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> DateTransformationList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> DateTransformation | DateTransformationList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: DateTransformationTextFields | Sequence[DateTransformationTextFields] | None = None,
        transformation: str | list[str] | None = None,
        transformation_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> DateTransformationList:
        filter_ = _create_filter(
            self._view_id,
            transformation,
            transformation_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _DATETRANSFORMATION_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: DateTransformationFields | Sequence[DateTransformationFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: DateTransformationTextFields | Sequence[DateTransformationTextFields] | None = None,
        transformation: str | list[str] | None = None,
        transformation_prefix: str | None = None,
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
        property: DateTransformationFields | Sequence[DateTransformationFields] | None = None,
        group_by: DateTransformationFields | Sequence[DateTransformationFields] = None,
        query: str | None = None,
        search_properties: DateTransformationTextFields | Sequence[DateTransformationTextFields] | None = None,
        transformation: str | list[str] | None = None,
        transformation_prefix: str | None = None,
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
        property: DateTransformationFields | Sequence[DateTransformationFields] | None = None,
        group_by: DateTransformationFields | Sequence[DateTransformationFields] | None = None,
        query: str | None = None,
        search_property: DateTransformationTextFields | Sequence[DateTransformationTextFields] | None = None,
        transformation: str | list[str] | None = None,
        transformation_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            transformation,
            transformation_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _DATETRANSFORMATION_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: DateTransformationFields,
        interval: float,
        query: str | None = None,
        search_property: DateTransformationTextFields | Sequence[DateTransformationTextFields] | None = None,
        transformation: str | list[str] | None = None,
        transformation_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            transformation,
            transformation_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _DATETRANSFORMATION_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        transformation: str | list[str] | None = None,
        transformation_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> DateTransformationList:
        filter_ = _create_filter(
            self._view_id,
            transformation,
            transformation_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    transformation: str | list[str] | None = None,
    transformation_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if transformation and isinstance(transformation, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("transformation"), value=transformation))
    if transformation and isinstance(transformation, list):
        filters.append(dm.filters.In(view_id.as_property_ref("transformation"), values=transformation))
    if transformation_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("transformation"), value=transformation_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
