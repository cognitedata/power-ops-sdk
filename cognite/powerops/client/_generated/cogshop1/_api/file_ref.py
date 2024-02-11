from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.cogshop1.data_classes import (
    FileRef,
    FileRefApply,
    FileRefApplyList,
    FileRefFields,
    FileRefList,
    FileRefTextFields,
)
from cognite.powerops.client._generated.cogshop1.data_classes._file_ref import _FILEREF_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, Aggregations, TypeAPI


class FileRefAPI(TypeAPI[FileRef, FileRefApply, FileRefList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=FileRef,
            class_apply_type=FileRefApply,
            class_list=FileRefList,
        )
        self._view_id = view_id

    def apply(self, file_ref: FileRefApply | Sequence[FileRefApply], replace: bool = False) -> dm.InstancesApplyResult:
        if isinstance(file_ref, FileRefApply):
            instances = file_ref.to_instances_apply()
        else:
            instances = FileRefApplyList(file_ref).to_instances_apply()
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
    def retrieve(self, external_id: str) -> FileRef: ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> FileRefList: ...

    def retrieve(self, external_id: str | Sequence[str]) -> FileRef | FileRefList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: FileRefTextFields | Sequence[FileRefTextFields] | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        file_external_id: str | list[str] | None = None,
        file_external_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> FileRefList:
        filter_ = _create_filter(
            self._view_id,
            type,
            type_prefix,
            file_external_id,
            file_external_id_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _FILEREF_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: FileRefFields | Sequence[FileRefFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: FileRefTextFields | Sequence[FileRefTextFields] | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        file_external_id: str | list[str] | None = None,
        file_external_id_prefix: str | None = None,
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
        property: FileRefFields | Sequence[FileRefFields] | None = None,
        group_by: FileRefFields | Sequence[FileRefFields] = None,
        query: str | None = None,
        search_properties: FileRefTextFields | Sequence[FileRefTextFields] | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        file_external_id: str | list[str] | None = None,
        file_external_id_prefix: str | None = None,
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
        property: FileRefFields | Sequence[FileRefFields] | None = None,
        group_by: FileRefFields | Sequence[FileRefFields] | None = None,
        query: str | None = None,
        search_property: FileRefTextFields | Sequence[FileRefTextFields] | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        file_external_id: str | list[str] | None = None,
        file_external_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            type,
            type_prefix,
            file_external_id,
            file_external_id_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _FILEREF_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: FileRefFields,
        interval: float,
        query: str | None = None,
        search_property: FileRefTextFields | Sequence[FileRefTextFields] | None = None,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        file_external_id: str | list[str] | None = None,
        file_external_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            type,
            type_prefix,
            file_external_id,
            file_external_id_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _FILEREF_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        type: str | list[str] | None = None,
        type_prefix: str | None = None,
        file_external_id: str | list[str] | None = None,
        file_external_id_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> FileRefList:
        filter_ = _create_filter(
            self._view_id,
            type,
            type_prefix,
            file_external_id,
            file_external_id_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    type: str | list[str] | None = None,
    type_prefix: str | None = None,
    file_external_id: str | list[str] | None = None,
    file_external_id_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if type and isinstance(type, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("type"), value=type))
    if type and isinstance(type, list):
        filters.append(dm.filters.In(view_id.as_property_ref("type"), values=type))
    if type_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("type"), value=type_prefix))
    if file_external_id and isinstance(file_external_id, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("fileExternalId"), value=file_external_id))
    if file_external_id and isinstance(file_external_id, list):
        filters.append(dm.filters.In(view_id.as_property_ref("fileExternalId"), values=file_external_id))
    if file_external_id_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("fileExternalId"), value=file_external_id_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
