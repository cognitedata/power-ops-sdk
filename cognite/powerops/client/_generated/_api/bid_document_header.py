from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.data_classes import (
    BidDocumentHeader,
    BidDocumentHeaderApply,
    BidDocumentHeaderApplyList,
    BidDocumentHeaderFields,
    BidDocumentHeaderList,
    BidDocumentHeaderTextFields,
)
from cognite.powerops.client._generated.data_classes._bid_document_header import _BIDDOCUMENTHEADER_PROPERTIES_BY_FIELD

from ._core import DEFAULT_LIMIT_READ, Aggregations, TypeAPI


class BidDocumentHeaderAPI(TypeAPI[BidDocumentHeader, BidDocumentHeaderApply, BidDocumentHeaderList]):
    def __init__(self, client: CogniteClient, view_id: dm.ViewId):
        super().__init__(
            client=client,
            sources=view_id,
            class_type=BidDocumentHeader,
            class_apply_type=BidDocumentHeaderApply,
            class_list=BidDocumentHeaderList,
        )
        self._view_id = view_id

    def apply(
        self, bid_document_header: BidDocumentHeaderApply | Sequence[BidDocumentHeaderApply], replace: bool = False
    ) -> dm.InstancesApplyResult:
        if isinstance(bid_document_header, BidDocumentHeaderApply):
            instances = bid_document_header.to_instances_apply()
        else:
            instances = BidDocumentHeaderApplyList(bid_document_header).to_instances_apply()
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
    def retrieve(self, external_id: str) -> BidDocumentHeader:
        ...

    @overload
    def retrieve(self, external_id: Sequence[str]) -> BidDocumentHeaderList:
        ...

    def retrieve(self, external_id: str | Sequence[str]) -> BidDocumentHeader | BidDocumentHeaderList:
        if isinstance(external_id, str):
            return self._retrieve((self._sources.space, external_id))
        else:
            return self._retrieve([(self._sources.space, ext_id) for ext_id in external_id])

    def search(
        self,
        query: str,
        properties: BidDocumentHeaderTextFields | Sequence[BidDocumentHeaderTextFields] | None = None,
        document_type_name: str | list[str] | None = None,
        document_type_name_prefix: str | None = None,
        process_type_name: str | list[str] | None = None,
        process_type_name_prefix: str | None = None,
        tso: str | list[str] | None = None,
        tso_prefix: str | None = None,
        owner: str | list[str] | None = None,
        owner_prefix: str | None = None,
        min_created_date_time: datetime.datetime | None = None,
        max_created_date_time: datetime.datetime | None = None,
        bid_interval: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        country: str | list[str] | None = None,
        country_prefix: str | None = None,
        origin: str | list[str] | None = None,
        origin_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BidDocumentHeaderList:
        filter_ = _create_filter(
            self._view_id,
            document_type_name,
            document_type_name_prefix,
            process_type_name,
            process_type_name_prefix,
            tso,
            tso_prefix,
            owner,
            owner_prefix,
            min_created_date_time,
            max_created_date_time,
            bid_interval,
            country,
            country_prefix,
            origin,
            origin_prefix,
            external_id_prefix,
            filter,
        )
        return self._search(self._view_id, query, _BIDDOCUMENTHEADER_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: BidDocumentHeaderFields | Sequence[BidDocumentHeaderFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: BidDocumentHeaderTextFields | Sequence[BidDocumentHeaderTextFields] | None = None,
        document_type_name: str | list[str] | None = None,
        document_type_name_prefix: str | None = None,
        process_type_name: str | list[str] | None = None,
        process_type_name_prefix: str | None = None,
        tso: str | list[str] | None = None,
        tso_prefix: str | None = None,
        owner: str | list[str] | None = None,
        owner_prefix: str | None = None,
        min_created_date_time: datetime.datetime | None = None,
        max_created_date_time: datetime.datetime | None = None,
        bid_interval: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        country: str | list[str] | None = None,
        country_prefix: str | None = None,
        origin: str | list[str] | None = None,
        origin_prefix: str | None = None,
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
        property: BidDocumentHeaderFields | Sequence[BidDocumentHeaderFields] | None = None,
        group_by: BidDocumentHeaderFields | Sequence[BidDocumentHeaderFields] = None,
        query: str | None = None,
        search_properties: BidDocumentHeaderTextFields | Sequence[BidDocumentHeaderTextFields] | None = None,
        document_type_name: str | list[str] | None = None,
        document_type_name_prefix: str | None = None,
        process_type_name: str | list[str] | None = None,
        process_type_name_prefix: str | None = None,
        tso: str | list[str] | None = None,
        tso_prefix: str | None = None,
        owner: str | list[str] | None = None,
        owner_prefix: str | None = None,
        min_created_date_time: datetime.datetime | None = None,
        max_created_date_time: datetime.datetime | None = None,
        bid_interval: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        country: str | list[str] | None = None,
        country_prefix: str | None = None,
        origin: str | list[str] | None = None,
        origin_prefix: str | None = None,
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
        property: BidDocumentHeaderFields | Sequence[BidDocumentHeaderFields] | None = None,
        group_by: BidDocumentHeaderFields | Sequence[BidDocumentHeaderFields] | None = None,
        query: str | None = None,
        search_property: BidDocumentHeaderTextFields | Sequence[BidDocumentHeaderTextFields] | None = None,
        document_type_name: str | list[str] | None = None,
        document_type_name_prefix: str | None = None,
        process_type_name: str | list[str] | None = None,
        process_type_name_prefix: str | None = None,
        tso: str | list[str] | None = None,
        tso_prefix: str | None = None,
        owner: str | list[str] | None = None,
        owner_prefix: str | None = None,
        min_created_date_time: datetime.datetime | None = None,
        max_created_date_time: datetime.datetime | None = None,
        bid_interval: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        country: str | list[str] | None = None,
        country_prefix: str | None = None,
        origin: str | list[str] | None = None,
        origin_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        filter_ = _create_filter(
            self._view_id,
            document_type_name,
            document_type_name_prefix,
            process_type_name,
            process_type_name_prefix,
            tso,
            tso_prefix,
            owner,
            owner_prefix,
            min_created_date_time,
            max_created_date_time,
            bid_interval,
            country,
            country_prefix,
            origin,
            origin_prefix,
            external_id_prefix,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _BIDDOCUMENTHEADER_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: BidDocumentHeaderFields,
        interval: float,
        query: str | None = None,
        search_property: BidDocumentHeaderTextFields | Sequence[BidDocumentHeaderTextFields] | None = None,
        document_type_name: str | list[str] | None = None,
        document_type_name_prefix: str | None = None,
        process_type_name: str | list[str] | None = None,
        process_type_name_prefix: str | None = None,
        tso: str | list[str] | None = None,
        tso_prefix: str | None = None,
        owner: str | list[str] | None = None,
        owner_prefix: str | None = None,
        min_created_date_time: datetime.datetime | None = None,
        max_created_date_time: datetime.datetime | None = None,
        bid_interval: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        country: str | list[str] | None = None,
        country_prefix: str | None = None,
        origin: str | list[str] | None = None,
        origin_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        filter_ = _create_filter(
            self._view_id,
            document_type_name,
            document_type_name_prefix,
            process_type_name,
            process_type_name_prefix,
            tso,
            tso_prefix,
            owner,
            owner_prefix,
            min_created_date_time,
            max_created_date_time,
            bid_interval,
            country,
            country_prefix,
            origin,
            origin_prefix,
            external_id_prefix,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _BIDDOCUMENTHEADER_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        document_type_name: str | list[str] | None = None,
        document_type_name_prefix: str | None = None,
        process_type_name: str | list[str] | None = None,
        process_type_name_prefix: str | None = None,
        tso: str | list[str] | None = None,
        tso_prefix: str | None = None,
        owner: str | list[str] | None = None,
        owner_prefix: str | None = None,
        min_created_date_time: datetime.datetime | None = None,
        max_created_date_time: datetime.datetime | None = None,
        bid_interval: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        country: str | list[str] | None = None,
        country_prefix: str | None = None,
        origin: str | list[str] | None = None,
        origin_prefix: str | None = None,
        external_id_prefix: str | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BidDocumentHeaderList:
        filter_ = _create_filter(
            self._view_id,
            document_type_name,
            document_type_name_prefix,
            process_type_name,
            process_type_name_prefix,
            tso,
            tso_prefix,
            owner,
            owner_prefix,
            min_created_date_time,
            max_created_date_time,
            bid_interval,
            country,
            country_prefix,
            origin,
            origin_prefix,
            external_id_prefix,
            filter,
        )

        return self._list(limit=limit, filter=filter_)


def _create_filter(
    view_id: dm.ViewId,
    document_type_name: str | list[str] | None = None,
    document_type_name_prefix: str | None = None,
    process_type_name: str | list[str] | None = None,
    process_type_name_prefix: str | None = None,
    tso: str | list[str] | None = None,
    tso_prefix: str | None = None,
    owner: str | list[str] | None = None,
    owner_prefix: str | None = None,
    min_created_date_time: datetime.datetime | None = None,
    max_created_date_time: datetime.datetime | None = None,
    bid_interval: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
    country: str | list[str] | None = None,
    country_prefix: str | None = None,
    origin: str | list[str] | None = None,
    origin_prefix: str | None = None,
    external_id_prefix: str | None = None,
    filter: dm.Filter | None = None,
) -> dm.Filter | None:
    filters = []
    if document_type_name and isinstance(document_type_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("DocumentTypeName"), value=document_type_name))
    if document_type_name and isinstance(document_type_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("DocumentTypeName"), values=document_type_name))
    if document_type_name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("DocumentTypeName"), value=document_type_name_prefix))
    if process_type_name and isinstance(process_type_name, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("ProcessTypeName"), value=process_type_name))
    if process_type_name and isinstance(process_type_name, list):
        filters.append(dm.filters.In(view_id.as_property_ref("ProcessTypeName"), values=process_type_name))
    if process_type_name_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("ProcessTypeName"), value=process_type_name_prefix))
    if tso and isinstance(tso, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("TSO"), value=tso))
    if tso and isinstance(tso, list):
        filters.append(dm.filters.In(view_id.as_property_ref("TSO"), values=tso))
    if tso_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("TSO"), value=tso_prefix))
    if owner and isinstance(owner, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("Owner"), value=owner))
    if owner and isinstance(owner, list):
        filters.append(dm.filters.In(view_id.as_property_ref("Owner"), values=owner))
    if owner_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("Owner"), value=owner_prefix))
    if min_created_date_time or max_created_date_time:
        filters.append(
            dm.filters.Range(
                view_id.as_property_ref("CreatedDateTime"),
                gte=min_created_date_time.isoformat() if min_created_date_time else None,
                lte=max_created_date_time.isoformat() if max_created_date_time else None,
            )
        )
    if bid_interval and isinstance(bid_interval, str):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("BidInterval"), value={"space": "power-ops", "externalId": bid_interval}
            )
        )
    if bid_interval and isinstance(bid_interval, tuple):
        filters.append(
            dm.filters.Equals(
                view_id.as_property_ref("BidInterval"), value={"space": bid_interval[0], "externalId": bid_interval[1]}
            )
        )
    if bid_interval and isinstance(bid_interval, list) and isinstance(bid_interval[0], str):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("BidInterval"),
                values=[{"space": "power-ops", "externalId": item} for item in bid_interval],
            )
        )
    if bid_interval and isinstance(bid_interval, list) and isinstance(bid_interval[0], tuple):
        filters.append(
            dm.filters.In(
                view_id.as_property_ref("BidInterval"),
                values=[{"space": item[0], "externalId": item[1]} for item in bid_interval],
            )
        )
    if country and isinstance(country, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("Country"), value=country))
    if country and isinstance(country, list):
        filters.append(dm.filters.In(view_id.as_property_ref("Country"), values=country))
    if country_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("Country"), value=country_prefix))
    if origin and isinstance(origin, str):
        filters.append(dm.filters.Equals(view_id.as_property_ref("Origin"), value=origin))
    if origin and isinstance(origin, list):
        filters.append(dm.filters.In(view_id.as_property_ref("Origin"), values=origin))
    if origin_prefix:
        filters.append(dm.filters.Prefix(view_id.as_property_ref("Origin"), value=origin_prefix))
    if external_id_prefix:
        filters.append(dm.filters.Prefix(["node", "externalId"], value=external_id_prefix))
    if filter:
        filters.append(filter)
    return dm.filters.And(*filters) if filters else None
