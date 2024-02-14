from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.day_ahead_bid.data_classes._core import DEFAULT_INSTANCE_SPACE
from cognite.powerops.client._generated.day_ahead_bid.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    BidDocument,
    BidDocumentWrite,
    BidDocumentFields,
    BidDocumentList,
    BidDocumentWriteList,
    BidDocumentTextFields,
)
from cognite.powerops.client._generated.day_ahead_bid.data_classes._bid_document import (
    _BIDDOCUMENT_PROPERTIES_BY_FIELD,
    _create_bid_document_filter,
)
from ._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_QUERY_LIMIT,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
    QueryStep,
    QueryBuilder,
)
from .bid_document_alerts import BidDocumentAlertsAPI
from .bid_document_partials import BidDocumentPartialsAPI
from .bid_document_query import BidDocumentQueryAPI


class BidDocumentAPI(NodeAPI[BidDocument, BidDocumentWrite, BidDocumentList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[BidDocument]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=BidDocument,
            class_list=BidDocumentList,
            class_write_list=BidDocumentWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.alerts_edge = BidDocumentAlertsAPI(client)
        self.partials_edge = BidDocumentPartialsAPI(client)

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_delivery_date: datetime.date | None = None,
        max_delivery_date: datetime.date | None = None,
        min_start_calculation: datetime.datetime | None = None,
        max_start_calculation: datetime.datetime | None = None,
        min_end_calculation: datetime.datetime | None = None,
        max_end_calculation: datetime.datetime | None = None,
        is_complete: bool | None = None,
        price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        total: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> BidDocumentQueryAPI[BidDocumentList]:
        """Query starting at bid documents.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_delivery_date: The minimum value of the delivery date to filter on.
            max_delivery_date: The maximum value of the delivery date to filter on.
            min_start_calculation: The minimum value of the start calculation to filter on.
            max_start_calculation: The maximum value of the start calculation to filter on.
            min_end_calculation: The minimum value of the end calculation to filter on.
            max_end_calculation: The maximum value of the end calculation to filter on.
            is_complete: The is complete to filter on.
            price_area: The price area to filter on.
            method: The method to filter on.
            total: The total to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid documents to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for bid documents.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_bid_document_filter(
            self._view_id,
            name,
            name_prefix,
            min_delivery_date,
            max_delivery_date,
            min_start_calculation,
            max_start_calculation,
            min_end_calculation,
            max_end_calculation,
            is_complete,
            price_area,
            method,
            total,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(BidDocumentList)
        return BidDocumentQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        bid_document: BidDocumentWrite | Sequence[BidDocumentWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) bid documents.

        Note: This method iterates through all nodes and timeseries linked to bid_document and creates them including the edges
        between the nodes. For example, if any of `alerts` or `partials` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            bid_document: Bid document or sequence of bid documents to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new bid_document:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> from cognite.powerops.client._generated.day_ahead_bid.data_classes import BidDocumentWrite
                >>> client = DayAheadBidAPI()
                >>> bid_document = BidDocumentWrite(external_id="my_bid_document", ...)
                >>> result = client.bid_document.apply(bid_document)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.bid_document.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(bid_document, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more bid document.

        Args:
            external_id: External id of the bid document to delete.
            space: The space where all the bid document are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete bid_document by id:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> client.bid_document.delete("my_bid_document")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.bid_document.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> BidDocument | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> BidDocumentList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> BidDocument | BidDocumentList | None:
        """Retrieve one or more bid documents by id(s).

        Args:
            external_id: External id or list of external ids of the bid documents.
            space: The space where all the bid documents are located.

        Returns:
            The requested bid documents.

        Examples:

            Retrieve bid_document by id:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> bid_document = client.bid_document.retrieve("my_bid_document")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.alerts_edge,
                    "alerts",
                    dm.DirectRelationReference("power-ops-types", "calculationIssue"),
                    "outwards",
                    dm.ViewId("power-ops-shared", "Alert", "1"),
                ),
                (
                    self.partials_edge,
                    "partials",
                    dm.DirectRelationReference("power-ops-types", "partialBid"),
                    "outwards",
                    dm.ViewId("power-ops-day-ahead-bid", "BidMatrix", "1"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: BidDocumentTextFields | Sequence[BidDocumentTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_delivery_date: datetime.date | None = None,
        max_delivery_date: datetime.date | None = None,
        min_start_calculation: datetime.datetime | None = None,
        max_start_calculation: datetime.datetime | None = None,
        min_end_calculation: datetime.datetime | None = None,
        max_end_calculation: datetime.datetime | None = None,
        is_complete: bool | None = None,
        price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        total: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BidDocumentList:
        """Search bid documents

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_delivery_date: The minimum value of the delivery date to filter on.
            max_delivery_date: The maximum value of the delivery date to filter on.
            min_start_calculation: The minimum value of the start calculation to filter on.
            max_start_calculation: The maximum value of the start calculation to filter on.
            min_end_calculation: The minimum value of the end calculation to filter on.
            max_end_calculation: The maximum value of the end calculation to filter on.
            is_complete: The is complete to filter on.
            price_area: The price area to filter on.
            method: The method to filter on.
            total: The total to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid documents to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results bid documents matching the query.

        Examples:

           Search for 'my_bid_document' in all text properties:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> bid_documents = client.bid_document.search('my_bid_document')

        """
        filter_ = _create_bid_document_filter(
            self._view_id,
            name,
            name_prefix,
            min_delivery_date,
            max_delivery_date,
            min_start_calculation,
            max_start_calculation,
            min_end_calculation,
            max_end_calculation,
            is_complete,
            price_area,
            method,
            total,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _BIDDOCUMENT_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: BidDocumentFields | Sequence[BidDocumentFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: BidDocumentTextFields | Sequence[BidDocumentTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_delivery_date: datetime.date | None = None,
        max_delivery_date: datetime.date | None = None,
        min_start_calculation: datetime.datetime | None = None,
        max_start_calculation: datetime.datetime | None = None,
        min_end_calculation: datetime.datetime | None = None,
        max_end_calculation: datetime.datetime | None = None,
        is_complete: bool | None = None,
        price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        total: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
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
        property: BidDocumentFields | Sequence[BidDocumentFields] | None = None,
        group_by: BidDocumentFields | Sequence[BidDocumentFields] = None,
        query: str | None = None,
        search_properties: BidDocumentTextFields | Sequence[BidDocumentTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_delivery_date: datetime.date | None = None,
        max_delivery_date: datetime.date | None = None,
        min_start_calculation: datetime.datetime | None = None,
        max_start_calculation: datetime.datetime | None = None,
        min_end_calculation: datetime.datetime | None = None,
        max_end_calculation: datetime.datetime | None = None,
        is_complete: bool | None = None,
        price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        total: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
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
        property: BidDocumentFields | Sequence[BidDocumentFields] | None = None,
        group_by: BidDocumentFields | Sequence[BidDocumentFields] | None = None,
        query: str | None = None,
        search_property: BidDocumentTextFields | Sequence[BidDocumentTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_delivery_date: datetime.date | None = None,
        max_delivery_date: datetime.date | None = None,
        min_start_calculation: datetime.datetime | None = None,
        max_start_calculation: datetime.datetime | None = None,
        min_end_calculation: datetime.datetime | None = None,
        max_end_calculation: datetime.datetime | None = None,
        is_complete: bool | None = None,
        price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        total: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across bid documents

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_delivery_date: The minimum value of the delivery date to filter on.
            max_delivery_date: The maximum value of the delivery date to filter on.
            min_start_calculation: The minimum value of the start calculation to filter on.
            max_start_calculation: The maximum value of the start calculation to filter on.
            min_end_calculation: The minimum value of the end calculation to filter on.
            max_end_calculation: The maximum value of the end calculation to filter on.
            is_complete: The is complete to filter on.
            price_area: The price area to filter on.
            method: The method to filter on.
            total: The total to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid documents to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count bid documents in space `my_space`:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> result = client.bid_document.aggregate("count", space="my_space")

        """

        filter_ = _create_bid_document_filter(
            self._view_id,
            name,
            name_prefix,
            min_delivery_date,
            max_delivery_date,
            min_start_calculation,
            max_start_calculation,
            min_end_calculation,
            max_end_calculation,
            is_complete,
            price_area,
            method,
            total,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _BIDDOCUMENT_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: BidDocumentFields,
        interval: float,
        query: str | None = None,
        search_property: BidDocumentTextFields | Sequence[BidDocumentTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_delivery_date: datetime.date | None = None,
        max_delivery_date: datetime.date | None = None,
        min_start_calculation: datetime.datetime | None = None,
        max_start_calculation: datetime.datetime | None = None,
        min_end_calculation: datetime.datetime | None = None,
        max_end_calculation: datetime.datetime | None = None,
        is_complete: bool | None = None,
        price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        total: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for bid documents

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_delivery_date: The minimum value of the delivery date to filter on.
            max_delivery_date: The maximum value of the delivery date to filter on.
            min_start_calculation: The minimum value of the start calculation to filter on.
            max_start_calculation: The maximum value of the start calculation to filter on.
            min_end_calculation: The minimum value of the end calculation to filter on.
            max_end_calculation: The maximum value of the end calculation to filter on.
            is_complete: The is complete to filter on.
            price_area: The price area to filter on.
            method: The method to filter on.
            total: The total to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid documents to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_bid_document_filter(
            self._view_id,
            name,
            name_prefix,
            min_delivery_date,
            max_delivery_date,
            min_start_calculation,
            max_start_calculation,
            min_end_calculation,
            max_end_calculation,
            is_complete,
            price_area,
            method,
            total,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _BIDDOCUMENT_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_delivery_date: datetime.date | None = None,
        max_delivery_date: datetime.date | None = None,
        min_start_calculation: datetime.datetime | None = None,
        max_start_calculation: datetime.datetime | None = None,
        min_end_calculation: datetime.datetime | None = None,
        max_end_calculation: datetime.datetime | None = None,
        is_complete: bool | None = None,
        price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        total: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> BidDocumentList:
        """List/filter bid documents

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_delivery_date: The minimum value of the delivery date to filter on.
            max_delivery_date: The maximum value of the delivery date to filter on.
            min_start_calculation: The minimum value of the start calculation to filter on.
            max_start_calculation: The maximum value of the start calculation to filter on.
            min_end_calculation: The minimum value of the end calculation to filter on.
            max_end_calculation: The maximum value of the end calculation to filter on.
            is_complete: The is complete to filter on.
            price_area: The price area to filter on.
            method: The method to filter on.
            total: The total to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid documents to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `alerts` or `partials` external ids for the bid documents. Defaults to True.

        Returns:
            List of requested bid documents

        Examples:

            List bid documents and limit to 5:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> bid_documents = client.bid_document.list(limit=5)

        """
        filter_ = _create_bid_document_filter(
            self._view_id,
            name,
            name_prefix,
            min_delivery_date,
            max_delivery_date,
            min_start_calculation,
            max_start_calculation,
            min_end_calculation,
            max_end_calculation,
            is_complete,
            price_area,
            method,
            total,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            retrieve_edges=retrieve_edges,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.alerts_edge,
                    "alerts",
                    dm.DirectRelationReference("power-ops-types", "calculationIssue"),
                    "outwards",
                    dm.ViewId("power-ops-shared", "Alert", "1"),
                ),
                (
                    self.partials_edge,
                    "partials",
                    dm.DirectRelationReference("power-ops-types", "partialBid"),
                    "outwards",
                    dm.ViewId("power-ops-day-ahead-bid", "BidMatrix", "1"),
                ),
            ],
        )
