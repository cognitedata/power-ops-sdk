from __future__ import annotations

import datetime
import warnings
from collections.abc import Iterator, Sequence
from typing import Any, ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite.powerops.client._generated.v1._api._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_CHUNK_SIZE,
    instantiate_classes,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite.powerops.client._generated.v1.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    QueryBuildStepFactory,
    QueryBuilder,
    QueryExecutor,
    QueryUnpacker,
    ViewPropertyId,
)
from cognite.powerops.client._generated.v1.data_classes._bid_document_day_ahead import (
    BidDocumentDayAheadQuery,
    _BIDDOCUMENTDAYAHEAD_PROPERTIES_BY_FIELD,
    _create_bid_document_day_ahead_filter,
)
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    BidDocumentDayAhead,
    BidDocumentDayAheadWrite,
    BidDocumentDayAheadFields,
    BidDocumentDayAheadList,
    BidDocumentDayAheadWriteList,
    BidDocumentDayAheadTextFields,
    Alert,
    BidConfigurationDayAhead,
    BidMatrixInformation,
    PartialBidMatrixInformation,
)
from cognite.powerops.client._generated.v1._api.bid_document_day_ahead_alerts import BidDocumentDayAheadAlertsAPI
from cognite.powerops.client._generated.v1._api.bid_document_day_ahead_partials import BidDocumentDayAheadPartialsAPI


class BidDocumentDayAheadAPI(NodeAPI[BidDocumentDayAhead, BidDocumentDayAheadWrite, BidDocumentDayAheadList, BidDocumentDayAheadWriteList]):
    _view_id = dm.ViewId("power_ops_core", "BidDocumentDayAhead", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _BIDDOCUMENTDAYAHEAD_PROPERTIES_BY_FIELD
    _class_type = BidDocumentDayAhead
    _class_list = BidDocumentDayAheadList
    _class_write_list = BidDocumentDayAheadWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.alerts_edge = BidDocumentDayAheadAlertsAPI(client)
        self.partials_edge = BidDocumentDayAheadPartialsAPI(client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> BidDocumentDayAhead | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> BidDocumentDayAheadList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> BidDocumentDayAhead | BidDocumentDayAheadList | None:
        """Retrieve one or more bid document day aheads by id(s).

        Args:
            external_id: External id or list of external ids of the bid document day aheads.
            space: The space where all the bid document day aheads are located.
            retrieve_connections: Whether to retrieve `alerts`, `bid_configuration`, `total` and `partials` for the bid
            document day aheads. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only
            retrieve the identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            The requested bid document day aheads.

        Examples:

            Retrieve bid_document_day_ahead by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_document_day_ahead = client.bid_document_day_ahead.retrieve(
                ...     "my_bid_document_day_ahead"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_connections=retrieve_connections,
        )

    def search(
        self,
        query: str,
        properties: BidDocumentDayAheadTextFields | SequenceNotStr[BidDocumentDayAheadTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_delivery_date: datetime.date | None = None,
        max_delivery_date: datetime.date | None = None,
        min_start_calculation: datetime.datetime | None = None,
        max_start_calculation: datetime.datetime | None = None,
        min_end_calculation: datetime.datetime | None = None,
        max_end_calculation: datetime.datetime | None = None,
        is_complete: bool | None = None,
        bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        total: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: BidDocumentDayAheadFields | SequenceNotStr[BidDocumentDayAheadFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> BidDocumentDayAheadList:
        """Search bid document day aheads

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            workflow_execution_id: The workflow execution id to filter on.
            workflow_execution_id_prefix: The prefix of the workflow execution id to filter on.
            min_delivery_date: The minimum value of the delivery date to filter on.
            max_delivery_date: The maximum value of the delivery date to filter on.
            min_start_calculation: The minimum value of the start calculation to filter on.
            max_start_calculation: The maximum value of the start calculation to filter on.
            min_end_calculation: The minimum value of the end calculation to filter on.
            max_end_calculation: The maximum value of the end calculation to filter on.
            is_complete: The is complete to filter on.
            bid_configuration: The bid configuration to filter on.
            total: The total to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid document day aheads to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results bid document day aheads matching the query.

        Examples:

           Search for 'my_bid_document_day_ahead' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_document_day_aheads = client.bid_document_day_ahead.search(
                ...     'my_bid_document_day_ahead'
                ... )

        """
        filter_ = _create_bid_document_day_ahead_filter(
            self._view_id,
            name,
            name_prefix,
            workflow_execution_id,
            workflow_execution_id_prefix,
            min_delivery_date,
            max_delivery_date,
            min_start_calculation,
            max_start_calculation,
            min_end_calculation,
            max_end_calculation,
            is_complete,
            bid_configuration,
            total,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            query=query,
            properties=properties,
            filter_=filter_,
            limit=limit,
            sort_by=sort_by,  # type: ignore[arg-type]
            direction=direction,
            sort=sort,
        )

    @overload
    def aggregate(
        self,
        aggregate: Aggregations | dm.aggregations.MetricAggregation,
        group_by: None = None,
        property: BidDocumentDayAheadFields | SequenceNotStr[BidDocumentDayAheadFields] | None = None,
        query: str | None = None,
        search_property: BidDocumentDayAheadTextFields | SequenceNotStr[BidDocumentDayAheadTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_delivery_date: datetime.date | None = None,
        max_delivery_date: datetime.date | None = None,
        min_start_calculation: datetime.datetime | None = None,
        max_start_calculation: datetime.datetime | None = None,
        min_end_calculation: datetime.datetime | None = None,
        max_end_calculation: datetime.datetime | None = None,
        is_complete: bool | None = None,
        bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        total: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.AggregatedNumberedValue: ...

    @overload
    def aggregate(
        self,
        aggregate: SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: None = None,
        property: BidDocumentDayAheadFields | SequenceNotStr[BidDocumentDayAheadFields] | None = None,
        query: str | None = None,
        search_property: BidDocumentDayAheadTextFields | SequenceNotStr[BidDocumentDayAheadTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_delivery_date: datetime.date | None = None,
        max_delivery_date: datetime.date | None = None,
        min_start_calculation: datetime.datetime | None = None,
        max_start_calculation: datetime.datetime | None = None,
        min_end_calculation: datetime.datetime | None = None,
        max_end_calculation: datetime.datetime | None = None,
        is_complete: bool | None = None,
        bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        total: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]: ...

    @overload
    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: BidDocumentDayAheadFields | SequenceNotStr[BidDocumentDayAheadFields],
        property: BidDocumentDayAheadFields | SequenceNotStr[BidDocumentDayAheadFields] | None = None,
        query: str | None = None,
        search_property: BidDocumentDayAheadTextFields | SequenceNotStr[BidDocumentDayAheadTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_delivery_date: datetime.date | None = None,
        max_delivery_date: datetime.date | None = None,
        min_start_calculation: datetime.datetime | None = None,
        max_start_calculation: datetime.datetime | None = None,
        min_end_calculation: datetime.datetime | None = None,
        max_end_calculation: datetime.datetime | None = None,
        is_complete: bool | None = None,
        bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        total: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList: ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: BidDocumentDayAheadFields | SequenceNotStr[BidDocumentDayAheadFields] | None = None,
        property: BidDocumentDayAheadFields | SequenceNotStr[BidDocumentDayAheadFields] | None = None,
        query: str | None = None,
        search_property: BidDocumentDayAheadTextFields | SequenceNotStr[BidDocumentDayAheadTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_delivery_date: datetime.date | None = None,
        max_delivery_date: datetime.date | None = None,
        min_start_calculation: datetime.datetime | None = None,
        max_start_calculation: datetime.datetime | None = None,
        min_end_calculation: datetime.datetime | None = None,
        max_end_calculation: datetime.datetime | None = None,
        is_complete: bool | None = None,
        bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        total: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across bid document day aheads

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            workflow_execution_id: The workflow execution id to filter on.
            workflow_execution_id_prefix: The prefix of the workflow execution id to filter on.
            min_delivery_date: The minimum value of the delivery date to filter on.
            max_delivery_date: The maximum value of the delivery date to filter on.
            min_start_calculation: The minimum value of the start calculation to filter on.
            max_start_calculation: The maximum value of the start calculation to filter on.
            min_end_calculation: The minimum value of the end calculation to filter on.
            max_end_calculation: The maximum value of the end calculation to filter on.
            is_complete: The is complete to filter on.
            bid_configuration: The bid configuration to filter on.
            total: The total to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid document day aheads to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count bid document day aheads in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.bid_document_day_ahead.aggregate("count", space="my_space")

        """

        filter_ = _create_bid_document_day_ahead_filter(
            self._view_id,
            name,
            name_prefix,
            workflow_execution_id,
            workflow_execution_id_prefix,
            min_delivery_date,
            max_delivery_date,
            min_start_calculation,
            max_start_calculation,
            min_end_calculation,
            max_end_calculation,
            is_complete,
            bid_configuration,
            total,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            aggregate=aggregate,
            group_by=group_by,  # type: ignore[arg-type]
            properties=property,  # type: ignore[arg-type]
            query=query,
            search_properties=search_property,  # type: ignore[arg-type]
            limit=limit,
            filter=filter_,
        )

    def histogram(
        self,
        property: BidDocumentDayAheadFields,
        interval: float,
        query: str | None = None,
        search_property: BidDocumentDayAheadTextFields | SequenceNotStr[BidDocumentDayAheadTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_delivery_date: datetime.date | None = None,
        max_delivery_date: datetime.date | None = None,
        min_start_calculation: datetime.datetime | None = None,
        max_start_calculation: datetime.datetime | None = None,
        min_end_calculation: datetime.datetime | None = None,
        max_end_calculation: datetime.datetime | None = None,
        is_complete: bool | None = None,
        bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        total: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for bid document day aheads

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            workflow_execution_id: The workflow execution id to filter on.
            workflow_execution_id_prefix: The prefix of the workflow execution id to filter on.
            min_delivery_date: The minimum value of the delivery date to filter on.
            max_delivery_date: The maximum value of the delivery date to filter on.
            min_start_calculation: The minimum value of the start calculation to filter on.
            max_start_calculation: The maximum value of the start calculation to filter on.
            min_end_calculation: The minimum value of the end calculation to filter on.
            max_end_calculation: The maximum value of the end calculation to filter on.
            is_complete: The is complete to filter on.
            bid_configuration: The bid configuration to filter on.
            total: The total to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid document day aheads to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_bid_document_day_ahead_filter(
            self._view_id,
            name,
            name_prefix,
            workflow_execution_id,
            workflow_execution_id_prefix,
            min_delivery_date,
            max_delivery_date,
            min_start_calculation,
            max_start_calculation,
            min_end_calculation,
            max_end_calculation,
            is_complete,
            bid_configuration,
            total,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            property,
            interval,
            query,
            search_property,  # type: ignore[arg-type]
            limit,
            filter_,
        )

    def select(self) -> BidDocumentDayAheadQuery:
        """Start selecting from bid document day aheads."""
        return BidDocumentDayAheadQuery(self._client)

    def _build(
        self,
        filter_: dm.Filter | None,
        limit: int | None,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
        chunk_size: int | None = None,
    ) -> QueryExecutor:
        builder = QueryBuilder()
        factory = QueryBuildStepFactory(builder.create_name, view_id=self._view_id, edge_connection_property="end_node")
        builder.append(factory.root(
            filter=filter_,
            sort=sort,
            limit=limit,
            max_retrieve_batch_limit=chunk_size,
            has_container_fields=True,
        ))
        if retrieve_connections == "identifier" or retrieve_connections == "full":
            builder.extend(
                factory.from_edge(
                    Alert._view_id,
                    "outwards",
                    ViewPropertyId(self._view_id, "alerts"),
                    include_end_node=retrieve_connections == "full",
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_edge(
                    PartialBidMatrixInformation._view_id,
                    "outwards",
                    ViewPropertyId(self._view_id, "partials"),
                    include_end_node=retrieve_connections == "full",
                    has_container_fields=True,
                )
            )
        if retrieve_connections == "full":
            builder.extend(
                factory.from_direct_relation(
                    BidConfigurationDayAhead._view_id,
                    ViewPropertyId(self._view_id, "bidConfiguration"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    BidMatrixInformation._view_id,
                    ViewPropertyId(self._view_id, "total"),
                    has_container_fields=True,
                )
            )
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_delivery_date: datetime.date | None = None,
        max_delivery_date: datetime.date | None = None,
        min_start_calculation: datetime.datetime | None = None,
        max_start_calculation: datetime.datetime | None = None,
        min_end_calculation: datetime.datetime | None = None,
        max_end_calculation: datetime.datetime | None = None,
        is_complete: bool | None = None,
        bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        total: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[BidDocumentDayAheadList]:
        """Iterate over bid document day aheads

        Args:
            chunk_size: The number of bid document day aheads to return in each iteration. Defaults to 100.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            workflow_execution_id: The workflow execution id to filter on.
            workflow_execution_id_prefix: The prefix of the workflow execution id to filter on.
            min_delivery_date: The minimum value of the delivery date to filter on.
            max_delivery_date: The maximum value of the delivery date to filter on.
            min_start_calculation: The minimum value of the start calculation to filter on.
            max_start_calculation: The maximum value of the start calculation to filter on.
            min_end_calculation: The minimum value of the end calculation to filter on.
            max_end_calculation: The maximum value of the end calculation to filter on.
            is_complete: The is complete to filter on.
            bid_configuration: The bid configuration to filter on.
            total: The total to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `alerts`, `bid_configuration`, `total` and `partials` for the bid
            document day aheads. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only
            retrieve the identifier of the connected items, and 'full' will retrieve the full connected items.
            limit: Maximum number of bid document day aheads to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of bid document day aheads

        Examples:

            Iterate bid document day aheads in chunks of 100 up to 2000 items:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for bid_document_day_aheads in client.bid_document_day_ahead.iterate(chunk_size=100, limit=2000):
                ...     for bid_document_day_ahead in bid_document_day_aheads:
                ...         print(bid_document_day_ahead.external_id)

            Iterate bid document day aheads in chunks of 100 sorted by external_id in descending order:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for bid_document_day_aheads in client.bid_document_day_ahead.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for bid_document_day_ahead in bid_document_day_aheads:
                ...         print(bid_document_day_ahead.external_id)

            Iterate bid document day aheads in chunks of 100 and use cursors to resume the iteration:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for first_iteration in client.bid_document_day_ahead.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for bid_document_day_aheads in client.bid_document_day_ahead.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for bid_document_day_ahead in bid_document_day_aheads:
                ...         print(bid_document_day_ahead.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_bid_document_day_ahead_filter(
            self._view_id,
            name,
            name_prefix,
            workflow_execution_id,
            workflow_execution_id_prefix,
            min_delivery_date,
            max_delivery_date,
            min_start_calculation,
            max_start_calculation,
            min_end_calculation,
            max_end_calculation,
            is_complete,
            bid_configuration,
            total,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, retrieve_connections, cursors=cursors)

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        workflow_execution_id: str | list[str] | None = None,
        workflow_execution_id_prefix: str | None = None,
        min_delivery_date: datetime.date | None = None,
        max_delivery_date: datetime.date | None = None,
        min_start_calculation: datetime.datetime | None = None,
        max_start_calculation: datetime.datetime | None = None,
        min_end_calculation: datetime.datetime | None = None,
        max_end_calculation: datetime.datetime | None = None,
        is_complete: bool | None = None,
        bid_configuration: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        total: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: BidDocumentDayAheadFields | Sequence[BidDocumentDayAheadFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> BidDocumentDayAheadList:
        """List/filter bid document day aheads

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            workflow_execution_id: The workflow execution id to filter on.
            workflow_execution_id_prefix: The prefix of the workflow execution id to filter on.
            min_delivery_date: The minimum value of the delivery date to filter on.
            max_delivery_date: The maximum value of the delivery date to filter on.
            min_start_calculation: The minimum value of the start calculation to filter on.
            max_start_calculation: The maximum value of the start calculation to filter on.
            min_end_calculation: The minimum value of the end calculation to filter on.
            max_end_calculation: The maximum value of the end calculation to filter on.
            is_complete: The is complete to filter on.
            bid_configuration: The bid configuration to filter on.
            total: The total to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid document day aheads to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `alerts`, `bid_configuration`, `total` and `partials` for the bid
            document day aheads. Defaults to 'skip'.'skip' will not retrieve any connections, 'identifier' will only
            retrieve the identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested bid document day aheads

        Examples:

            List bid document day aheads and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_document_day_aheads = client.bid_document_day_ahead.list(limit=5)

        """
        filter_ = _create_bid_document_day_ahead_filter(
            self._view_id,
            name,
            name_prefix,
            workflow_execution_id,
            workflow_execution_id_prefix,
            min_delivery_date,
            max_delivery_date,
            min_start_calculation,
            max_start_calculation,
            min_end_calculation,
            max_end_calculation,
            is_complete,
            bid_configuration,
            total,
            external_id_prefix,
            space,
            filter,
        )
        sort_input =  self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        if retrieve_connections == "skip":
            return self._list(limit=limit,  filter=filter_, sort=sort_input)
        return self._query(filter_, limit, retrieve_connections, sort_input, "list")
