from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    BidDocumentDayAhead,
    BidDocumentDayAheadWrite,
    BidDocumentDayAheadFields,
    BidDocumentDayAheadList,
    BidDocumentDayAheadWriteList,
    BidDocumentDayAheadTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._bid_document_day_ahead import (
    _BIDDOCUMENTDAYAHEAD_PROPERTIES_BY_FIELD,
    _create_bid_document_day_ahead_filter,
)
from ._core import DEFAULT_LIMIT_READ, DEFAULT_QUERY_LIMIT, Aggregations, NodeAPI, SequenceNotStr, QueryStep, QueryBuilder
from .bid_document_day_ahead_alerts import BidDocumentDayAheadAlertsAPI
from .bid_document_day_ahead_partials import BidDocumentDayAheadPartialsAPI
from .bid_document_day_ahead_query import BidDocumentDayAheadQueryAPI


class BidDocumentDayAheadAPI(NodeAPI[BidDocumentDayAhead, BidDocumentDayAheadWrite, BidDocumentDayAheadList, BidDocumentDayAheadWriteList]):
    _view_id = dm.ViewId("power_ops_core", "BidDocumentDayAhead", "1")
    _properties_by_field = _BIDDOCUMENTDAYAHEAD_PROPERTIES_BY_FIELD
    _class_type = BidDocumentDayAhead
    _class_list = BidDocumentDayAheadList
    _class_write_list = BidDocumentDayAheadWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.alerts_edge = BidDocumentDayAheadAlertsAPI(client)
        self.partials_edge = BidDocumentDayAheadPartialsAPI(client)

    def __call__(
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
            bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
            total: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit: int = DEFAULT_QUERY_LIMIT,
            filter: dm.Filter | None = None,
    ) -> BidDocumentDayAheadQueryAPI[BidDocumentDayAheadList]:
        """Query starting at bid document day aheads.

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
            limit: Maximum number of bid document day aheads to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for bid document day aheads.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
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
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(BidDocumentDayAheadList)
        return BidDocumentDayAheadQueryAPI(self._client, builder, filter_, limit)


    def apply(
        self,
        bid_document_day_ahead: BidDocumentDayAheadWrite | Sequence[BidDocumentDayAheadWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) bid document day aheads.

        Note: This method iterates through all nodes and timeseries linked to bid_document_day_ahead and creates them including the edges
        between the nodes. For example, if any of `alerts` or `partials` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            bid_document_day_ahead: Bid document day ahead or sequence of bid document day aheads to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new bid_document_day_ahead:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import BidDocumentDayAheadWrite
                >>> client = PowerOpsModelsV1Client()
                >>> bid_document_day_ahead = BidDocumentDayAheadWrite(external_id="my_bid_document_day_ahead", ...)
                >>> result = client.bid_document_day_ahead.apply(bid_document_day_ahead)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.bid_document_day_ahead.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(bid_document_day_ahead, replace, write_none)

    def delete(self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> dm.InstancesDeleteResult:
        """Delete one or more bid document day ahead.

        Args:
            external_id: External id of the bid document day ahead to delete.
            space: The space where all the bid document day ahead are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete bid_document_day_ahead by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.bid_document_day_ahead.delete("my_bid_document_day_ahead")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.bid_document_day_ahead.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> BidDocumentDayAhead | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> BidDocumentDayAheadList:
        ...

    def retrieve(self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> BidDocumentDayAhead | BidDocumentDayAheadList | None:
        """Retrieve one or more bid document day aheads by id(s).

        Args:
            external_id: External id or list of external ids of the bid document day aheads.
            space: The space where all the bid document day aheads are located.

        Returns:
            The requested bid document day aheads.

        Examples:

            Retrieve bid_document_day_ahead by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_document_day_ahead = client.bid_document_day_ahead.retrieve("my_bid_document_day_ahead")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.alerts_edge,
                    "alerts",
                    dm.DirectRelationReference("power_ops_types", "calculationIssue"),
                    "outwards",
                    dm.ViewId("power_ops_core", "Alert", "1"),
                ),
                (
                    self.partials_edge,
                    "partials",
                    dm.DirectRelationReference("power_ops_types", "partialBid"),
                    "outwards",
                    dm.ViewId("power_ops_core", "PartialBidMatrixInformation", "1"),
                ),
                                               ]
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
        bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        total: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
            limit: Maximum number of bid document day aheads to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results bid document day aheads matching the query.

        Examples:

           Search for 'my_bid_document_day_ahead' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_document_day_aheads = client.bid_document_day_ahead.search('my_bid_document_day_ahead')

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
        bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        total: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.AggregatedNumberedValue:
        ...

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
        bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        total: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

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
        bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        total: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList:
        ...

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
        bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        total: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
            limit: Maximum number of bid document day aheads to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

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
        bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        total: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
            limit: Maximum number of bid document day aheads to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

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
        bid_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        total: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: BidDocumentDayAheadFields | Sequence[BidDocumentDayAheadFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_edges: bool = True,
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
            limit: Maximum number of bid document day aheads to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_edges: Whether to retrieve `alerts` or `partials` external ids for the bid document day aheads. Defaults to True.

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

        return self._list(
            limit=limit,
            filter=filter_,
            sort_by=sort_by,  # type: ignore[arg-type]
            direction=direction,
            sort=sort,
            retrieve_edges=retrieve_edges,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.alerts_edge,
                    "alerts",
                    dm.DirectRelationReference("power_ops_types", "calculationIssue"),
                    "outwards",
                    dm.ViewId("power_ops_core", "Alert", "1"),
                ),
                (
                    self.partials_edge,
                    "partials",
                    dm.DirectRelationReference("power_ops_types", "partialBid"),
                    "outwards",
                    dm.ViewId("power_ops_core", "PartialBidMatrixInformation", "1"),
                ),
                                               ]
        )
