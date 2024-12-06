from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite.powerops.client._generated.v1.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    NodeQueryStep,
    EdgeQueryStep,
    DataClassQueryBuilder,
)
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    BidDocumentAFRR,
    BidDocumentAFRRWrite,
    BidDocumentAFRRFields,
    BidDocumentAFRRList,
    BidDocumentAFRRWriteList,
    BidDocumentAFRRTextFields,
    Alert,
    BidRow,
    PriceAreaAFRR,
)
from cognite.powerops.client._generated.v1.data_classes._bid_document_afrr import (
    BidDocumentAFRRQuery,
    _BIDDOCUMENTAFRR_PROPERTIES_BY_FIELD,
    _create_bid_document_afrr_filter,
)
from cognite.powerops.client._generated.v1._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite.powerops.client._generated.v1._api.bid_document_afrr_alerts import BidDocumentAFRRAlertsAPI
from cognite.powerops.client._generated.v1._api.bid_document_afrr_bids import BidDocumentAFRRBidsAPI
from cognite.powerops.client._generated.v1._api.bid_document_afrr_query import BidDocumentAFRRQueryAPI


class BidDocumentAFRRAPI(NodeAPI[BidDocumentAFRR, BidDocumentAFRRWrite, BidDocumentAFRRList, BidDocumentAFRRWriteList]):
    _view_id = dm.ViewId("power_ops_core", "BidDocumentAFRR", "1")
    _properties_by_field = _BIDDOCUMENTAFRR_PROPERTIES_BY_FIELD
    _class_type = BidDocumentAFRR
    _class_list = BidDocumentAFRRList
    _class_write_list = BidDocumentAFRRWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.alerts_edge = BidDocumentAFRRAlertsAPI(client)
        self.bids_edge = BidDocumentAFRRBidsAPI(client)

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
        price_area: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> BidDocumentAFRRQueryAPI[BidDocumentAFRRList]:
        """Query starting at bid document afrrs.

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
            price_area: The price area to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid document afrrs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for bid document afrrs.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. "
            "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_bid_document_afrr_filter(
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
            price_area,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = DataClassQueryBuilder(BidDocumentAFRRList)
        return BidDocumentAFRRQueryAPI(self._client, builder, filter_, limit)

    def apply(
        self,
        bid_document_afrr: BidDocumentAFRRWrite | Sequence[BidDocumentAFRRWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) bid document afrrs.

        Note: This method iterates through all nodes and timeseries linked to bid_document_afrr and creates them including the edges
        between the nodes. For example, if any of `alerts`, `price_area` or `bids` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            bid_document_afrr: Bid document afrr or sequence of bid document afrrs to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new bid_document_afrr:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import BidDocumentAFRRWrite
                >>> client = PowerOpsModelsV1Client()
                >>> bid_document_afrr = BidDocumentAFRRWrite(external_id="my_bid_document_afrr", ...)
                >>> result = client.bid_document_afrr.apply(bid_document_afrr)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.bid_document_afrr.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(bid_document_afrr, replace, write_none)

    def delete(self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> dm.InstancesDeleteResult:
        """Delete one or more bid document afrr.

        Args:
            external_id: External id of the bid document afrr to delete.
            space: The space where all the bid document afrr are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete bid_document_afrr by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.bid_document_afrr.delete("my_bid_document_afrr")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.bid_document_afrr.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str | dm.NodeId | tuple[str, str], space: str = DEFAULT_INSTANCE_SPACE) -> BidDocumentAFRR | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE) -> BidDocumentAFRRList:
        ...

    def retrieve(self, external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE) -> BidDocumentAFRR | BidDocumentAFRRList | None:
        """Retrieve one or more bid document afrrs by id(s).

        Args:
            external_id: External id or list of external ids of the bid document afrrs.
            space: The space where all the bid document afrrs are located.

        Returns:
            The requested bid document afrrs.

        Examples:

            Retrieve bid_document_afrr by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_document_afrr = client.bid_document_afrr.retrieve("my_bid_document_afrr")

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
                    self.bids_edge,
                    "bids",
                    dm.DirectRelationReference("power_ops_types", "partialBid"),
                    "outwards",
                    dm.ViewId("power_ops_core", "BidRow", "1"),
                ),
                                               ]
        )

    def search(
        self,
        query: str,
        properties: BidDocumentAFRRTextFields | SequenceNotStr[BidDocumentAFRRTextFields] | None = None,
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
        price_area: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: BidDocumentAFRRFields | SequenceNotStr[BidDocumentAFRRFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> BidDocumentAFRRList:
        """Search bid document afrrs

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
            price_area: The price area to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid document afrrs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results bid document afrrs matching the query.

        Examples:

           Search for 'my_bid_document_afrr' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_document_afrrs = client.bid_document_afrr.search('my_bid_document_afrr')

        """
        filter_ = _create_bid_document_afrr_filter(
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
            price_area,
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
        property: BidDocumentAFRRFields | SequenceNotStr[BidDocumentAFRRFields] | None = None,
        query: str | None = None,
        search_property: BidDocumentAFRRTextFields | SequenceNotStr[BidDocumentAFRRTextFields] | None = None,
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
        price_area: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
        property: BidDocumentAFRRFields | SequenceNotStr[BidDocumentAFRRFields] | None = None,
        query: str | None = None,
        search_property: BidDocumentAFRRTextFields | SequenceNotStr[BidDocumentAFRRTextFields] | None = None,
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
        price_area: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
        group_by: BidDocumentAFRRFields | SequenceNotStr[BidDocumentAFRRFields],
        property: BidDocumentAFRRFields | SequenceNotStr[BidDocumentAFRRFields] | None = None,
        query: str | None = None,
        search_property: BidDocumentAFRRTextFields | SequenceNotStr[BidDocumentAFRRTextFields] | None = None,
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
        price_area: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
        group_by: BidDocumentAFRRFields | SequenceNotStr[BidDocumentAFRRFields] | None = None,
        property: BidDocumentAFRRFields | SequenceNotStr[BidDocumentAFRRFields] | None = None,
        query: str | None = None,
        search_property: BidDocumentAFRRTextFields | SequenceNotStr[BidDocumentAFRRTextFields] | None = None,
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
        price_area: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across bid document afrrs

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
            price_area: The price area to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid document afrrs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count bid document afrrs in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.bid_document_afrr.aggregate("count", space="my_space")

        """

        filter_ = _create_bid_document_afrr_filter(
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
            price_area,
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
        property: BidDocumentAFRRFields,
        interval: float,
        query: str | None = None,
        search_property: BidDocumentAFRRTextFields | SequenceNotStr[BidDocumentAFRRTextFields] | None = None,
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
        price_area: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for bid document afrrs

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
            price_area: The price area to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid document afrrs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_bid_document_afrr_filter(
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
            price_area,
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

    def query(self) -> BidDocumentAFRRQuery:
        """Start a query for bid document afrrs."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return BidDocumentAFRRQuery(self._client)

    def select(self) -> BidDocumentAFRRQuery:
        """Start selecting from bid document afrrs."""
        warnings.warn("The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2)
        return BidDocumentAFRRQuery(self._client)

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
        price_area: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: BidDocumentAFRRFields | Sequence[BidDocumentAFRRFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> BidDocumentAFRRList:
        """List/filter bid document afrrs

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
            price_area: The price area to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid document afrrs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `alerts`, `price_area` and `bids` for the bid document afrrs. Defaults to 'skip'.
                'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested bid document afrrs

        Examples:

            List bid document afrrs and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_document_afrrs = client.bid_document_afrr.list(limit=5)

        """
        filter_ = _create_bid_document_afrr_filter(
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
            price_area,
            external_id_prefix,
            space,
            filter,
        )

        if retrieve_connections == "skip":
            return self._list(
                limit=limit,
                filter=filter_,
                sort_by=sort_by,  # type: ignore[arg-type]
                direction=direction,
                sort=sort,
            )

        builder = DataClassQueryBuilder(BidDocumentAFRRList)
        has_data = dm.filters.HasData(views=[self._view_id])
        builder.append(
            NodeQueryStep(
                builder.create_name(None),
                dm.query.NodeResultSetExpression(
                    filter=dm.filters.And(filter_, has_data) if filter_ else has_data,
                    sort=self._create_sort(sort_by, direction, sort),  # type: ignore[arg-type]
                ),
                BidDocumentAFRR,
                max_retrieve_limit=limit,
                raw_filter=filter_,
            )
        )
        from_root = builder.get_from()
        edge_alerts = builder.create_name(from_root)
        builder.append(
            EdgeQueryStep(
                edge_alerts,
                dm.query.EdgeResultSetExpression(
                    from_=from_root,
                    direction="outwards",
                    chain_to="destination",
                ),
            )
        )
        edge_bids = builder.create_name(from_root)
        builder.append(
            EdgeQueryStep(
                edge_bids,
                dm.query.EdgeResultSetExpression(
                    from_=from_root,
                    direction="outwards",
                    chain_to="destination",
                ),
            )
        )
        if retrieve_connections == "full":
            builder.append(
                NodeQueryStep(
                    builder.create_name( edge_alerts),
                    dm.query.NodeResultSetExpression(
                        from_= edge_alerts,
                        filter=dm.filters.HasData(views=[Alert._view_id]),
                    ),
                    Alert,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name( edge_bids),
                    dm.query.NodeResultSetExpression(
                        from_= edge_bids,
                        filter=dm.filters.HasData(views=[BidRow._view_id]),
                    ),
                    BidRow,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[PriceAreaAFRR._view_id]),
                        direction="outwards",
                        through=self._view_id.as_property_ref("priceArea"),
                    ),
                    PriceAreaAFRR,
                )
            )
        # We know that that all nodes are connected as it is not possible to filter on connections
        builder.execute_query(self._client, remove_not_connected=False)
        return builder.unpack()
