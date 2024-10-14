from __future__ import annotations

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
    BidMatrix,
    BidMatrixWrite,
    BidMatrixFields,
    BidMatrixList,
    BidMatrixWriteList,
    BidMatrixTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._bid_matrix import (
    _BIDMATRIX_PROPERTIES_BY_FIELD,
    _create_bid_matrix_filter,
)
from ._core import DEFAULT_LIMIT_READ, DEFAULT_QUERY_LIMIT, Aggregations, NodeAPI, SequenceNotStr, QueryStep, QueryBuilder
from .bid_matrix_query import BidMatrixQueryAPI


class BidMatrixAPI(NodeAPI[BidMatrix, BidMatrixWrite, BidMatrixList, BidMatrixWriteList]):
    _view_id = dm.ViewId("power_ops_core", "BidMatrix", "1")
    _properties_by_field = _BIDMATRIX_PROPERTIES_BY_FIELD
    _class_type = BidMatrix
    _class_list = BidMatrixList
    _class_write_list = BidMatrixWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)


    def __call__(
            self,
            state: str | list[str] | None = None,
            state_prefix: str | None = None,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit: int = DEFAULT_QUERY_LIMIT,
            filter: dm.Filter | None = None,
    ) -> BidMatrixQueryAPI[BidMatrixList]:
        """Query starting at bid matrixes.

        Args:
            state: The state to filter on.
            state_prefix: The prefix of the state to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid matrixes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for bid matrixes.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_bid_matrix_filter(
            self._view_id,
            state,
            state_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(BidMatrixList)
        return BidMatrixQueryAPI(self._client, builder, filter_, limit)


    def apply(
        self,
        bid_matrix: BidMatrixWrite | Sequence[BidMatrixWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) bid matrixes.

        Args:
            bid_matrix: Bid matrix or sequence of bid matrixes to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new bid_matrix:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import BidMatrixWrite
                >>> client = PowerOpsModelsV1Client()
                >>> bid_matrix = BidMatrixWrite(external_id="my_bid_matrix", ...)
                >>> result = client.bid_matrix.apply(bid_matrix)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.bid_matrix.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(bid_matrix, replace, write_none)

    def delete(self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> dm.InstancesDeleteResult:
        """Delete one or more bid matrix.

        Args:
            external_id: External id of the bid matrix to delete.
            space: The space where all the bid matrix are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete bid_matrix by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.bid_matrix.delete("my_bid_matrix")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.bid_matrix.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> BidMatrix | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> BidMatrixList:
        ...

    def retrieve(self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> BidMatrix | BidMatrixList | None:
        """Retrieve one or more bid matrixes by id(s).

        Args:
            external_id: External id or list of external ids of the bid matrixes.
            space: The space where all the bid matrixes are located.

        Returns:
            The requested bid matrixes.

        Examples:

            Retrieve bid_matrix by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_matrix = client.bid_matrix.retrieve("my_bid_matrix")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: BidMatrixTextFields | SequenceNotStr[BidMatrixTextFields] | None = None,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: BidMatrixFields | SequenceNotStr[BidMatrixFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> BidMatrixList:
        """Search bid matrixes

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            state: The state to filter on.
            state_prefix: The prefix of the state to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid matrixes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results bid matrixes matching the query.

        Examples:

           Search for 'my_bid_matrix' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_matrixes = client.bid_matrix.search('my_bid_matrix')

        """
        filter_ = _create_bid_matrix_filter(
            self._view_id,
            state,
            state_prefix,
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
        property: BidMatrixFields | SequenceNotStr[BidMatrixFields] | None = None,
        query: str | None = None,
        search_property: BidMatrixTextFields | SequenceNotStr[BidMatrixTextFields] | None = None,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
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
        property: BidMatrixFields | SequenceNotStr[BidMatrixFields] | None = None,
        query: str | None = None,
        search_property: BidMatrixTextFields | SequenceNotStr[BidMatrixTextFields] | None = None,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
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
        group_by: BidMatrixFields | SequenceNotStr[BidMatrixFields],
        property: BidMatrixFields | SequenceNotStr[BidMatrixFields] | None = None,
        query: str | None = None,
        search_property: BidMatrixTextFields | SequenceNotStr[BidMatrixTextFields] | None = None,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
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
        group_by: BidMatrixFields | SequenceNotStr[BidMatrixFields] | None = None,
        property: BidMatrixFields | SequenceNotStr[BidMatrixFields] | None = None,
        query: str | None = None,
        search_property: BidMatrixTextFields | SequenceNotStr[BidMatrixTextFields] | None = None,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across bid matrixes

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            state: The state to filter on.
            state_prefix: The prefix of the state to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid matrixes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count bid matrixes in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.bid_matrix.aggregate("count", space="my_space")

        """

        filter_ = _create_bid_matrix_filter(
            self._view_id,
            state,
            state_prefix,
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
        property: BidMatrixFields,
        interval: float,
        query: str | None = None,
        search_property: BidMatrixTextFields | SequenceNotStr[BidMatrixTextFields] | None = None,
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for bid matrixes

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            state: The state to filter on.
            state_prefix: The prefix of the state to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid matrixes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_bid_matrix_filter(
            self._view_id,
            state,
            state_prefix,
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
        state: str | list[str] | None = None,
        state_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: BidMatrixFields | Sequence[BidMatrixFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> BidMatrixList:
        """List/filter bid matrixes

        Args:
            state: The state to filter on.
            state_prefix: The prefix of the state to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid matrixes to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested bid matrixes

        Examples:

            List bid matrixes and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_matrixes = client.bid_matrix.list(limit=5)

        """
        filter_ = _create_bid_matrix_filter(
            self._view_id,
            state,
            state_prefix,
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
        )
