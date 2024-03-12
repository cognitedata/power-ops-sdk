from __future__ import annotations

from collections.abc import Sequence
from typing import overload
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    SHOPTimeSeries,
    SHOPTimeSeriesWrite,
    SHOPTimeSeriesFields,
    SHOPTimeSeriesList,
    SHOPTimeSeriesWriteList,
    SHOPTimeSeriesTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._shop_time_series import (
    _SHOPTIMESERIES_PROPERTIES_BY_FIELD,
    _create_shop_time_series_filter,
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
from .shop_time_series_timeseries import SHOPTimeSeriesTimeseriesAPI
from .shop_time_series_query import SHOPTimeSeriesQueryAPI


class SHOPTimeSeriesAPI(NodeAPI[SHOPTimeSeries, SHOPTimeSeriesWrite, SHOPTimeSeriesList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[SHOPTimeSeries]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=SHOPTimeSeries,
            class_list=SHOPTimeSeriesList,
            class_write_list=SHOPTimeSeriesWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.timeseries = SHOPTimeSeriesTimeseriesAPI(client, view_id)

    def __call__(
        self,
        object_type: str | list[str] | None = None,
        object_type_prefix: str | None = None,
        object_name: str | list[str] | None = None,
        object_name_prefix: str | None = None,
        attribute_name: str | list[str] | None = None,
        attribute_name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> SHOPTimeSeriesQueryAPI[SHOPTimeSeriesList]:
        """Query starting at shop time series.

        Args:
            object_type: The object type to filter on.
            object_type_prefix: The prefix of the object type to filter on.
            object_name: The object name to filter on.
            object_name_prefix: The prefix of the object name to filter on.
            attribute_name: The attribute name to filter on.
            attribute_name_prefix: The prefix of the attribute name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop time series to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for shop time series.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_shop_time_series_filter(
            self._view_id,
            object_type,
            object_type_prefix,
            object_name,
            object_name_prefix,
            attribute_name,
            attribute_name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(SHOPTimeSeriesList)
        return SHOPTimeSeriesQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        shop_time_series: SHOPTimeSeriesWrite | Sequence[SHOPTimeSeriesWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) shop time series.

        Args:
            shop_time_series: Shop time series or sequence of shop time series to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new shop_time_series:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import SHOPTimeSeriesWrite
                >>> client = PowerOpsModelsV1Client()
                >>> shop_time_series = SHOPTimeSeriesWrite(external_id="my_shop_time_series", ...)
                >>> result = client.shop_time_series.apply(shop_time_series)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.shop_time_series.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(shop_time_series, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more shop time series.

        Args:
            external_id: External id of the shop time series to delete.
            space: The space where all the shop time series are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete shop_time_series by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.shop_time_series.delete("my_shop_time_series")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.shop_time_series.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> SHOPTimeSeries | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> SHOPTimeSeriesList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> SHOPTimeSeries | SHOPTimeSeriesList | None:
        """Retrieve one or more shop time series by id(s).

        Args:
            external_id: External id or list of external ids of the shop time series.
            space: The space where all the shop time series are located.

        Returns:
            The requested shop time series.

        Examples:

            Retrieve shop_time_series by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_time_series = client.shop_time_series.retrieve("my_shop_time_series")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: SHOPTimeSeriesTextFields | Sequence[SHOPTimeSeriesTextFields] | None = None,
        object_type: str | list[str] | None = None,
        object_type_prefix: str | None = None,
        object_name: str | list[str] | None = None,
        object_name_prefix: str | None = None,
        attribute_name: str | list[str] | None = None,
        attribute_name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> SHOPTimeSeriesList:
        """Search shop time series

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            object_type: The object type to filter on.
            object_type_prefix: The prefix of the object type to filter on.
            object_name: The object name to filter on.
            object_name_prefix: The prefix of the object name to filter on.
            attribute_name: The attribute name to filter on.
            attribute_name_prefix: The prefix of the attribute name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop time series to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results shop time series matching the query.

        Examples:

           Search for 'my_shop_time_series' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_time_series_list = client.shop_time_series.search('my_shop_time_series')

        """
        filter_ = _create_shop_time_series_filter(
            self._view_id,
            object_type,
            object_type_prefix,
            object_name,
            object_name_prefix,
            attribute_name,
            attribute_name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _SHOPTIMESERIES_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: SHOPTimeSeriesFields | Sequence[SHOPTimeSeriesFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: SHOPTimeSeriesTextFields | Sequence[SHOPTimeSeriesTextFields] | None = None,
        object_type: str | list[str] | None = None,
        object_type_prefix: str | None = None,
        object_name: str | list[str] | None = None,
        object_name_prefix: str | None = None,
        attribute_name: str | list[str] | None = None,
        attribute_name_prefix: str | None = None,
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
        property: SHOPTimeSeriesFields | Sequence[SHOPTimeSeriesFields] | None = None,
        group_by: SHOPTimeSeriesFields | Sequence[SHOPTimeSeriesFields] = None,
        query: str | None = None,
        search_properties: SHOPTimeSeriesTextFields | Sequence[SHOPTimeSeriesTextFields] | None = None,
        object_type: str | list[str] | None = None,
        object_type_prefix: str | None = None,
        object_name: str | list[str] | None = None,
        object_name_prefix: str | None = None,
        attribute_name: str | list[str] | None = None,
        attribute_name_prefix: str | None = None,
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
        property: SHOPTimeSeriesFields | Sequence[SHOPTimeSeriesFields] | None = None,
        group_by: SHOPTimeSeriesFields | Sequence[SHOPTimeSeriesFields] | None = None,
        query: str | None = None,
        search_property: SHOPTimeSeriesTextFields | Sequence[SHOPTimeSeriesTextFields] | None = None,
        object_type: str | list[str] | None = None,
        object_type_prefix: str | None = None,
        object_name: str | list[str] | None = None,
        object_name_prefix: str | None = None,
        attribute_name: str | list[str] | None = None,
        attribute_name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across shop time series

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            object_type: The object type to filter on.
            object_type_prefix: The prefix of the object type to filter on.
            object_name: The object name to filter on.
            object_name_prefix: The prefix of the object name to filter on.
            attribute_name: The attribute name to filter on.
            attribute_name_prefix: The prefix of the attribute name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop time series to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count shop time series in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.shop_time_series.aggregate("count", space="my_space")

        """

        filter_ = _create_shop_time_series_filter(
            self._view_id,
            object_type,
            object_type_prefix,
            object_name,
            object_name_prefix,
            attribute_name,
            attribute_name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _SHOPTIMESERIES_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: SHOPTimeSeriesFields,
        interval: float,
        query: str | None = None,
        search_property: SHOPTimeSeriesTextFields | Sequence[SHOPTimeSeriesTextFields] | None = None,
        object_type: str | list[str] | None = None,
        object_type_prefix: str | None = None,
        object_name: str | list[str] | None = None,
        object_name_prefix: str | None = None,
        attribute_name: str | list[str] | None = None,
        attribute_name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for shop time series

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            object_type: The object type to filter on.
            object_type_prefix: The prefix of the object type to filter on.
            object_name: The object name to filter on.
            object_name_prefix: The prefix of the object name to filter on.
            attribute_name: The attribute name to filter on.
            attribute_name_prefix: The prefix of the attribute name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop time series to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_shop_time_series_filter(
            self._view_id,
            object_type,
            object_type_prefix,
            object_name,
            object_name_prefix,
            attribute_name,
            attribute_name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _SHOPTIMESERIES_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        object_type: str | list[str] | None = None,
        object_type_prefix: str | None = None,
        object_name: str | list[str] | None = None,
        object_name_prefix: str | None = None,
        attribute_name: str | list[str] | None = None,
        attribute_name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> SHOPTimeSeriesList:
        """List/filter shop time series

        Args:
            object_type: The object type to filter on.
            object_type_prefix: The prefix of the object type to filter on.
            object_name: The object name to filter on.
            object_name_prefix: The prefix of the object name to filter on.
            attribute_name: The attribute name to filter on.
            attribute_name_prefix: The prefix of the attribute name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop time series to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested shop time series

        Examples:

            List shop time series and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_time_series_list = client.shop_time_series.list(limit=5)

        """
        filter_ = _create_shop_time_series_filter(
            self._view_id,
            object_type,
            object_type_prefix,
            object_name,
            object_name_prefix,
            attribute_name,
            attribute_name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
