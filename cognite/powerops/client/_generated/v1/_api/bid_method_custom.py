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
    BidMethodCustom,
    BidMethodCustomWrite,
    BidMethodCustomFields,
    BidMethodCustomList,
    BidMethodCustomWriteList,
    BidMethodCustomTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._bid_method_custom import (
    _BIDMETHODCUSTOM_PROPERTIES_BY_FIELD,
    _create_bid_method_custom_filter,
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
from .bid_method_custom_query import BidMethodCustomQueryAPI


class BidMethodCustomAPI(NodeAPI[BidMethodCustom, BidMethodCustomWrite, BidMethodCustomList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[BidMethodCustom]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=BidMethodCustom,
            class_list=BidMethodCustomList,
            class_write_list=BidMethodCustomWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        main_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> BidMethodCustomQueryAPI[BidMethodCustomList]:
        """Query starting at bid method customs.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            main_scenario: The main scenario to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid method customs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for bid method customs.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_bid_method_custom_filter(
            self._view_id,
            name,
            name_prefix,
            main_scenario,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(BidMethodCustomList)
        return BidMethodCustomQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        bid_method_custom: BidMethodCustomWrite | Sequence[BidMethodCustomWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) bid method customs.

        Args:
            bid_method_custom: Bid method custom or sequence of bid method customs to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new bid_method_custom:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import BidMethodCustomWrite
                >>> client = PowerOpsModelsV1Client()
                >>> bid_method_custom = BidMethodCustomWrite(external_id="my_bid_method_custom", ...)
                >>> result = client.bid_method_custom.apply(bid_method_custom)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.bid_method_custom.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(bid_method_custom, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more bid method custom.

        Args:
            external_id: External id of the bid method custom to delete.
            space: The space where all the bid method custom are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete bid_method_custom by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.bid_method_custom.delete("my_bid_method_custom")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.bid_method_custom.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> BidMethodCustom | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> BidMethodCustomList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> BidMethodCustom | BidMethodCustomList | None:
        """Retrieve one or more bid method customs by id(s).

        Args:
            external_id: External id or list of external ids of the bid method customs.
            space: The space where all the bid method customs are located.

        Returns:
            The requested bid method customs.

        Examples:

            Retrieve bid_method_custom by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_method_custom = client.bid_method_custom.retrieve("my_bid_method_custom")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: BidMethodCustomTextFields | Sequence[BidMethodCustomTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        main_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BidMethodCustomList:
        """Search bid method customs

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            main_scenario: The main scenario to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid method customs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results bid method customs matching the query.

        Examples:

           Search for 'my_bid_method_custom' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_method_customs = client.bid_method_custom.search('my_bid_method_custom')

        """
        filter_ = _create_bid_method_custom_filter(
            self._view_id,
            name,
            name_prefix,
            main_scenario,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _BIDMETHODCUSTOM_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: BidMethodCustomFields | Sequence[BidMethodCustomFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: BidMethodCustomTextFields | Sequence[BidMethodCustomTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        main_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: BidMethodCustomFields | Sequence[BidMethodCustomFields] | None = None,
        group_by: BidMethodCustomFields | Sequence[BidMethodCustomFields] = None,
        query: str | None = None,
        search_properties: BidMethodCustomTextFields | Sequence[BidMethodCustomTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        main_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: BidMethodCustomFields | Sequence[BidMethodCustomFields] | None = None,
        group_by: BidMethodCustomFields | Sequence[BidMethodCustomFields] | None = None,
        query: str | None = None,
        search_property: BidMethodCustomTextFields | Sequence[BidMethodCustomTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        main_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across bid method customs

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            main_scenario: The main scenario to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid method customs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count bid method customs in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.bid_method_custom.aggregate("count", space="my_space")

        """

        filter_ = _create_bid_method_custom_filter(
            self._view_id,
            name,
            name_prefix,
            main_scenario,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _BIDMETHODCUSTOM_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: BidMethodCustomFields,
        interval: float,
        query: str | None = None,
        search_property: BidMethodCustomTextFields | Sequence[BidMethodCustomTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        main_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for bid method customs

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            main_scenario: The main scenario to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid method customs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_bid_method_custom_filter(
            self._view_id,
            name,
            name_prefix,
            main_scenario,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _BIDMETHODCUSTOM_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        main_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BidMethodCustomList:
        """List/filter bid method customs

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            main_scenario: The main scenario to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid method customs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested bid method customs

        Examples:

            List bid method customs and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_method_customs = client.bid_method_custom.list(limit=5)

        """
        filter_ = _create_bid_method_custom_filter(
            self._view_id,
            name,
            name_prefix,
            main_scenario,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
