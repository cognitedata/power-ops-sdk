from __future__ import annotations

from collections.abc import Sequence
from typing import overload
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.afrr_bid.data_classes._core import DEFAULT_INSTANCE_SPACE
from cognite.powerops.client._generated.afrr_bid.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    PriceArea,
    PriceAreaFields,
    PriceAreaList,
    PriceAreaTextFields,
)
from cognite.powerops.client._generated.afrr_bid.data_classes._price_area import (
    _PRICEAREA_PROPERTIES_BY_FIELD,
    _create_price_area_filter,
)
from ._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_QUERY_LIMIT,
    Aggregations,
    NodeReadAPI,
    SequenceNotStr,
    QueryStep,
    QueryBuilder,
)
from .price_area_capacity_price_up import PriceAreaCapacityPriceUpAPI
from .price_area_capacity_price_down import PriceAreaCapacityPriceDownAPI
from .price_area_activation_price_up import PriceAreaActivationPriceUpAPI
from .price_area_activation_price_down import PriceAreaActivationPriceDownAPI
from .price_area_relative_activation import PriceAreaRelativeActivationAPI
from .price_area_total_capacity_allocation_up import PriceAreaTotalCapacityAllocationUpAPI
from .price_area_total_capacity_allocation_down import PriceAreaTotalCapacityAllocationDownAPI
from .price_area_own_capacity_allocation_up import PriceAreaOwnCapacityAllocationUpAPI
from .price_area_own_capacity_allocation_down import PriceAreaOwnCapacityAllocationDownAPI
from .price_area_query import PriceAreaQueryAPI


class PriceAreaAPI(NodeReadAPI[PriceArea, PriceAreaList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[PriceArea]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=PriceArea,
            class_list=PriceAreaList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.capacity_price_up = PriceAreaCapacityPriceUpAPI(client, view_id)
        self.capacity_price_down = PriceAreaCapacityPriceDownAPI(client, view_id)
        self.activation_price_up = PriceAreaActivationPriceUpAPI(client, view_id)
        self.activation_price_down = PriceAreaActivationPriceDownAPI(client, view_id)
        self.relative_activation = PriceAreaRelativeActivationAPI(client, view_id)
        self.total_capacity_allocation_up = PriceAreaTotalCapacityAllocationUpAPI(client, view_id)
        self.total_capacity_allocation_down = PriceAreaTotalCapacityAllocationDownAPI(client, view_id)
        self.own_capacity_allocation_up = PriceAreaOwnCapacityAllocationUpAPI(client, view_id)
        self.own_capacity_allocation_down = PriceAreaOwnCapacityAllocationDownAPI(client, view_id)

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> PriceAreaQueryAPI[PriceAreaList]:
        """Query starting at price areas.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of price areas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for price areas.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_price_area_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(PriceAreaList)
        return PriceAreaQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more price area.

        Args:
            external_id: External id of the price area to delete.
            space: The space where all the price area are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete price_area by id:

                >>> from cognite.powerops.client._generated.afrr_bid import AFRRBidAPI
                >>> client = AFRRBidAPI()
                >>> client.price_area.delete("my_price_area")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.price_area.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> PriceArea | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> PriceAreaList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> PriceArea | PriceAreaList | None:
        """Retrieve one or more price areas by id(s).

        Args:
            external_id: External id or list of external ids of the price areas.
            space: The space where all the price areas are located.

        Returns:
            The requested price areas.

        Examples:

            Retrieve price_area by id:

                >>> from cognite.powerops.client._generated.afrr_bid import AFRRBidAPI
                >>> client = AFRRBidAPI()
                >>> price_area = client.price_area.retrieve("my_price_area")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: PriceAreaTextFields | Sequence[PriceAreaTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PriceAreaList:
        """Search price areas

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of price areas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results price areas matching the query.

        Examples:

           Search for 'my_price_area' in all text properties:

                >>> from cognite.powerops.client._generated.afrr_bid import AFRRBidAPI
                >>> client = AFRRBidAPI()
                >>> price_areas = client.price_area.search('my_price_area')

        """
        filter_ = _create_price_area_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _PRICEAREA_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: PriceAreaFields | Sequence[PriceAreaFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: PriceAreaTextFields | Sequence[PriceAreaTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        property: PriceAreaFields | Sequence[PriceAreaFields] | None = None,
        group_by: PriceAreaFields | Sequence[PriceAreaFields] = None,
        query: str | None = None,
        search_properties: PriceAreaTextFields | Sequence[PriceAreaTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        property: PriceAreaFields | Sequence[PriceAreaFields] | None = None,
        group_by: PriceAreaFields | Sequence[PriceAreaFields] | None = None,
        query: str | None = None,
        search_property: PriceAreaTextFields | Sequence[PriceAreaTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across price areas

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of price areas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count price areas in space `my_space`:

                >>> from cognite.powerops.client._generated.afrr_bid import AFRRBidAPI
                >>> client = AFRRBidAPI()
                >>> result = client.price_area.aggregate("count", space="my_space")

        """

        filter_ = _create_price_area_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _PRICEAREA_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: PriceAreaFields,
        interval: float,
        query: str | None = None,
        search_property: PriceAreaTextFields | Sequence[PriceAreaTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for price areas

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of price areas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_price_area_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _PRICEAREA_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PriceAreaList:
        """List/filter price areas

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of price areas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested price areas

        Examples:

            List price areas and limit to 5:

                >>> from cognite.powerops.client._generated.afrr_bid import AFRRBidAPI
                >>> client = AFRRBidAPI()
                >>> price_areas = client.price_area.list(limit=5)

        """
        filter_ = _create_price_area_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
