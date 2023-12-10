from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.day_ahead_bid.data_classes._core import DEFAULT_INSTANCE_SPACE
from cognite.powerops.client._generated.day_ahead_bid.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    ProductionPricePair,
    ProductionPricePairApply,
    ProductionPricePairFields,
    ProductionPricePairList,
    ProductionPricePairApplyList,
)
from cognite.powerops.client._generated.day_ahead_bid.data_classes._production_price_pair import (
    _PRODUCTIONPRICEPAIR_PROPERTIES_BY_FIELD,
    _create_production_price_pair_filter,
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
from .production_price_pair_production import ProductionPricePairProductionAPI
from .production_price_pair_price import ProductionPricePairPriceAPI
from .production_price_pair_query import ProductionPricePairQueryAPI


class ProductionPricePairAPI(NodeAPI[ProductionPricePair, ProductionPricePairApply, ProductionPricePairList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[ProductionPricePairApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=ProductionPricePair,
            class_apply_type=ProductionPricePairApply,
            class_list=ProductionPricePairList,
            class_apply_list=ProductionPricePairApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.production = ProductionPricePairProductionAPI(client, view_id)
        self.price = ProductionPricePairPriceAPI(client, view_id)

    def __call__(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> ProductionPricePairQueryAPI[ProductionPricePairList]:
        """Query starting at production price pairs.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of production price pairs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for production price pairs.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_production_price_pair_filter(
            self._view_id,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(ProductionPricePairList)
        return ProductionPricePairQueryAPI(self._client, builder, self._view_by_write_class, filter_, limit)

    def apply(
        self,
        production_price_pair: ProductionPricePairApply | Sequence[ProductionPricePairApply],
        replace: bool = False,
    ) -> ResourcesApplyResult:
        """Add or update (upsert) production price pairs.

        Args:
            production_price_pair: Production price pair or sequence of production price pairs to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new production_price_pair:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> from cognite.powerops.client._generated.day_ahead_bid.data_classes import ProductionPricePairApply
                >>> client = DayAheadBidAPI()
                >>> production_price_pair = ProductionPricePairApply(external_id="my_production_price_pair", ...)
                >>> result = client.production_price_pair.apply(production_price_pair)

        """
        return self._apply(production_price_pair, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more production price pair.

        Args:
            external_id: External id of the production price pair to delete.
            space: The space where all the production price pair are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete production_price_pair by id:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> client.production_price_pair.delete("my_production_price_pair")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> ProductionPricePair | None:
        ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> ProductionPricePairList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> ProductionPricePair | ProductionPricePairList | None:
        """Retrieve one or more production price pairs by id(s).

        Args:
            external_id: External id or list of external ids of the production price pairs.
            space: The space where all the production price pairs are located.

        Returns:
            The requested production price pairs.

        Examples:

            Retrieve production_price_pair by id:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> production_price_pair = client.production_price_pair.retrieve("my_production_price_pair")

        """
        return self._retrieve(external_id, space)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ProductionPricePairFields | Sequence[ProductionPricePairFields] | None = None,
        group_by: None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
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
        property: ProductionPricePairFields | Sequence[ProductionPricePairFields] | None = None,
        group_by: ProductionPricePairFields | Sequence[ProductionPricePairFields] = None,
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
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: ProductionPricePairFields | Sequence[ProductionPricePairFields] | None = None,
        group_by: ProductionPricePairFields | Sequence[ProductionPricePairFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across production price pairs

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of production price pairs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count production price pairs in space `my_space`:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> result = client.production_price_pair.aggregate("count", space="my_space")

        """

        filter_ = _create_production_price_pair_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _PRODUCTIONPRICEPAIR_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: ProductionPricePairFields,
        interval: float,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for production price pairs

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of production price pairs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_production_price_pair_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _PRODUCTIONPRICEPAIR_PROPERTIES_BY_FIELD,
            None,
            None,
            limit,
            filter_,
        )

    def list(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ProductionPricePairList:
        """List/filter production price pairs

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of production price pairs to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested production price pairs

        Examples:

            List production price pairs and limit to 5:

                >>> from cognite.powerops.client._generated.day_ahead_bid import DayAheadBidAPI
                >>> client = DayAheadBidAPI()
                >>> production_price_pairs = client.production_price_pair.list(limit=5)

        """
        filter_ = _create_production_price_pair_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
