from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.production.data_classes._core import DEFAULT_INSTANCE_SPACE
from cognite.powerops.client._generated.production.data_classes import (
    DomainModelCore,
    DomainModelApply,
    ResourcesApplyResult,
    WatercourseShop,
    WatercourseShopApply,
    WatercourseShopFields,
    WatercourseShopList,
    WatercourseShopApplyList,
)
from cognite.powerops.client._generated.production.data_classes._watercourse_shop import (
    _WATERCOURSESHOP_PROPERTIES_BY_FIELD,
    _create_watercourse_shop_filter,
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
from .watercourse_shop_query import WatercourseShopQueryAPI


class WatercourseShopAPI(NodeAPI[WatercourseShop, WatercourseShopApply, WatercourseShopList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[WatercourseShop]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=WatercourseShop,
            class_list=WatercourseShopList,
            class_apply_list=WatercourseShopApplyList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> WatercourseShopQueryAPI[WatercourseShopList]:
        """Query starting at watercourse shops.

        Args:
            min_penalty_limit: The minimum value of the penalty limit to filter on.
            max_penalty_limit: The maximum value of the penalty limit to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of watercourse shops to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for watercourse shops.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_watercourse_shop_filter(
            self._view_id,
            min_penalty_limit,
            max_penalty_limit,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(WatercourseShopList)
        return WatercourseShopQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self, watercourse_shop: WatercourseShopApply | Sequence[WatercourseShopApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) watercourse shops.

        Args:
            watercourse_shop: Watercourse shop or sequence of watercourse shops to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new watercourse_shop:

                >>> from cognite.powerops.client._generated.production import ProductionModelAPI
                >>> from cognite.powerops.client._generated.production.data_classes import WatercourseShopApply
                >>> client = ProductionModelAPI()
                >>> watercourse_shop = WatercourseShopApply(external_id="my_watercourse_shop", ...)
                >>> result = client.watercourse_shop.apply(watercourse_shop)

        """
        return self._apply(watercourse_shop, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more watercourse shop.

        Args:
            external_id: External id of the watercourse shop to delete.
            space: The space where all the watercourse shop are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete watercourse_shop by id:

                >>> from cognite.powerops.client._generated.production import ProductionModelAPI
                >>> client = ProductionModelAPI()
                >>> client.watercourse_shop.delete("my_watercourse_shop")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> WatercourseShop | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> WatercourseShopList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> WatercourseShop | WatercourseShopList | None:
        """Retrieve one or more watercourse shops by id(s).

        Args:
            external_id: External id or list of external ids of the watercourse shops.
            space: The space where all the watercourse shops are located.

        Returns:
            The requested watercourse shops.

        Examples:

            Retrieve watercourse_shop by id:

                >>> from cognite.powerops.client._generated.production import ProductionModelAPI
                >>> client = ProductionModelAPI()
                >>> watercourse_shop = client.watercourse_shop.retrieve("my_watercourse_shop")

        """
        return self._retrieve(external_id, space)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: WatercourseShopFields | Sequence[WatercourseShopFields] | None = None,
        group_by: None = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
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
        property: WatercourseShopFields | Sequence[WatercourseShopFields] | None = None,
        group_by: WatercourseShopFields | Sequence[WatercourseShopFields] = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
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
        property: WatercourseShopFields | Sequence[WatercourseShopFields] | None = None,
        group_by: WatercourseShopFields | Sequence[WatercourseShopFields] | None = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across watercourse shops

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            min_penalty_limit: The minimum value of the penalty limit to filter on.
            max_penalty_limit: The maximum value of the penalty limit to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of watercourse shops to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count watercourse shops in space `my_space`:

                >>> from cognite.powerops.client._generated.production import ProductionModelAPI
                >>> client = ProductionModelAPI()
                >>> result = client.watercourse_shop.aggregate("count", space="my_space")

        """

        filter_ = _create_watercourse_shop_filter(
            self._view_id,
            min_penalty_limit,
            max_penalty_limit,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _WATERCOURSESHOP_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: WatercourseShopFields,
        interval: float,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for watercourse shops

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            min_penalty_limit: The minimum value of the penalty limit to filter on.
            max_penalty_limit: The maximum value of the penalty limit to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of watercourse shops to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_watercourse_shop_filter(
            self._view_id,
            min_penalty_limit,
            max_penalty_limit,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _WATERCOURSESHOP_PROPERTIES_BY_FIELD,
            None,
            None,
            limit,
            filter_,
        )

    def list(
        self,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WatercourseShopList:
        """List/filter watercourse shops

        Args:
            min_penalty_limit: The minimum value of the penalty limit to filter on.
            max_penalty_limit: The maximum value of the penalty limit to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of watercourse shops to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested watercourse shops

        Examples:

            List watercourse shops and limit to 5:

                >>> from cognite.powerops.client._generated.production import ProductionModelAPI
                >>> client = ProductionModelAPI()
                >>> watercourse_shops = client.watercourse_shop.list(limit=5)

        """
        filter_ = _create_watercourse_shop_filter(
            self._view_id,
            min_penalty_limit,
            max_penalty_limit,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
