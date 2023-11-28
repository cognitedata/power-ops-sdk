from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    SHOP,
    SHOPApply,
    SHOPFields,
    SHOPList,
    SHOPApplyList,
    SHOPTextFields,
)
from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes._shop import (
    _SHOP_PROPERTIES_BY_FIELD,
    _create_shop_filter,
)
from ._core import DEFAULT_LIMIT_READ, DEFAULT_QUERY_LIMIT, Aggregations, NodeAPI, SequenceNotStr, QueryStep, QueryBuilder
from .shop_price_scenarios import SHOPPriceScenariosAPI
from .shop_query import SHOPQueryAPI


class SHOPAPI(NodeAPI[SHOP, SHOPApply, SHOPList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[SHOPApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=SHOP,
            class_apply_type=SHOPApply,
            class_list=SHOPList,
            class_apply_list=SHOPApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.price_scenarios = SHOPPriceScenariosAPI(client, view_id)

    def __call__(
            self,
            name: str | list[str] | None = None,
            name_prefix: str | None = None,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit: int = DEFAULT_QUERY_LIMIT,
            filter: dm.Filter | None = None,
    ) -> SHOPQueryAPI[SHOPList]:
        """Query starting at shops.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shops to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for shops.

        """
        filter_ = _create_shop_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            SHOPList,
            [
                QueryStep(
                    name="shop",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_SHOP_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls= SHOP,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return SHOPQueryAPI(self._client, builder, self._view_by_write_class)


    def apply(self, shop: SHOPApply | Sequence[SHOPApply], replace: bool = False) -> ResourcesApplyResult:
        """Add or update (upsert) shops.

        Args:
            shop: Shop or sequence of shops to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new shop:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes import SHOPApply
                >>> client = DayAheadFrontendContractAPI()
                >>> shop = SHOPApply(external_id="my_shop", ...)
                >>> result = client.shop.apply(shop)

        """
        return self._apply(shop, replace)

    def delete(self, external_id: str | SequenceNotStr[str], space: str ="dayAheadFrontendContractModel") -> dm.InstancesDeleteResult:
        """Delete one or more shop.

        Args:
            external_id: External id of the shop to delete.
            space: The space where all the shop are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete shop by id:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> client.shop.delete("my_shop")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> SHOP | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> SHOPList:
        ...

    def retrieve(self, external_id: str | SequenceNotStr[str], space: str ="dayAheadFrontendContractModel") -> SHOP | SHOPList | None:
        """Retrieve one or more shops by id(s).

        Args:
            external_id: External id or list of external ids of the shops.
            space: The space where all the shops are located.

        Returns:
            The requested shops.

        Examples:

            Retrieve shop by id:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> shop = client.shop.retrieve("my_shop")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: SHOPTextFields | Sequence[SHOPTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> SHOPList:
        """Search shops

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shops to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results shops matching the query.

        Examples:

           Search for 'my_shop' in all text properties:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> shops = client.shop.search('my_shop')

        """
        filter_ = _create_shop_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _SHOP_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: SHOPFields | Sequence[SHOPFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: SHOPTextFields | Sequence[SHOPTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        property: SHOPFields | Sequence[SHOPFields] | None = None,
        group_by: SHOPFields | Sequence[SHOPFields] = None,
        query: str | None = None,
        search_properties: SHOPTextFields | Sequence[SHOPTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
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
        property: SHOPFields | Sequence[SHOPFields] | None = None,
        group_by: SHOPFields | Sequence[SHOPFields] | None = None,
        query: str | None = None,
        search_property: SHOPTextFields | Sequence[SHOPTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across shops

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
            limit: Maximum number of shops to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count shops in space `my_space`:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> result = client.shop.aggregate("count", space="my_space")

        """

        filter_ = _create_shop_filter(
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
            _SHOP_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: SHOPFields,
        interval: float,
        query: str | None = None,
        search_property: SHOPTextFields | Sequence[SHOPTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for shops

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shops to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_shop_filter(
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
            _SHOP_PROPERTIES_BY_FIELD,
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
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> SHOPList:
        """List/filter shops

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shops to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above. 

        Returns:
            List of requested shops

        Examples:

            List shops and limit to 5:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> shops = client.shop.list(limit=5)

        """
        filter_ = _create_shop_filter(
            self._view_id,
            name,
            name_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
