from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes import (
    DomainModelApply,
    ResourcesApplyResult,
    MarketPriceArea,
    MarketPriceAreaApply,
    MarketPriceAreaFields,
    MarketPriceAreaList,
    MarketPriceAreaApplyList,
    MarketPriceAreaTextFields,
)
from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes._market_price_area import (
    _MARKETPRICEAREA_PROPERTIES_BY_FIELD,
    _create_market_price_area_filter,
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
from .market_price_area_main_scenario import MarketPriceAreaMainScenarioAPI
from .market_price_area_query import MarketPriceAreaQueryAPI


class MarketPriceAreaAPI(NodeAPI[MarketPriceArea, MarketPriceAreaApply, MarketPriceAreaList]):
    def __init__(self, client: CogniteClient, view_by_write_class: dict[type[DomainModelApply], dm.ViewId]):
        view_id = view_by_write_class[MarketPriceAreaApply]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=MarketPriceArea,
            class_apply_type=MarketPriceAreaApply,
            class_list=MarketPriceAreaList,
            class_apply_list=MarketPriceAreaApplyList,
            view_by_write_class=view_by_write_class,
        )
        self._view_id = view_id
        self.main_scenario = MarketPriceAreaMainScenarioAPI(client, view_id)

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        default_method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> MarketPriceAreaQueryAPI[MarketPriceAreaList]:
        """Query starting at market price areas.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            default_method: The default method to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of market price areas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for market price areas.

        """
        filter_ = _create_market_price_area_filter(
            self._view_id,
            name,
            name_prefix,
            price_area,
            price_area_prefix,
            default_method,
            timezone,
            timezone_prefix,
            external_id_prefix,
            space,
            filter,
        )
        builder = QueryBuilder(
            MarketPriceAreaList,
            [
                QueryStep(
                    name="market_price_area",
                    expression=dm.query.NodeResultSetExpression(
                        from_=None,
                        filter=filter_,
                    ),
                    select=dm.query.Select(
                        [dm.query.SourceSelector(self._view_id, list(_MARKETPRICEAREA_PROPERTIES_BY_FIELD.values()))]
                    ),
                    result_cls=MarketPriceArea,
                    max_retrieve_limit=limit,
                )
            ],
        )
        return MarketPriceAreaQueryAPI(self._client, builder, self._view_by_write_class)

    def apply(
        self, market_price_area: MarketPriceAreaApply | Sequence[MarketPriceAreaApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) market price areas.

        Args:
            market_price_area: Market price area or sequence of market price areas to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new market_price_area:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes import MarketPriceAreaApply
                >>> client = DayAheadFrontendContractAPI()
                >>> market_price_area = MarketPriceAreaApply(external_id="my_market_price_area", ...)
                >>> result = client.market_price_area.apply(market_price_area)

        """
        return self._apply(market_price_area, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = "poweropsDayAheadFrontendContractModel"
    ) -> dm.InstancesDeleteResult:
        """Delete one or more market price area.

        Args:
            external_id: External id of the market price area to delete.
            space: The space where all the market price area are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete market_price_area by id:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> client.market_price_area.delete("my_market_price_area")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str) -> MarketPriceArea | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> MarketPriceAreaList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = "poweropsDayAheadFrontendContractModel"
    ) -> MarketPriceArea | MarketPriceAreaList | None:
        """Retrieve one or more market price areas by id(s).

        Args:
            external_id: External id or list of external ids of the market price areas.
            space: The space where all the market price areas are located.

        Returns:
            The requested market price areas.

        Examples:

            Retrieve market_price_area by id:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> market_price_area = client.market_price_area.retrieve("my_market_price_area")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: MarketPriceAreaTextFields | Sequence[MarketPriceAreaTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        default_method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> MarketPriceAreaList:
        """Search market price areas

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            default_method: The default method to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of market price areas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results market price areas matching the query.

        Examples:

           Search for 'my_market_price_area' in all text properties:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> market_price_areas = client.market_price_area.search('my_market_price_area')

        """
        filter_ = _create_market_price_area_filter(
            self._view_id,
            name,
            name_prefix,
            price_area,
            price_area_prefix,
            default_method,
            timezone,
            timezone_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _MARKETPRICEAREA_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: MarketPriceAreaFields | Sequence[MarketPriceAreaFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: MarketPriceAreaTextFields | Sequence[MarketPriceAreaTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        default_method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
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
        property: MarketPriceAreaFields | Sequence[MarketPriceAreaFields] | None = None,
        group_by: MarketPriceAreaFields | Sequence[MarketPriceAreaFields] = None,
        query: str | None = None,
        search_properties: MarketPriceAreaTextFields | Sequence[MarketPriceAreaTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        default_method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
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
        property: MarketPriceAreaFields | Sequence[MarketPriceAreaFields] | None = None,
        group_by: MarketPriceAreaFields | Sequence[MarketPriceAreaFields] | None = None,
        query: str | None = None,
        search_property: MarketPriceAreaTextFields | Sequence[MarketPriceAreaTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        default_method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across market price areas

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            default_method: The default method to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of market price areas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count market price areas in space `my_space`:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> result = client.market_price_area.aggregate("count", space="my_space")

        """

        filter_ = _create_market_price_area_filter(
            self._view_id,
            name,
            name_prefix,
            price_area,
            price_area_prefix,
            default_method,
            timezone,
            timezone_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _MARKETPRICEAREA_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: MarketPriceAreaFields,
        interval: float,
        query: str | None = None,
        search_property: MarketPriceAreaTextFields | Sequence[MarketPriceAreaTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        default_method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for market price areas

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            default_method: The default method to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of market price areas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_market_price_area_filter(
            self._view_id,
            name,
            name_prefix,
            price_area,
            price_area_prefix,
            default_method,
            timezone,
            timezone_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _MARKETPRICEAREA_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        price_area: str | list[str] | None = None,
        price_area_prefix: str | None = None,
        default_method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> MarketPriceAreaList:
        """List/filter market price areas

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            price_area: The price area to filter on.
            price_area_prefix: The prefix of the price area to filter on.
            default_method: The default method to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of market price areas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested market price areas

        Examples:

            List market price areas and limit to 5:

                >>> from cognite.powerops.client._generated.day_ahead_frontend_contract import DayAheadFrontendContractAPI
                >>> client = DayAheadFrontendContractAPI()
                >>> market_price_areas = client.market_price_area.list(limit=5)

        """
        filter_ = _create_market_price_area_filter(
            self._view_id,
            name,
            name_prefix,
            price_area,
            price_area_prefix,
            default_method,
            timezone,
            timezone_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
