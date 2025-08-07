from __future__ import annotations

import warnings
from collections.abc import Iterator, Sequence
from typing import Any, ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite.powerops.client._generated.v1._api._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_CHUNK_SIZE,
    instantiate_classes,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite.powerops.client._generated.v1.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    QueryBuildStepFactory,
    QueryBuilder,
    QueryExecutor,
    QueryUnpacker,
    ViewPropertyId,
)
from cognite.powerops.client._generated.v1.data_classes._market_configuration import (
    MarketConfigurationQuery,
    _MARKETCONFIGURATION_PROPERTIES_BY_FIELD,
    _create_market_configuration_filter,
)
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    MarketConfiguration,
    MarketConfigurationWrite,
    MarketConfigurationFields,
    MarketConfigurationList,
    MarketConfigurationWriteList,
    MarketConfigurationTextFields,
)


class MarketConfigurationAPI(NodeAPI[MarketConfiguration, MarketConfigurationWrite, MarketConfigurationList, MarketConfigurationWriteList]):
    _view_id = dm.ViewId("power_ops_core", "MarketConfiguration", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _MARKETCONFIGURATION_PROPERTIES_BY_FIELD
    _class_type = MarketConfiguration
    _class_list = MarketConfigurationList
    _class_write_list = MarketConfigurationWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)


    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> MarketConfiguration | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> MarketConfigurationList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> MarketConfiguration | MarketConfigurationList | None:
        """Retrieve one or more market configurations by id(s).

        Args:
            external_id: External id or list of external ids of the market configurations.
            space: The space where all the market configurations are located.

        Returns:
            The requested market configurations.

        Examples:

            Retrieve market_configuration by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> market_configuration = client.market_configuration.retrieve(
                ...     "my_market_configuration"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
        )

    def search(
        self,
        query: str,
        properties: MarketConfigurationTextFields | SequenceNotStr[MarketConfigurationTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_max_price: float | None = None,
        max_max_price: float | None = None,
        min_min_price: float | None = None,
        max_min_price: float | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        price_unit: str | list[str] | None = None,
        price_unit_prefix: str | None = None,
        min_price_steps: int | None = None,
        max_price_steps: int | None = None,
        min_tick_size: float | None = None,
        max_tick_size: float | None = None,
        time_unit: str | list[str] | None = None,
        time_unit_prefix: str | None = None,
        min_trade_lot: float | None = None,
        max_trade_lot: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: MarketConfigurationFields | SequenceNotStr[MarketConfigurationFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> MarketConfigurationList:
        """Search market configurations

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_max_price: The minimum value of the max price to filter on.
            max_max_price: The maximum value of the max price to filter on.
            min_min_price: The minimum value of the min price to filter on.
            max_min_price: The maximum value of the min price to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            price_unit: The price unit to filter on.
            price_unit_prefix: The prefix of the price unit to filter on.
            min_price_steps: The minimum value of the price step to filter on.
            max_price_steps: The maximum value of the price step to filter on.
            min_tick_size: The minimum value of the tick size to filter on.
            max_tick_size: The maximum value of the tick size to filter on.
            time_unit: The time unit to filter on.
            time_unit_prefix: The prefix of the time unit to filter on.
            min_trade_lot: The minimum value of the trade lot to filter on.
            max_trade_lot: The maximum value of the trade lot to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of market configurations to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results market configurations matching the query.

        Examples:

           Search for 'my_market_configuration' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> market_configurations = client.market_configuration.search(
                ...     'my_market_configuration'
                ... )

        """
        filter_ = _create_market_configuration_filter(
            self._view_id,
            name,
            name_prefix,
            min_max_price,
            max_max_price,
            min_min_price,
            max_min_price,
            timezone,
            timezone_prefix,
            price_unit,
            price_unit_prefix,
            min_price_steps,
            max_price_steps,
            min_tick_size,
            max_tick_size,
            time_unit,
            time_unit_prefix,
            min_trade_lot,
            max_trade_lot,
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
        property: MarketConfigurationFields | SequenceNotStr[MarketConfigurationFields] | None = None,
        query: str | None = None,
        search_property: MarketConfigurationTextFields | SequenceNotStr[MarketConfigurationTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_max_price: float | None = None,
        max_max_price: float | None = None,
        min_min_price: float | None = None,
        max_min_price: float | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        price_unit: str | list[str] | None = None,
        price_unit_prefix: str | None = None,
        min_price_steps: int | None = None,
        max_price_steps: int | None = None,
        min_tick_size: float | None = None,
        max_tick_size: float | None = None,
        time_unit: str | list[str] | None = None,
        time_unit_prefix: str | None = None,
        min_trade_lot: float | None = None,
        max_trade_lot: float | None = None,
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
        property: MarketConfigurationFields | SequenceNotStr[MarketConfigurationFields] | None = None,
        query: str | None = None,
        search_property: MarketConfigurationTextFields | SequenceNotStr[MarketConfigurationTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_max_price: float | None = None,
        max_max_price: float | None = None,
        min_min_price: float | None = None,
        max_min_price: float | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        price_unit: str | list[str] | None = None,
        price_unit_prefix: str | None = None,
        min_price_steps: int | None = None,
        max_price_steps: int | None = None,
        min_tick_size: float | None = None,
        max_tick_size: float | None = None,
        time_unit: str | list[str] | None = None,
        time_unit_prefix: str | None = None,
        min_trade_lot: float | None = None,
        max_trade_lot: float | None = None,
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
        group_by: MarketConfigurationFields | SequenceNotStr[MarketConfigurationFields],
        property: MarketConfigurationFields | SequenceNotStr[MarketConfigurationFields] | None = None,
        query: str | None = None,
        search_property: MarketConfigurationTextFields | SequenceNotStr[MarketConfigurationTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_max_price: float | None = None,
        max_max_price: float | None = None,
        min_min_price: float | None = None,
        max_min_price: float | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        price_unit: str | list[str] | None = None,
        price_unit_prefix: str | None = None,
        min_price_steps: int | None = None,
        max_price_steps: int | None = None,
        min_tick_size: float | None = None,
        max_tick_size: float | None = None,
        time_unit: str | list[str] | None = None,
        time_unit_prefix: str | None = None,
        min_trade_lot: float | None = None,
        max_trade_lot: float | None = None,
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
        group_by: MarketConfigurationFields | SequenceNotStr[MarketConfigurationFields] | None = None,
        property: MarketConfigurationFields | SequenceNotStr[MarketConfigurationFields] | None = None,
        query: str | None = None,
        search_property: MarketConfigurationTextFields | SequenceNotStr[MarketConfigurationTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_max_price: float | None = None,
        max_max_price: float | None = None,
        min_min_price: float | None = None,
        max_min_price: float | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        price_unit: str | list[str] | None = None,
        price_unit_prefix: str | None = None,
        min_price_steps: int | None = None,
        max_price_steps: int | None = None,
        min_tick_size: float | None = None,
        max_tick_size: float | None = None,
        time_unit: str | list[str] | None = None,
        time_unit_prefix: str | None = None,
        min_trade_lot: float | None = None,
        max_trade_lot: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across market configurations

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_max_price: The minimum value of the max price to filter on.
            max_max_price: The maximum value of the max price to filter on.
            min_min_price: The minimum value of the min price to filter on.
            max_min_price: The maximum value of the min price to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            price_unit: The price unit to filter on.
            price_unit_prefix: The prefix of the price unit to filter on.
            min_price_steps: The minimum value of the price step to filter on.
            max_price_steps: The maximum value of the price step to filter on.
            min_tick_size: The minimum value of the tick size to filter on.
            max_tick_size: The maximum value of the tick size to filter on.
            time_unit: The time unit to filter on.
            time_unit_prefix: The prefix of the time unit to filter on.
            min_trade_lot: The minimum value of the trade lot to filter on.
            max_trade_lot: The maximum value of the trade lot to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of market configurations to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count market configurations in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.market_configuration.aggregate("count", space="my_space")

        """

        filter_ = _create_market_configuration_filter(
            self._view_id,
            name,
            name_prefix,
            min_max_price,
            max_max_price,
            min_min_price,
            max_min_price,
            timezone,
            timezone_prefix,
            price_unit,
            price_unit_prefix,
            min_price_steps,
            max_price_steps,
            min_tick_size,
            max_tick_size,
            time_unit,
            time_unit_prefix,
            min_trade_lot,
            max_trade_lot,
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
        property: MarketConfigurationFields,
        interval: float,
        query: str | None = None,
        search_property: MarketConfigurationTextFields | SequenceNotStr[MarketConfigurationTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_max_price: float | None = None,
        max_max_price: float | None = None,
        min_min_price: float | None = None,
        max_min_price: float | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        price_unit: str | list[str] | None = None,
        price_unit_prefix: str | None = None,
        min_price_steps: int | None = None,
        max_price_steps: int | None = None,
        min_tick_size: float | None = None,
        max_tick_size: float | None = None,
        time_unit: str | list[str] | None = None,
        time_unit_prefix: str | None = None,
        min_trade_lot: float | None = None,
        max_trade_lot: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for market configurations

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_max_price: The minimum value of the max price to filter on.
            max_max_price: The maximum value of the max price to filter on.
            min_min_price: The minimum value of the min price to filter on.
            max_min_price: The maximum value of the min price to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            price_unit: The price unit to filter on.
            price_unit_prefix: The prefix of the price unit to filter on.
            min_price_steps: The minimum value of the price step to filter on.
            max_price_steps: The maximum value of the price step to filter on.
            min_tick_size: The minimum value of the tick size to filter on.
            max_tick_size: The maximum value of the tick size to filter on.
            time_unit: The time unit to filter on.
            time_unit_prefix: The prefix of the time unit to filter on.
            min_trade_lot: The minimum value of the trade lot to filter on.
            max_trade_lot: The maximum value of the trade lot to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of market configurations to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_market_configuration_filter(
            self._view_id,
            name,
            name_prefix,
            min_max_price,
            max_max_price,
            min_min_price,
            max_min_price,
            timezone,
            timezone_prefix,
            price_unit,
            price_unit_prefix,
            min_price_steps,
            max_price_steps,
            min_tick_size,
            max_tick_size,
            time_unit,
            time_unit_prefix,
            min_trade_lot,
            max_trade_lot,
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

    def select(self) -> MarketConfigurationQuery:
        """Start selecting from market configurations."""
        return MarketConfigurationQuery(self._client)

    def _build(
        self,
        filter_: dm.Filter | None,
        limit: int | None,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
        chunk_size: int | None = None,
    ) -> QueryExecutor:
        builder = QueryBuilder()
        factory = QueryBuildStepFactory(builder.create_name, view_id=self._view_id, edge_connection_property="end_node")
        builder.append(factory.root(
            filter=filter_,
            sort=sort,
            limit=limit,
            max_retrieve_batch_limit=chunk_size,
            has_container_fields=True,
        ))
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_max_price: float | None = None,
        max_max_price: float | None = None,
        min_min_price: float | None = None,
        max_min_price: float | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        price_unit: str | list[str] | None = None,
        price_unit_prefix: str | None = None,
        min_price_steps: int | None = None,
        max_price_steps: int | None = None,
        min_tick_size: float | None = None,
        max_tick_size: float | None = None,
        time_unit: str | list[str] | None = None,
        time_unit_prefix: str | None = None,
        min_trade_lot: float | None = None,
        max_trade_lot: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[MarketConfigurationList]:
        """Iterate over market configurations

        Args:
            chunk_size: The number of market configurations to return in each iteration. Defaults to 100.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_max_price: The minimum value of the max price to filter on.
            max_max_price: The maximum value of the max price to filter on.
            min_min_price: The minimum value of the min price to filter on.
            max_min_price: The maximum value of the min price to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            price_unit: The price unit to filter on.
            price_unit_prefix: The prefix of the price unit to filter on.
            min_price_steps: The minimum value of the price step to filter on.
            max_price_steps: The maximum value of the price step to filter on.
            min_tick_size: The minimum value of the tick size to filter on.
            max_tick_size: The maximum value of the tick size to filter on.
            time_unit: The time unit to filter on.
            time_unit_prefix: The prefix of the time unit to filter on.
            min_trade_lot: The minimum value of the trade lot to filter on.
            max_trade_lot: The maximum value of the trade lot to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of market configurations to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of market configurations

        Examples:

            Iterate market configurations in chunks of 100 up to 2000 items:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for market_configurations in client.market_configuration.iterate(chunk_size=100, limit=2000):
                ...     for market_configuration in market_configurations:
                ...         print(market_configuration.external_id)

            Iterate market configurations in chunks of 100 sorted by external_id in descending order:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for market_configurations in client.market_configuration.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for market_configuration in market_configurations:
                ...         print(market_configuration.external_id)

            Iterate market configurations in chunks of 100 and use cursors to resume the iteration:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for first_iteration in client.market_configuration.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for market_configurations in client.market_configuration.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for market_configuration in market_configurations:
                ...         print(market_configuration.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_market_configuration_filter(
            self._view_id,
            name,
            name_prefix,
            min_max_price,
            max_max_price,
            min_min_price,
            max_min_price,
            timezone,
            timezone_prefix,
            price_unit,
            price_unit_prefix,
            min_price_steps,
            max_price_steps,
            min_tick_size,
            max_tick_size,
            time_unit,
            time_unit_prefix,
            min_trade_lot,
            max_trade_lot,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, "skip", cursors=cursors)

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        min_max_price: float | None = None,
        max_max_price: float | None = None,
        min_min_price: float | None = None,
        max_min_price: float | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        price_unit: str | list[str] | None = None,
        price_unit_prefix: str | None = None,
        min_price_steps: int | None = None,
        max_price_steps: int | None = None,
        min_tick_size: float | None = None,
        max_tick_size: float | None = None,
        time_unit: str | list[str] | None = None,
        time_unit_prefix: str | None = None,
        min_trade_lot: float | None = None,
        max_trade_lot: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: MarketConfigurationFields | Sequence[MarketConfigurationFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> MarketConfigurationList:
        """List/filter market configurations

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            min_max_price: The minimum value of the max price to filter on.
            max_max_price: The maximum value of the max price to filter on.
            min_min_price: The minimum value of the min price to filter on.
            max_min_price: The maximum value of the min price to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            price_unit: The price unit to filter on.
            price_unit_prefix: The prefix of the price unit to filter on.
            min_price_steps: The minimum value of the price step to filter on.
            max_price_steps: The maximum value of the price step to filter on.
            min_tick_size: The minimum value of the tick size to filter on.
            max_tick_size: The maximum value of the tick size to filter on.
            time_unit: The time unit to filter on.
            time_unit_prefix: The prefix of the time unit to filter on.
            min_trade_lot: The minimum value of the trade lot to filter on.
            max_trade_lot: The maximum value of the trade lot to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of market configurations to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested market configurations

        Examples:

            List market configurations and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> market_configurations = client.market_configuration.list(limit=5)

        """
        filter_ = _create_market_configuration_filter(
            self._view_id,
            name,
            name_prefix,
            min_max_price,
            max_max_price,
            min_min_price,
            max_min_price,
            timezone,
            timezone_prefix,
            price_unit,
            price_unit_prefix,
            min_price_steps,
            max_price_steps,
            min_tick_size,
            max_tick_size,
            time_unit,
            time_unit_prefix,
            min_trade_lot,
            max_trade_lot,
            external_id_prefix,
            space,
            filter,
        )
        sort_input =  self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        return self._list(limit=limit,  filter=filter_, sort=sort_input)
