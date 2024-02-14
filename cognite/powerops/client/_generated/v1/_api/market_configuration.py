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
    MarketConfiguration,
    MarketConfigurationWrite,
    MarketConfigurationFields,
    MarketConfigurationList,
    MarketConfigurationWriteList,
    MarketConfigurationTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._market_configuration import (
    _MARKETCONFIGURATION_PROPERTIES_BY_FIELD,
    _create_market_configuration_filter,
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
from .market_configuration_query import MarketConfigurationQueryAPI


class MarketConfigurationAPI(NodeAPI[MarketConfiguration, MarketConfigurationWrite, MarketConfigurationList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[MarketConfiguration]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=MarketConfiguration,
            class_list=MarketConfigurationList,
            class_write_list=MarketConfigurationWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        market_type: str | list[str] | None = None,
        market_type_prefix: str | None = None,
        min_max_price: float | None = None,
        max_max_price: float | None = None,
        min_min_price: float | None = None,
        max_min_price: float | None = None,
        time_zone: str | list[str] | None = None,
        time_zone_prefix: str | None = None,
        price_unit: str | list[str] | None = None,
        price_unit_prefix: str | None = None,
        price_steps: str | list[str] | None = None,
        price_steps_prefix: str | None = None,
        tick_size: str | list[str] | None = None,
        tick_size_prefix: str | None = None,
        time_unit: str | list[str] | None = None,
        time_unit_prefix: str | None = None,
        trade_lot: str | list[str] | None = None,
        trade_lot_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> MarketConfigurationQueryAPI[MarketConfigurationList]:
        """Query starting at market configurations.

        Args:
            market_type: The market type to filter on.
            market_type_prefix: The prefix of the market type to filter on.
            min_max_price: The minimum value of the max price to filter on.
            max_max_price: The maximum value of the max price to filter on.
            min_min_price: The minimum value of the min price to filter on.
            max_min_price: The maximum value of the min price to filter on.
            time_zone: The time zone to filter on.
            time_zone_prefix: The prefix of the time zone to filter on.
            price_unit: The price unit to filter on.
            price_unit_prefix: The prefix of the price unit to filter on.
            price_steps: The price step to filter on.
            price_steps_prefix: The prefix of the price step to filter on.
            tick_size: The tick size to filter on.
            tick_size_prefix: The prefix of the tick size to filter on.
            time_unit: The time unit to filter on.
            time_unit_prefix: The prefix of the time unit to filter on.
            trade_lot: The trade lot to filter on.
            trade_lot_prefix: The prefix of the trade lot to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of market configurations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for market configurations.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_market_configuration_filter(
            self._view_id,
            market_type,
            market_type_prefix,
            min_max_price,
            max_max_price,
            min_min_price,
            max_min_price,
            time_zone,
            time_zone_prefix,
            price_unit,
            price_unit_prefix,
            price_steps,
            price_steps_prefix,
            tick_size,
            tick_size_prefix,
            time_unit,
            time_unit_prefix,
            trade_lot,
            trade_lot_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(MarketConfigurationList)
        return MarketConfigurationQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        market_configuration: MarketConfigurationWrite | Sequence[MarketConfigurationWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) market configurations.

        Args:
            market_configuration: Market configuration or sequence of market configurations to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new market_configuration:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import MarketConfigurationWrite
                >>> client = PowerOpsModelsV1Client()
                >>> market_configuration = MarketConfigurationWrite(external_id="my_market_configuration", ...)
                >>> result = client.market_configuration.apply(market_configuration)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.market_configuration.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(market_configuration, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more market configuration.

        Args:
            external_id: External id of the market configuration to delete.
            space: The space where all the market configuration are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete market_configuration by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.market_configuration.delete("my_market_configuration")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.market_configuration.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> MarketConfiguration | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> MarketConfigurationList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
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
                >>> market_configuration = client.market_configuration.retrieve("my_market_configuration")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: MarketConfigurationTextFields | Sequence[MarketConfigurationTextFields] | None = None,
        market_type: str | list[str] | None = None,
        market_type_prefix: str | None = None,
        min_max_price: float | None = None,
        max_max_price: float | None = None,
        min_min_price: float | None = None,
        max_min_price: float | None = None,
        time_zone: str | list[str] | None = None,
        time_zone_prefix: str | None = None,
        price_unit: str | list[str] | None = None,
        price_unit_prefix: str | None = None,
        price_steps: str | list[str] | None = None,
        price_steps_prefix: str | None = None,
        tick_size: str | list[str] | None = None,
        tick_size_prefix: str | None = None,
        time_unit: str | list[str] | None = None,
        time_unit_prefix: str | None = None,
        trade_lot: str | list[str] | None = None,
        trade_lot_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> MarketConfigurationList:
        """Search market configurations

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            market_type: The market type to filter on.
            market_type_prefix: The prefix of the market type to filter on.
            min_max_price: The minimum value of the max price to filter on.
            max_max_price: The maximum value of the max price to filter on.
            min_min_price: The minimum value of the min price to filter on.
            max_min_price: The maximum value of the min price to filter on.
            time_zone: The time zone to filter on.
            time_zone_prefix: The prefix of the time zone to filter on.
            price_unit: The price unit to filter on.
            price_unit_prefix: The prefix of the price unit to filter on.
            price_steps: The price step to filter on.
            price_steps_prefix: The prefix of the price step to filter on.
            tick_size: The tick size to filter on.
            tick_size_prefix: The prefix of the tick size to filter on.
            time_unit: The time unit to filter on.
            time_unit_prefix: The prefix of the time unit to filter on.
            trade_lot: The trade lot to filter on.
            trade_lot_prefix: The prefix of the trade lot to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of market configurations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results market configurations matching the query.

        Examples:

           Search for 'my_market_configuration' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> market_configurations = client.market_configuration.search('my_market_configuration')

        """
        filter_ = _create_market_configuration_filter(
            self._view_id,
            market_type,
            market_type_prefix,
            min_max_price,
            max_max_price,
            min_min_price,
            max_min_price,
            time_zone,
            time_zone_prefix,
            price_unit,
            price_unit_prefix,
            price_steps,
            price_steps_prefix,
            tick_size,
            tick_size_prefix,
            time_unit,
            time_unit_prefix,
            trade_lot,
            trade_lot_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _MARKETCONFIGURATION_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: MarketConfigurationFields | Sequence[MarketConfigurationFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: MarketConfigurationTextFields | Sequence[MarketConfigurationTextFields] | None = None,
        market_type: str | list[str] | None = None,
        market_type_prefix: str | None = None,
        min_max_price: float | None = None,
        max_max_price: float | None = None,
        min_min_price: float | None = None,
        max_min_price: float | None = None,
        time_zone: str | list[str] | None = None,
        time_zone_prefix: str | None = None,
        price_unit: str | list[str] | None = None,
        price_unit_prefix: str | None = None,
        price_steps: str | list[str] | None = None,
        price_steps_prefix: str | None = None,
        tick_size: str | list[str] | None = None,
        tick_size_prefix: str | None = None,
        time_unit: str | list[str] | None = None,
        time_unit_prefix: str | None = None,
        trade_lot: str | list[str] | None = None,
        trade_lot_prefix: str | None = None,
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
        property: MarketConfigurationFields | Sequence[MarketConfigurationFields] | None = None,
        group_by: MarketConfigurationFields | Sequence[MarketConfigurationFields] = None,
        query: str | None = None,
        search_properties: MarketConfigurationTextFields | Sequence[MarketConfigurationTextFields] | None = None,
        market_type: str | list[str] | None = None,
        market_type_prefix: str | None = None,
        min_max_price: float | None = None,
        max_max_price: float | None = None,
        min_min_price: float | None = None,
        max_min_price: float | None = None,
        time_zone: str | list[str] | None = None,
        time_zone_prefix: str | None = None,
        price_unit: str | list[str] | None = None,
        price_unit_prefix: str | None = None,
        price_steps: str | list[str] | None = None,
        price_steps_prefix: str | None = None,
        tick_size: str | list[str] | None = None,
        tick_size_prefix: str | None = None,
        time_unit: str | list[str] | None = None,
        time_unit_prefix: str | None = None,
        trade_lot: str | list[str] | None = None,
        trade_lot_prefix: str | None = None,
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
        property: MarketConfigurationFields | Sequence[MarketConfigurationFields] | None = None,
        group_by: MarketConfigurationFields | Sequence[MarketConfigurationFields] | None = None,
        query: str | None = None,
        search_property: MarketConfigurationTextFields | Sequence[MarketConfigurationTextFields] | None = None,
        market_type: str | list[str] | None = None,
        market_type_prefix: str | None = None,
        min_max_price: float | None = None,
        max_max_price: float | None = None,
        min_min_price: float | None = None,
        max_min_price: float | None = None,
        time_zone: str | list[str] | None = None,
        time_zone_prefix: str | None = None,
        price_unit: str | list[str] | None = None,
        price_unit_prefix: str | None = None,
        price_steps: str | list[str] | None = None,
        price_steps_prefix: str | None = None,
        tick_size: str | list[str] | None = None,
        tick_size_prefix: str | None = None,
        time_unit: str | list[str] | None = None,
        time_unit_prefix: str | None = None,
        trade_lot: str | list[str] | None = None,
        trade_lot_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across market configurations

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            market_type: The market type to filter on.
            market_type_prefix: The prefix of the market type to filter on.
            min_max_price: The minimum value of the max price to filter on.
            max_max_price: The maximum value of the max price to filter on.
            min_min_price: The minimum value of the min price to filter on.
            max_min_price: The maximum value of the min price to filter on.
            time_zone: The time zone to filter on.
            time_zone_prefix: The prefix of the time zone to filter on.
            price_unit: The price unit to filter on.
            price_unit_prefix: The prefix of the price unit to filter on.
            price_steps: The price step to filter on.
            price_steps_prefix: The prefix of the price step to filter on.
            tick_size: The tick size to filter on.
            tick_size_prefix: The prefix of the tick size to filter on.
            time_unit: The time unit to filter on.
            time_unit_prefix: The prefix of the time unit to filter on.
            trade_lot: The trade lot to filter on.
            trade_lot_prefix: The prefix of the trade lot to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of market configurations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

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
            market_type,
            market_type_prefix,
            min_max_price,
            max_max_price,
            min_min_price,
            max_min_price,
            time_zone,
            time_zone_prefix,
            price_unit,
            price_unit_prefix,
            price_steps,
            price_steps_prefix,
            tick_size,
            tick_size_prefix,
            time_unit,
            time_unit_prefix,
            trade_lot,
            trade_lot_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _MARKETCONFIGURATION_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: MarketConfigurationFields,
        interval: float,
        query: str | None = None,
        search_property: MarketConfigurationTextFields | Sequence[MarketConfigurationTextFields] | None = None,
        market_type: str | list[str] | None = None,
        market_type_prefix: str | None = None,
        min_max_price: float | None = None,
        max_max_price: float | None = None,
        min_min_price: float | None = None,
        max_min_price: float | None = None,
        time_zone: str | list[str] | None = None,
        time_zone_prefix: str | None = None,
        price_unit: str | list[str] | None = None,
        price_unit_prefix: str | None = None,
        price_steps: str | list[str] | None = None,
        price_steps_prefix: str | None = None,
        tick_size: str | list[str] | None = None,
        tick_size_prefix: str | None = None,
        time_unit: str | list[str] | None = None,
        time_unit_prefix: str | None = None,
        trade_lot: str | list[str] | None = None,
        trade_lot_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for market configurations

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            market_type: The market type to filter on.
            market_type_prefix: The prefix of the market type to filter on.
            min_max_price: The minimum value of the max price to filter on.
            max_max_price: The maximum value of the max price to filter on.
            min_min_price: The minimum value of the min price to filter on.
            max_min_price: The maximum value of the min price to filter on.
            time_zone: The time zone to filter on.
            time_zone_prefix: The prefix of the time zone to filter on.
            price_unit: The price unit to filter on.
            price_unit_prefix: The prefix of the price unit to filter on.
            price_steps: The price step to filter on.
            price_steps_prefix: The prefix of the price step to filter on.
            tick_size: The tick size to filter on.
            tick_size_prefix: The prefix of the tick size to filter on.
            time_unit: The time unit to filter on.
            time_unit_prefix: The prefix of the time unit to filter on.
            trade_lot: The trade lot to filter on.
            trade_lot_prefix: The prefix of the trade lot to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of market configurations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_market_configuration_filter(
            self._view_id,
            market_type,
            market_type_prefix,
            min_max_price,
            max_max_price,
            min_min_price,
            max_min_price,
            time_zone,
            time_zone_prefix,
            price_unit,
            price_unit_prefix,
            price_steps,
            price_steps_prefix,
            tick_size,
            tick_size_prefix,
            time_unit,
            time_unit_prefix,
            trade_lot,
            trade_lot_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _MARKETCONFIGURATION_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        market_type: str | list[str] | None = None,
        market_type_prefix: str | None = None,
        min_max_price: float | None = None,
        max_max_price: float | None = None,
        min_min_price: float | None = None,
        max_min_price: float | None = None,
        time_zone: str | list[str] | None = None,
        time_zone_prefix: str | None = None,
        price_unit: str | list[str] | None = None,
        price_unit_prefix: str | None = None,
        price_steps: str | list[str] | None = None,
        price_steps_prefix: str | None = None,
        tick_size: str | list[str] | None = None,
        tick_size_prefix: str | None = None,
        time_unit: str | list[str] | None = None,
        time_unit_prefix: str | None = None,
        trade_lot: str | list[str] | None = None,
        trade_lot_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> MarketConfigurationList:
        """List/filter market configurations

        Args:
            market_type: The market type to filter on.
            market_type_prefix: The prefix of the market type to filter on.
            min_max_price: The minimum value of the max price to filter on.
            max_max_price: The maximum value of the max price to filter on.
            min_min_price: The minimum value of the min price to filter on.
            max_min_price: The maximum value of the min price to filter on.
            time_zone: The time zone to filter on.
            time_zone_prefix: The prefix of the time zone to filter on.
            price_unit: The price unit to filter on.
            price_unit_prefix: The prefix of the price unit to filter on.
            price_steps: The price step to filter on.
            price_steps_prefix: The prefix of the price step to filter on.
            tick_size: The tick size to filter on.
            tick_size_prefix: The prefix of the tick size to filter on.
            time_unit: The time unit to filter on.
            time_unit_prefix: The prefix of the time unit to filter on.
            trade_lot: The trade lot to filter on.
            trade_lot_prefix: The prefix of the trade lot to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of market configurations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

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
            market_type,
            market_type_prefix,
            min_max_price,
            max_max_price,
            min_min_price,
            max_min_price,
            time_zone,
            time_zone_prefix,
            price_unit,
            price_unit_prefix,
            price_steps,
            price_steps_prefix,
            tick_size,
            tick_size_prefix,
            time_unit,
            time_unit_prefix,
            trade_lot,
            trade_lot_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
