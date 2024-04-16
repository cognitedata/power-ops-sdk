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
    ShopBasedPartialBidConfiguration,
    ShopBasedPartialBidConfigurationWrite,
    ShopBasedPartialBidConfigurationFields,
    ShopBasedPartialBidConfigurationList,
    ShopBasedPartialBidConfigurationWriteList,
    ShopBasedPartialBidConfigurationTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._shop_based_partial_bid_configuration import (
    _SHOPBASEDPARTIALBIDCONFIGURATION_PROPERTIES_BY_FIELD,
    _create_shop_based_partial_bid_configuration_filter,
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
from .shop_based_partial_bid_configuration_query import ShopBasedPartialBidConfigurationQueryAPI


class ShopBasedPartialBidConfigurationAPI(
    NodeAPI[
        ShopBasedPartialBidConfiguration, ShopBasedPartialBidConfigurationWrite, ShopBasedPartialBidConfigurationList
    ]
):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[ShopBasedPartialBidConfiguration]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=ShopBasedPartialBidConfiguration,
            class_list=ShopBasedPartialBidConfigurationList,
            class_write_list=ShopBasedPartialBidConfigurationWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        add_steps: bool | None = None,
        scenario_set: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> ShopBasedPartialBidConfigurationQueryAPI[ShopBasedPartialBidConfigurationList]:
        """Query starting at shop based partial bid configurations.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            method: The method to filter on.
            method_prefix: The prefix of the method to filter on.
            power_asset: The power asset to filter on.
            add_steps: The add step to filter on.
            scenario_set: The scenario set to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop based partial bid configurations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for shop based partial bid configurations.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_shop_based_partial_bid_configuration_filter(
            self._view_id,
            name,
            name_prefix,
            method,
            method_prefix,
            power_asset,
            add_steps,
            scenario_set,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(ShopBasedPartialBidConfigurationList)
        return ShopBasedPartialBidConfigurationQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        shop_based_partial_bid_configuration: (
            ShopBasedPartialBidConfigurationWrite | Sequence[ShopBasedPartialBidConfigurationWrite]
        ),
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) shop based partial bid configurations.

        Args:
            shop_based_partial_bid_configuration: Shop based partial bid configuration or sequence of shop based partial bid configurations to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new shop_based_partial_bid_configuration:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import ShopBasedPartialBidConfigurationWrite
                >>> client = PowerOpsModelsV1Client()
                >>> shop_based_partial_bid_configuration = ShopBasedPartialBidConfigurationWrite(external_id="my_shop_based_partial_bid_configuration", ...)
                >>> result = client.shop_based_partial_bid_configuration.apply(shop_based_partial_bid_configuration)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.shop_based_partial_bid_configuration.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(shop_based_partial_bid_configuration, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more shop based partial bid configuration.

        Args:
            external_id: External id of the shop based partial bid configuration to delete.
            space: The space where all the shop based partial bid configuration are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete shop_based_partial_bid_configuration by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.shop_based_partial_bid_configuration.delete("my_shop_based_partial_bid_configuration")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.shop_based_partial_bid_configuration.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE
    ) -> ShopBasedPartialBidConfiguration | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> ShopBasedPartialBidConfigurationList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> ShopBasedPartialBidConfiguration | ShopBasedPartialBidConfigurationList | None:
        """Retrieve one or more shop based partial bid configurations by id(s).

        Args:
            external_id: External id or list of external ids of the shop based partial bid configurations.
            space: The space where all the shop based partial bid configurations are located.

        Returns:
            The requested shop based partial bid configurations.

        Examples:

            Retrieve shop_based_partial_bid_configuration by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_based_partial_bid_configuration = client.shop_based_partial_bid_configuration.retrieve("my_shop_based_partial_bid_configuration")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: (
            ShopBasedPartialBidConfigurationTextFields | Sequence[ShopBasedPartialBidConfigurationTextFields] | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        add_steps: bool | None = None,
        scenario_set: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ShopBasedPartialBidConfigurationList:
        """Search shop based partial bid configurations

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            method: The method to filter on.
            method_prefix: The prefix of the method to filter on.
            power_asset: The power asset to filter on.
            add_steps: The add step to filter on.
            scenario_set: The scenario set to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop based partial bid configurations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results shop based partial bid configurations matching the query.

        Examples:

           Search for 'my_shop_based_partial_bid_configuration' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_based_partial_bid_configurations = client.shop_based_partial_bid_configuration.search('my_shop_based_partial_bid_configuration')

        """
        filter_ = _create_shop_based_partial_bid_configuration_filter(
            self._view_id,
            name,
            name_prefix,
            method,
            method_prefix,
            power_asset,
            add_steps,
            scenario_set,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            self._view_id, query, _SHOPBASEDPARTIALBIDCONFIGURATION_PROPERTIES_BY_FIELD, properties, filter_, limit
        )

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: (
            ShopBasedPartialBidConfigurationFields | Sequence[ShopBasedPartialBidConfigurationFields] | None
        ) = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: (
            ShopBasedPartialBidConfigurationTextFields | Sequence[ShopBasedPartialBidConfigurationTextFields] | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        add_steps: bool | None = None,
        scenario_set: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: (
            ShopBasedPartialBidConfigurationFields | Sequence[ShopBasedPartialBidConfigurationFields] | None
        ) = None,
        group_by: ShopBasedPartialBidConfigurationFields | Sequence[ShopBasedPartialBidConfigurationFields] = None,
        query: str | None = None,
        search_properties: (
            ShopBasedPartialBidConfigurationTextFields | Sequence[ShopBasedPartialBidConfigurationTextFields] | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        add_steps: bool | None = None,
        scenario_set: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: (
            ShopBasedPartialBidConfigurationFields | Sequence[ShopBasedPartialBidConfigurationFields] | None
        ) = None,
        group_by: (
            ShopBasedPartialBidConfigurationFields | Sequence[ShopBasedPartialBidConfigurationFields] | None
        ) = None,
        query: str | None = None,
        search_property: (
            ShopBasedPartialBidConfigurationTextFields | Sequence[ShopBasedPartialBidConfigurationTextFields] | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        add_steps: bool | None = None,
        scenario_set: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across shop based partial bid configurations

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            method: The method to filter on.
            method_prefix: The prefix of the method to filter on.
            power_asset: The power asset to filter on.
            add_steps: The add step to filter on.
            scenario_set: The scenario set to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop based partial bid configurations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count shop based partial bid configurations in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.shop_based_partial_bid_configuration.aggregate("count", space="my_space")

        """

        filter_ = _create_shop_based_partial_bid_configuration_filter(
            self._view_id,
            name,
            name_prefix,
            method,
            method_prefix,
            power_asset,
            add_steps,
            scenario_set,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _SHOPBASEDPARTIALBIDCONFIGURATION_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: ShopBasedPartialBidConfigurationFields,
        interval: float,
        query: str | None = None,
        search_property: (
            ShopBasedPartialBidConfigurationTextFields | Sequence[ShopBasedPartialBidConfigurationTextFields] | None
        ) = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        add_steps: bool | None = None,
        scenario_set: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for shop based partial bid configurations

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            method: The method to filter on.
            method_prefix: The prefix of the method to filter on.
            power_asset: The power asset to filter on.
            add_steps: The add step to filter on.
            scenario_set: The scenario set to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop based partial bid configurations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_shop_based_partial_bid_configuration_filter(
            self._view_id,
            name,
            name_prefix,
            method,
            method_prefix,
            power_asset,
            add_steps,
            scenario_set,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _SHOPBASEDPARTIALBIDCONFIGURATION_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | list[str] | None = None,
        method_prefix: str | None = None,
        power_asset: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        add_steps: bool | None = None,
        scenario_set: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ShopBasedPartialBidConfigurationList:
        """List/filter shop based partial bid configurations

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            method: The method to filter on.
            method_prefix: The prefix of the method to filter on.
            power_asset: The power asset to filter on.
            add_steps: The add step to filter on.
            scenario_set: The scenario set to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop based partial bid configurations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested shop based partial bid configurations

        Examples:

            List shop based partial bid configurations and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_based_partial_bid_configurations = client.shop_based_partial_bid_configuration.list(limit=5)

        """
        filter_ = _create_shop_based_partial_bid_configuration_filter(
            self._view_id,
            name,
            name_prefix,
            method,
            method_prefix,
            power_asset,
            add_steps,
            scenario_set,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
