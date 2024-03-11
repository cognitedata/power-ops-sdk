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
    BidConfigurationShop,
    BidConfigurationShopWrite,
    BidConfigurationShopFields,
    BidConfigurationShopList,
    BidConfigurationShopWriteList,
    BidConfigurationShopTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._bid_configuration_shop import (
    _BIDCONFIGURATIONSHOP_PROPERTIES_BY_FIELD,
    _create_bid_configuration_shop_filter,
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
from .bid_configuration_shop_plants_shop import BidConfigurationShopPlantsShopAPI
from .bid_configuration_shop_watercourses_shop import BidConfigurationShopWatercoursesShopAPI
from .bid_configuration_shop_query import BidConfigurationShopQueryAPI


class BidConfigurationShopAPI(NodeAPI[BidConfigurationShop, BidConfigurationShopWrite, BidConfigurationShopList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[BidConfigurationShop]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=BidConfigurationShop,
            class_list=BidConfigurationShopList,
            class_write_list=BidConfigurationShopWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.plants_shop_edge = BidConfigurationShopPlantsShopAPI(client)
        self.watercourses_shop_edge = BidConfigurationShopWatercoursesShopAPI(client)

    def __call__(
        self,
        market_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> BidConfigurationShopQueryAPI[BidConfigurationShopList]:
        """Query starting at bid configuration shops.

        Args:
            market_configuration: The market configuration to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            method: The method to filter on.
            price_area: The price area to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid configuration shops to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for bid configuration shops.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_bid_configuration_shop_filter(
            self._view_id,
            market_configuration,
            name,
            name_prefix,
            method,
            price_area,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(BidConfigurationShopList)
        return BidConfigurationShopQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        bid_configuration_shop: BidConfigurationShopWrite | Sequence[BidConfigurationShopWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) bid configuration shops.

        Note: This method iterates through all nodes and timeseries linked to bid_configuration_shop and creates them including the edges
        between the nodes. For example, if any of `plants_shop` or `watercourses_shop` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            bid_configuration_shop: Bid configuration shop or sequence of bid configuration shops to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new bid_configuration_shop:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import BidConfigurationShopWrite
                >>> client = PowerOpsModelsV1Client()
                >>> bid_configuration_shop = BidConfigurationShopWrite(external_id="my_bid_configuration_shop", ...)
                >>> result = client.bid_configuration_shop.apply(bid_configuration_shop)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.bid_configuration_shop.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(bid_configuration_shop, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more bid configuration shop.

        Args:
            external_id: External id of the bid configuration shop to delete.
            space: The space where all the bid configuration shop are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete bid_configuration_shop by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.bid_configuration_shop.delete("my_bid_configuration_shop")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.bid_configuration_shop.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> BidConfigurationShop | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> BidConfigurationShopList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> BidConfigurationShop | BidConfigurationShopList | None:
        """Retrieve one or more bid configuration shops by id(s).

        Args:
            external_id: External id or list of external ids of the bid configuration shops.
            space: The space where all the bid configuration shops are located.

        Returns:
            The requested bid configuration shops.

        Examples:

            Retrieve bid_configuration_shop by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_configuration_shop = client.bid_configuration_shop.retrieve("my_bid_configuration_shop")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.plants_shop_edge,
                    "plants_shop",
                    dm.DirectRelationReference("sp_powerops_types", "BidConfiguration.plantsShop"),
                    "outwards",
                    dm.ViewId("sp_powerops_models", "PlantShop", "1"),
                ),
                (
                    self.watercourses_shop_edge,
                    "watercourses_shop",
                    dm.DirectRelationReference("sp_powerops_types", "BidConfiguration.watercoursesShop"),
                    "outwards",
                    dm.ViewId("sp_powerops_models", "WatercourseShop", "1"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: BidConfigurationShopTextFields | Sequence[BidConfigurationShopTextFields] | None = None,
        market_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BidConfigurationShopList:
        """Search bid configuration shops

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            market_configuration: The market configuration to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            method: The method to filter on.
            price_area: The price area to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid configuration shops to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results bid configuration shops matching the query.

        Examples:

           Search for 'my_bid_configuration_shop' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_configuration_shops = client.bid_configuration_shop.search('my_bid_configuration_shop')

        """
        filter_ = _create_bid_configuration_shop_filter(
            self._view_id,
            market_configuration,
            name,
            name_prefix,
            method,
            price_area,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _BIDCONFIGURATIONSHOP_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: BidConfigurationShopFields | Sequence[BidConfigurationShopFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: BidConfigurationShopTextFields | Sequence[BidConfigurationShopTextFields] | None = None,
        market_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: BidConfigurationShopFields | Sequence[BidConfigurationShopFields] | None = None,
        group_by: BidConfigurationShopFields | Sequence[BidConfigurationShopFields] = None,
        query: str | None = None,
        search_properties: BidConfigurationShopTextFields | Sequence[BidConfigurationShopTextFields] | None = None,
        market_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: BidConfigurationShopFields | Sequence[BidConfigurationShopFields] | None = None,
        group_by: BidConfigurationShopFields | Sequence[BidConfigurationShopFields] | None = None,
        query: str | None = None,
        search_property: BidConfigurationShopTextFields | Sequence[BidConfigurationShopTextFields] | None = None,
        market_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across bid configuration shops

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            market_configuration: The market configuration to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            method: The method to filter on.
            price_area: The price area to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid configuration shops to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count bid configuration shops in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.bid_configuration_shop.aggregate("count", space="my_space")

        """

        filter_ = _create_bid_configuration_shop_filter(
            self._view_id,
            market_configuration,
            name,
            name_prefix,
            method,
            price_area,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _BIDCONFIGURATIONSHOP_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: BidConfigurationShopFields,
        interval: float,
        query: str | None = None,
        search_property: BidConfigurationShopTextFields | Sequence[BidConfigurationShopTextFields] | None = None,
        market_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for bid configuration shops

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            market_configuration: The market configuration to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            method: The method to filter on.
            price_area: The price area to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid configuration shops to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_bid_configuration_shop_filter(
            self._view_id,
            market_configuration,
            name,
            name_prefix,
            method,
            price_area,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _BIDCONFIGURATIONSHOP_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        market_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> BidConfigurationShopList:
        """List/filter bid configuration shops

        Args:
            market_configuration: The market configuration to filter on.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            method: The method to filter on.
            price_area: The price area to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid configuration shops to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `plants_shop` or `watercourses_shop` external ids for the bid configuration shops. Defaults to True.

        Returns:
            List of requested bid configuration shops

        Examples:

            List bid configuration shops and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_configuration_shops = client.bid_configuration_shop.list(limit=5)

        """
        filter_ = _create_bid_configuration_shop_filter(
            self._view_id,
            market_configuration,
            name,
            name_prefix,
            method,
            price_area,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            retrieve_edges=retrieve_edges,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.plants_shop_edge,
                    "plants_shop",
                    dm.DirectRelationReference("sp_powerops_types", "BidConfiguration.plantsShop"),
                    "outwards",
                    dm.ViewId("sp_powerops_models", "PlantShop", "1"),
                ),
                (
                    self.watercourses_shop_edge,
                    "watercourses_shop",
                    dm.DirectRelationReference("sp_powerops_types", "BidConfiguration.watercoursesShop"),
                    "outwards",
                    dm.ViewId("sp_powerops_models", "WatercourseShop", "1"),
                ),
            ],
        )
