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
    PriceProduction,
    PriceProductionWrite,
    PriceProductionFields,
    PriceProductionList,
    PriceProductionWriteList,
    PriceProductionTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._price_production import (
    _PRICEPRODUCTION_PROPERTIES_BY_FIELD,
    _create_price_production_filter,
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
from .price_production_price import PriceProductionPriceAPI
from .price_production_production import PriceProductionProductionAPI
from .price_production_query import PriceProductionQueryAPI


class PriceProductionAPI(NodeAPI[PriceProduction, PriceProductionWrite, PriceProductionList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[PriceProduction]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=PriceProduction,
            class_list=PriceProductionList,
            class_write_list=PriceProductionWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.price = PriceProductionPriceAPI(client, view_id)
        self.production = PriceProductionProductionAPI(client, view_id)

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        shop_result: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> PriceProductionQueryAPI[PriceProductionList]:
        """Query starting at price productions.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            shop_result: The shop result to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of price productions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for price productions.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_price_production_filter(
            self._view_id,
            name,
            name_prefix,
            shop_result,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(PriceProductionList)
        return PriceProductionQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        price_production: PriceProductionWrite | Sequence[PriceProductionWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) price productions.

        Args:
            price_production: Price production or sequence of price productions to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new price_production:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import PriceProductionWrite
                >>> client = PowerOpsModelsV1Client()
                >>> price_production = PriceProductionWrite(external_id="my_price_production", ...)
                >>> result = client.price_production.apply(price_production)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.price_production.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(price_production, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more price production.

        Args:
            external_id: External id of the price production to delete.
            space: The space where all the price production are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete price_production by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.price_production.delete("my_price_production")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.price_production.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> PriceProduction | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> PriceProductionList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> PriceProduction | PriceProductionList | None:
        """Retrieve one or more price productions by id(s).

        Args:
            external_id: External id or list of external ids of the price productions.
            space: The space where all the price productions are located.

        Returns:
            The requested price productions.

        Examples:

            Retrieve price_production by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> price_production = client.price_production.retrieve("my_price_production")

        """
        return self._retrieve(external_id, space)

    def search(
        self,
        query: str,
        properties: PriceProductionTextFields | Sequence[PriceProductionTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        shop_result: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PriceProductionList:
        """Search price productions

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            shop_result: The shop result to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of price productions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results price productions matching the query.

        Examples:

           Search for 'my_price_production' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> price_productions = client.price_production.search('my_price_production')

        """
        filter_ = _create_price_production_filter(
            self._view_id,
            name,
            name_prefix,
            shop_result,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _PRICEPRODUCTION_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: PriceProductionFields | Sequence[PriceProductionFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: PriceProductionTextFields | Sequence[PriceProductionTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        shop_result: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: PriceProductionFields | Sequence[PriceProductionFields] | None = None,
        group_by: PriceProductionFields | Sequence[PriceProductionFields] = None,
        query: str | None = None,
        search_properties: PriceProductionTextFields | Sequence[PriceProductionTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        shop_result: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: PriceProductionFields | Sequence[PriceProductionFields] | None = None,
        group_by: PriceProductionFields | Sequence[PriceProductionFields] | None = None,
        query: str | None = None,
        search_property: PriceProductionTextFields | Sequence[PriceProductionTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        shop_result: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across price productions

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            shop_result: The shop result to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of price productions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count price productions in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.price_production.aggregate("count", space="my_space")

        """

        filter_ = _create_price_production_filter(
            self._view_id,
            name,
            name_prefix,
            shop_result,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _PRICEPRODUCTION_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: PriceProductionFields,
        interval: float,
        query: str | None = None,
        search_property: PriceProductionTextFields | Sequence[PriceProductionTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        shop_result: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for price productions

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            shop_result: The shop result to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of price productions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_price_production_filter(
            self._view_id,
            name,
            name_prefix,
            shop_result,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _PRICEPRODUCTION_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        shop_result: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PriceProductionList:
        """List/filter price productions

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            shop_result: The shop result to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of price productions to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested price productions

        Examples:

            List price productions and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> price_productions = client.price_production.list(limit=5)

        """
        filter_ = _create_price_production_filter(
            self._view_id,
            name,
            name_prefix,
            shop_result,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
