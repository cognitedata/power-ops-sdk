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
    SHOPResultPriceProd,
    SHOPResultPriceProdWrite,
    SHOPResultPriceProdFields,
    SHOPResultPriceProdList,
    SHOPResultPriceProdWriteList,
)
from cognite.powerops.client._generated.v1.data_classes._shop_result_price_prod import (
    _SHOPRESULTPRICEPROD_PROPERTIES_BY_FIELD,
    _create_shop_result_price_prod_filter,
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
from .shop_result_price_prod_alerts import SHOPResultPriceProdAlertsAPI
from .shop_result_price_prod_production_timeseries import SHOPResultPriceProdProductionTimeseriesAPI
from .shop_result_price_prod_output_timeseries import SHOPResultPriceProdOutputTimeseriesAPI
from .shop_result_price_prod_query import SHOPResultPriceProdQueryAPI


class SHOPResultPriceProdAPI(NodeAPI[SHOPResultPriceProd, SHOPResultPriceProdWrite, SHOPResultPriceProdList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[SHOPResultPriceProd]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=SHOPResultPriceProd,
            class_list=SHOPResultPriceProdList,
            class_write_list=SHOPResultPriceProdWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.alerts_edge = SHOPResultPriceProdAlertsAPI(client)
        self.production_timeseries_edge = SHOPResultPriceProdProductionTimeseriesAPI(client)
        self.output_timeseries = SHOPResultPriceProdOutputTimeseriesAPI(client, view_id)

    def __call__(
        self,
        case: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_timeseries: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> SHOPResultPriceProdQueryAPI[SHOPResultPriceProdList]:
        """Query starting at shop result price prods.

        Args:
            case: The case to filter on.
            price_timeseries: The price timesery to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop result price prods to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for shop result price prods.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_shop_result_price_prod_filter(
            self._view_id,
            case,
            price_timeseries,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(SHOPResultPriceProdList)
        return SHOPResultPriceProdQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        shop_result_price_prod: SHOPResultPriceProdWrite | Sequence[SHOPResultPriceProdWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) shop result price prods.

        Note: This method iterates through all nodes and timeseries linked to shop_result_price_prod and creates them including the edges
        between the nodes. For example, if any of `alerts` or `production_timeseries` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            shop_result_price_prod: Shop result price prod or sequence of shop result price prods to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new shop_result_price_prod:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import SHOPResultPriceProdWrite
                >>> client = PowerOpsModelsV1Client()
                >>> shop_result_price_prod = SHOPResultPriceProdWrite(external_id="my_shop_result_price_prod", ...)
                >>> result = client.shop_result_price_prod.apply(shop_result_price_prod)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.shop_result_price_prod.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(shop_result_price_prod, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more shop result price prod.

        Args:
            external_id: External id of the shop result price prod to delete.
            space: The space where all the shop result price prod are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete shop_result_price_prod by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.shop_result_price_prod.delete("my_shop_result_price_prod")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.shop_result_price_prod.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> SHOPResultPriceProd | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> SHOPResultPriceProdList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> SHOPResultPriceProd | SHOPResultPriceProdList | None:
        """Retrieve one or more shop result price prods by id(s).

        Args:
            external_id: External id or list of external ids of the shop result price prods.
            space: The space where all the shop result price prods are located.

        Returns:
            The requested shop result price prods.

        Examples:

            Retrieve shop_result_price_prod by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_result_price_prod = client.shop_result_price_prod.retrieve("my_shop_result_price_prod")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.alerts_edge,
                    "alerts",
                    dm.DirectRelationReference("sp_powerops_types", "calculationIssue"),
                    "outwards",
                    dm.ViewId("sp_powerops_models", "Alert", "1"),
                ),
                (
                    self.production_timeseries_edge,
                    "production_timeseries",
                    dm.DirectRelationReference("sp_powerops_types", "SHOPResultPriceProd.productionTimeseries"),
                    "outwards",
                    dm.ViewId("sp_powerops_models", "SHOPTimeSeries", "1"),
                ),
            ],
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
        property: SHOPResultPriceProdFields | Sequence[SHOPResultPriceProdFields] | None = None,
        group_by: None = None,
        case: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_timeseries: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: SHOPResultPriceProdFields | Sequence[SHOPResultPriceProdFields] | None = None,
        group_by: SHOPResultPriceProdFields | Sequence[SHOPResultPriceProdFields] = None,
        case: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_timeseries: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: SHOPResultPriceProdFields | Sequence[SHOPResultPriceProdFields] | None = None,
        group_by: SHOPResultPriceProdFields | Sequence[SHOPResultPriceProdFields] | None = None,
        case: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_timeseries: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across shop result price prods

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            case: The case to filter on.
            price_timeseries: The price timesery to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop result price prods to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count shop result price prods in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.shop_result_price_prod.aggregate("count", space="my_space")

        """

        filter_ = _create_shop_result_price_prod_filter(
            self._view_id,
            case,
            price_timeseries,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _SHOPRESULTPRICEPROD_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: SHOPResultPriceProdFields,
        interval: float,
        case: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_timeseries: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for shop result price prods

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            case: The case to filter on.
            price_timeseries: The price timesery to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop result price prods to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_shop_result_price_prod_filter(
            self._view_id,
            case,
            price_timeseries,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _SHOPRESULTPRICEPROD_PROPERTIES_BY_FIELD,
            None,
            None,
            limit,
            filter_,
        )

    def list(
        self,
        case: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_timeseries: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> SHOPResultPriceProdList:
        """List/filter shop result price prods

        Args:
            case: The case to filter on.
            price_timeseries: The price timesery to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop result price prods to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `alerts` or `production_timeseries` external ids for the shop result price prods. Defaults to True.

        Returns:
            List of requested shop result price prods

        Examples:

            List shop result price prods and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_result_price_prods = client.shop_result_price_prod.list(limit=5)

        """
        filter_ = _create_shop_result_price_prod_filter(
            self._view_id,
            case,
            price_timeseries,
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
                    self.alerts_edge,
                    "alerts",
                    dm.DirectRelationReference("sp_powerops_types", "calculationIssue"),
                    "outwards",
                    dm.ViewId("sp_powerops_models", "Alert", "1"),
                ),
                (
                    self.production_timeseries_edge,
                    "production_timeseries",
                    dm.DirectRelationReference("sp_powerops_types", "SHOPResultPriceProd.productionTimeseries"),
                    "outwards",
                    dm.ViewId("sp_powerops_models", "SHOPTimeSeries", "1"),
                ),
            ],
        )
