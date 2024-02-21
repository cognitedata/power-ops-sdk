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
    SHOPResult,
    SHOPResultWrite,
    SHOPResultFields,
    SHOPResultList,
    SHOPResultWriteList,
)
from cognite.powerops.client._generated.v1.data_classes._shop_result import (
    _SHOPRESULT_PROPERTIES_BY_FIELD,
    _create_shop_result_filter,
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
from .shop_result_alerts import SHOPResultAlertsAPI
from .shop_result_production import SHOPResultProductionAPI
from .shop_result_price import SHOPResultPriceAPI
from .shop_result_query import SHOPResultQueryAPI


class SHOPResultAPI(NodeAPI[SHOPResult, SHOPResultWrite, SHOPResultList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[SHOPResult]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=SHOPResult,
            class_list=SHOPResultList,
            class_write_list=SHOPResultWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.alerts_edge = SHOPResultAlertsAPI(client)
        self.production = SHOPResultProductionAPI(client, view_id)
        self.price = SHOPResultPriceAPI(client, view_id)

    def __call__(
        self,
        scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> SHOPResultQueryAPI[SHOPResultList]:
        """Query starting at shop results.

        Args:
            scenario: The scenario to filter on.
            price_scenario: The price scenario to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop results to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for shop results.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_shop_result_filter(
            self._view_id,
            scenario,
            price_scenario,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(SHOPResultList)
        return SHOPResultQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        shop_result: SHOPResultWrite | Sequence[SHOPResultWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) shop results.

        Note: This method iterates through all nodes and timeseries linked to shop_result and creates them including the edges
        between the nodes. For example, if any of `alerts` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            shop_result: Shop result or sequence of shop results to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new shop_result:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import SHOPResultWrite
                >>> client = PowerOpsModelsV1Client()
                >>> shop_result = SHOPResultWrite(external_id="my_shop_result", ...)
                >>> result = client.shop_result.apply(shop_result)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.shop_result.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(shop_result, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more shop result.

        Args:
            external_id: External id of the shop result to delete.
            space: The space where all the shop result are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete shop_result by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.shop_result.delete("my_shop_result")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.shop_result.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> SHOPResult | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> SHOPResultList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> SHOPResult | SHOPResultList | None:
        """Retrieve one or more shop results by id(s).

        Args:
            external_id: External id or list of external ids of the shop results.
            space: The space where all the shop results are located.

        Returns:
            The requested shop results.

        Examples:

            Retrieve shop_result by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_result = client.shop_result.retrieve("my_shop_result")

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
        property: SHOPResultFields | Sequence[SHOPResultFields] | None = None,
        group_by: None = None,
        scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: SHOPResultFields | Sequence[SHOPResultFields] | None = None,
        group_by: SHOPResultFields | Sequence[SHOPResultFields] = None,
        scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: SHOPResultFields | Sequence[SHOPResultFields] | None = None,
        group_by: SHOPResultFields | Sequence[SHOPResultFields] | None = None,
        scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across shop results

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            scenario: The scenario to filter on.
            price_scenario: The price scenario to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop results to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count shop results in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.shop_result.aggregate("count", space="my_space")

        """

        filter_ = _create_shop_result_filter(
            self._view_id,
            scenario,
            price_scenario,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _SHOPRESULT_PROPERTIES_BY_FIELD,
            property,
            group_by,
            None,
            None,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: SHOPResultFields,
        interval: float,
        scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for shop results

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            scenario: The scenario to filter on.
            price_scenario: The price scenario to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop results to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_shop_result_filter(
            self._view_id,
            scenario,
            price_scenario,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _SHOPRESULT_PROPERTIES_BY_FIELD,
            None,
            None,
            limit,
            filter_,
        )

    def list(
        self,
        scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_scenario: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> SHOPResultList:
        """List/filter shop results

        Args:
            scenario: The scenario to filter on.
            price_scenario: The price scenario to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop results to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `alerts` external ids for the shop results. Defaults to True.

        Returns:
            List of requested shop results

        Examples:

            List shop results and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_results = client.shop_result.list(limit=5)

        """
        filter_ = _create_shop_result_filter(
            self._view_id,
            scenario,
            price_scenario,
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
            ],
        )
