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
    MultiScenarioMatrixRaw,
    MultiScenarioMatrixRawWrite,
    MultiScenarioMatrixRawFields,
    MultiScenarioMatrixRawList,
    MultiScenarioMatrixRawWriteList,
    MultiScenarioMatrixRawTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._multi_scenario_matrix_raw import (
    _MULTISCENARIOMATRIXRAW_PROPERTIES_BY_FIELD,
    _create_multi_scenario_matrix_raw_filter,
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
from .multi_scenario_matrix_raw_alerts import MultiScenarioMatrixRawAlertsAPI
from .multi_scenario_matrix_raw_shop_results import MultiScenarioMatrixRawShopResultsAPI
from .multi_scenario_matrix_raw_query import MultiScenarioMatrixRawQueryAPI


class MultiScenarioMatrixRawAPI(
    NodeAPI[MultiScenarioMatrixRaw, MultiScenarioMatrixRawWrite, MultiScenarioMatrixRawList]
):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[MultiScenarioMatrixRaw]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=MultiScenarioMatrixRaw,
            class_list=MultiScenarioMatrixRawList,
            class_write_list=MultiScenarioMatrixRawWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.alerts_edge = MultiScenarioMatrixRawAlertsAPI(client)
        self.shop_results_edge = MultiScenarioMatrixRawShopResultsAPI(client)

    def __call__(
        self,
        resource_cost: str | list[str] | None = None,
        resource_cost_prefix: str | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        asset_id: str | list[str] | None = None,
        asset_id_prefix: str | None = None,
        is_processed: bool | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> MultiScenarioMatrixRawQueryAPI[MultiScenarioMatrixRawList]:
        """Query starting at multi scenario matrix raws.

        Args:
            resource_cost: The resource cost to filter on.
            resource_cost_prefix: The prefix of the resource cost to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            asset_id: The asset id to filter on.
            asset_id_prefix: The prefix of the asset id to filter on.
            is_processed: The is processed to filter on.
            method: The method to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of multi scenario matrix raws to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for multi scenario matrix raws.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_multi_scenario_matrix_raw_filter(
            self._view_id,
            resource_cost,
            resource_cost_prefix,
            asset_type,
            asset_type_prefix,
            asset_id,
            asset_id_prefix,
            is_processed,
            method,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(MultiScenarioMatrixRawList)
        return MultiScenarioMatrixRawQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        multi_scenario_matrix_raw: MultiScenarioMatrixRawWrite | Sequence[MultiScenarioMatrixRawWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) multi scenario matrix raws.

        Note: This method iterates through all nodes and timeseries linked to multi_scenario_matrix_raw and creates them including the edges
        between the nodes. For example, if any of `alerts` or `shop_results` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            multi_scenario_matrix_raw: Multi scenario matrix raw or sequence of multi scenario matrix raws to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new multi_scenario_matrix_raw:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import MultiScenarioMatrixRawWrite
                >>> client = PowerOpsModelsV1Client()
                >>> multi_scenario_matrix_raw = MultiScenarioMatrixRawWrite(external_id="my_multi_scenario_matrix_raw", ...)
                >>> result = client.multi_scenario_matrix_raw.apply(multi_scenario_matrix_raw)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.multi_scenario_matrix_raw.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(multi_scenario_matrix_raw, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more multi scenario matrix raw.

        Args:
            external_id: External id of the multi scenario matrix raw to delete.
            space: The space where all the multi scenario matrix raw are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete multi_scenario_matrix_raw by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.multi_scenario_matrix_raw.delete("my_multi_scenario_matrix_raw")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.multi_scenario_matrix_raw.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> MultiScenarioMatrixRaw | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> MultiScenarioMatrixRawList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> MultiScenarioMatrixRaw | MultiScenarioMatrixRawList | None:
        """Retrieve one or more multi scenario matrix raws by id(s).

        Args:
            external_id: External id or list of external ids of the multi scenario matrix raws.
            space: The space where all the multi scenario matrix raws are located.

        Returns:
            The requested multi scenario matrix raws.

        Examples:

            Retrieve multi_scenario_matrix_raw by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> multi_scenario_matrix_raw = client.multi_scenario_matrix_raw.retrieve("my_multi_scenario_matrix_raw")

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
                    self.shop_results_edge,
                    "shop_results",
                    dm.DirectRelationReference("sp_powerops_types", "MultiScenarioMatrix.shopResults"),
                    "outwards",
                    dm.ViewId("sp_powerops_models", "SHOPResultPriceProd", "1"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: MultiScenarioMatrixRawTextFields | Sequence[MultiScenarioMatrixRawTextFields] | None = None,
        resource_cost: str | list[str] | None = None,
        resource_cost_prefix: str | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        asset_id: str | list[str] | None = None,
        asset_id_prefix: str | None = None,
        is_processed: bool | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> MultiScenarioMatrixRawList:
        """Search multi scenario matrix raws

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            resource_cost: The resource cost to filter on.
            resource_cost_prefix: The prefix of the resource cost to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            asset_id: The asset id to filter on.
            asset_id_prefix: The prefix of the asset id to filter on.
            is_processed: The is processed to filter on.
            method: The method to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of multi scenario matrix raws to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results multi scenario matrix raws matching the query.

        Examples:

           Search for 'my_multi_scenario_matrix_raw' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> multi_scenario_matrix_raws = client.multi_scenario_matrix_raw.search('my_multi_scenario_matrix_raw')

        """
        filter_ = _create_multi_scenario_matrix_raw_filter(
            self._view_id,
            resource_cost,
            resource_cost_prefix,
            asset_type,
            asset_type_prefix,
            asset_id,
            asset_id_prefix,
            is_processed,
            method,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            self._view_id, query, _MULTISCENARIOMATRIXRAW_PROPERTIES_BY_FIELD, properties, filter_, limit
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
        property: MultiScenarioMatrixRawFields | Sequence[MultiScenarioMatrixRawFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: MultiScenarioMatrixRawTextFields | Sequence[MultiScenarioMatrixRawTextFields] | None = None,
        resource_cost: str | list[str] | None = None,
        resource_cost_prefix: str | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        asset_id: str | list[str] | None = None,
        asset_id_prefix: str | None = None,
        is_processed: bool | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: MultiScenarioMatrixRawFields | Sequence[MultiScenarioMatrixRawFields] | None = None,
        group_by: MultiScenarioMatrixRawFields | Sequence[MultiScenarioMatrixRawFields] = None,
        query: str | None = None,
        search_properties: MultiScenarioMatrixRawTextFields | Sequence[MultiScenarioMatrixRawTextFields] | None = None,
        resource_cost: str | list[str] | None = None,
        resource_cost_prefix: str | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        asset_id: str | list[str] | None = None,
        asset_id_prefix: str | None = None,
        is_processed: bool | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: MultiScenarioMatrixRawFields | Sequence[MultiScenarioMatrixRawFields] | None = None,
        group_by: MultiScenarioMatrixRawFields | Sequence[MultiScenarioMatrixRawFields] | None = None,
        query: str | None = None,
        search_property: MultiScenarioMatrixRawTextFields | Sequence[MultiScenarioMatrixRawTextFields] | None = None,
        resource_cost: str | list[str] | None = None,
        resource_cost_prefix: str | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        asset_id: str | list[str] | None = None,
        asset_id_prefix: str | None = None,
        is_processed: bool | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across multi scenario matrix raws

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            resource_cost: The resource cost to filter on.
            resource_cost_prefix: The prefix of the resource cost to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            asset_id: The asset id to filter on.
            asset_id_prefix: The prefix of the asset id to filter on.
            is_processed: The is processed to filter on.
            method: The method to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of multi scenario matrix raws to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count multi scenario matrix raws in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.multi_scenario_matrix_raw.aggregate("count", space="my_space")

        """

        filter_ = _create_multi_scenario_matrix_raw_filter(
            self._view_id,
            resource_cost,
            resource_cost_prefix,
            asset_type,
            asset_type_prefix,
            asset_id,
            asset_id_prefix,
            is_processed,
            method,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _MULTISCENARIOMATRIXRAW_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: MultiScenarioMatrixRawFields,
        interval: float,
        query: str | None = None,
        search_property: MultiScenarioMatrixRawTextFields | Sequence[MultiScenarioMatrixRawTextFields] | None = None,
        resource_cost: str | list[str] | None = None,
        resource_cost_prefix: str | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        asset_id: str | list[str] | None = None,
        asset_id_prefix: str | None = None,
        is_processed: bool | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for multi scenario matrix raws

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            resource_cost: The resource cost to filter on.
            resource_cost_prefix: The prefix of the resource cost to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            asset_id: The asset id to filter on.
            asset_id_prefix: The prefix of the asset id to filter on.
            is_processed: The is processed to filter on.
            method: The method to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of multi scenario matrix raws to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_multi_scenario_matrix_raw_filter(
            self._view_id,
            resource_cost,
            resource_cost_prefix,
            asset_type,
            asset_type_prefix,
            asset_id,
            asset_id_prefix,
            is_processed,
            method,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _MULTISCENARIOMATRIXRAW_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        resource_cost: str | list[str] | None = None,
        resource_cost_prefix: str | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        asset_id: str | list[str] | None = None,
        asset_id_prefix: str | None = None,
        is_processed: bool | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> MultiScenarioMatrixRawList:
        """List/filter multi scenario matrix raws

        Args:
            resource_cost: The resource cost to filter on.
            resource_cost_prefix: The prefix of the resource cost to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            asset_id: The asset id to filter on.
            asset_id_prefix: The prefix of the asset id to filter on.
            is_processed: The is processed to filter on.
            method: The method to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of multi scenario matrix raws to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `alerts` or `shop_results` external ids for the multi scenario matrix raws. Defaults to True.

        Returns:
            List of requested multi scenario matrix raws

        Examples:

            List multi scenario matrix raws and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> multi_scenario_matrix_raws = client.multi_scenario_matrix_raw.list(limit=5)

        """
        filter_ = _create_multi_scenario_matrix_raw_filter(
            self._view_id,
            resource_cost,
            resource_cost_prefix,
            asset_type,
            asset_type_prefix,
            asset_id,
            asset_id_prefix,
            is_processed,
            method,
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
                    self.shop_results_edge,
                    "shop_results",
                    dm.DirectRelationReference("sp_powerops_types", "MultiScenarioMatrix.shopResults"),
                    "outwards",
                    dm.ViewId("sp_powerops_models", "SHOPResultPriceProd", "1"),
                ),
            ],
        )
