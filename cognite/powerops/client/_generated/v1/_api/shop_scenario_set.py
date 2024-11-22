from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite.powerops.client._generated.v1.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    NodeQueryStep,
    EdgeQueryStep,
    DataClassQueryBuilder,
)
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    ShopScenarioSet,
    ShopScenarioSetWrite,
    ShopScenarioSetFields,
    ShopScenarioSetList,
    ShopScenarioSetWriteList,
    ShopScenarioSetTextFields,
    DateSpecification,
    ShopScenario,
)
from cognite.powerops.client._generated.v1.data_classes._shop_scenario_set import (
    ShopScenarioSetQuery,
    _SHOPSCENARIOSET_PROPERTIES_BY_FIELD,
    _create_shop_scenario_set_filter,
)
from cognite.powerops.client._generated.v1._api._core import (
    DEFAULT_LIMIT_READ,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite.powerops.client._generated.v1._api.shop_scenario_set_scenarios import ShopScenarioSetScenariosAPI
from cognite.powerops.client._generated.v1._api.shop_scenario_set_query import ShopScenarioSetQueryAPI


class ShopScenarioSetAPI(NodeAPI[ShopScenarioSet, ShopScenarioSetWrite, ShopScenarioSetList, ShopScenarioSetWriteList]):
    _view_id = dm.ViewId("power_ops_core", "ShopScenarioSet", "1")
    _properties_by_field = _SHOPSCENARIOSET_PROPERTIES_BY_FIELD
    _class_type = ShopScenarioSet
    _class_list = ShopScenarioSetList
    _class_write_list = ShopScenarioSetWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.scenarios_edge = ShopScenarioSetScenariosAPI(client)

    def __call__(
            self,
            name: str | list[str] | None = None,
            name_prefix: str | None = None,
            start_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
            end_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit: int = DEFAULT_QUERY_LIMIT,
            filter: dm.Filter | None = None,
    ) -> ShopScenarioSetQueryAPI[ShopScenarioSetList]:
        """Query starting at shop scenario sets.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            start_specification: The start specification to filter on.
            end_specification: The end specification to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop scenario sets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for shop scenario sets.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. "
            "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_shop_scenario_set_filter(
            self._view_id,
            name,
            name_prefix,
            start_specification,
            end_specification,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = DataClassQueryBuilder(ShopScenarioSetList)
        return ShopScenarioSetQueryAPI(self._client, builder, filter_, limit)


    def apply(
        self,
        shop_scenario_set: ShopScenarioSetWrite | Sequence[ShopScenarioSetWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) shop scenario sets.

        Note: This method iterates through all nodes and timeseries linked to shop_scenario_set and creates them including the edges
        between the nodes. For example, if any of `start_specification`, `end_specification` or `scenarios` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            shop_scenario_set: Shop scenario set or sequence of shop scenario sets to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new shop_scenario_set:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import ShopScenarioSetWrite
                >>> client = PowerOpsModelsV1Client()
                >>> shop_scenario_set = ShopScenarioSetWrite(external_id="my_shop_scenario_set", ...)
                >>> result = client.shop_scenario_set.apply(shop_scenario_set)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.shop_scenario_set.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(shop_scenario_set, replace, write_none)

    def delete(self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> dm.InstancesDeleteResult:
        """Delete one or more shop scenario set.

        Args:
            external_id: External id of the shop scenario set to delete.
            space: The space where all the shop scenario set are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete shop_scenario_set by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.shop_scenario_set.delete("my_shop_scenario_set")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.shop_scenario_set.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str | dm.NodeId | tuple[str, str], space: str = DEFAULT_INSTANCE_SPACE) -> ShopScenarioSet | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE) -> ShopScenarioSetList:
        ...

    def retrieve(self, external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]], space: str = DEFAULT_INSTANCE_SPACE) -> ShopScenarioSet | ShopScenarioSetList | None:
        """Retrieve one or more shop scenario sets by id(s).

        Args:
            external_id: External id or list of external ids of the shop scenario sets.
            space: The space where all the shop scenario sets are located.

        Returns:
            The requested shop scenario sets.

        Examples:

            Retrieve shop_scenario_set by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_scenario_set = client.shop_scenario_set.retrieve("my_shop_scenario_set")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.scenarios_edge,
                    "scenarios",
                    dm.DirectRelationReference("power_ops_types", "ShopScenarioSet.scenarios"),
                    "outwards",
                    dm.ViewId("power_ops_core", "ShopScenario", "1"),
                ),
                                               ]
        )


    def search(
        self,
        query: str,
        properties: ShopScenarioSetTextFields | SequenceNotStr[ShopScenarioSetTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        start_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        end_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ShopScenarioSetFields | SequenceNotStr[ShopScenarioSetFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> ShopScenarioSetList:
        """Search shop scenario sets

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            start_specification: The start specification to filter on.
            end_specification: The end specification to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop scenario sets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results shop scenario sets matching the query.

        Examples:

           Search for 'my_shop_scenario_set' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_scenario_sets = client.shop_scenario_set.search('my_shop_scenario_set')

        """
        filter_ = _create_shop_scenario_set_filter(
            self._view_id,
            name,
            name_prefix,
            start_specification,
            end_specification,
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
        property: ShopScenarioSetFields | SequenceNotStr[ShopScenarioSetFields] | None = None,
        query: str | None = None,
        search_property: ShopScenarioSetTextFields | SequenceNotStr[ShopScenarioSetTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        start_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        end_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.AggregatedNumberedValue:
        ...

    @overload
    def aggregate(
        self,
        aggregate: SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: None = None,
        property: ShopScenarioSetFields | SequenceNotStr[ShopScenarioSetFields] | None = None,
        query: str | None = None,
        search_property: ShopScenarioSetTextFields | SequenceNotStr[ShopScenarioSetTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        start_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        end_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: ShopScenarioSetFields | SequenceNotStr[ShopScenarioSetFields],
        property: ShopScenarioSetFields | SequenceNotStr[ShopScenarioSetFields] | None = None,
        query: str | None = None,
        search_property: ShopScenarioSetTextFields | SequenceNotStr[ShopScenarioSetTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        start_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        end_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
        | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: ShopScenarioSetFields | SequenceNotStr[ShopScenarioSetFields] | None = None,
        property: ShopScenarioSetFields | SequenceNotStr[ShopScenarioSetFields] | None = None,
        query: str | None = None,
        search_property: ShopScenarioSetTextFields | SequenceNotStr[ShopScenarioSetTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        start_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        end_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across shop scenario sets

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            start_specification: The start specification to filter on.
            end_specification: The end specification to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop scenario sets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count shop scenario sets in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.shop_scenario_set.aggregate("count", space="my_space")

        """

        filter_ = _create_shop_scenario_set_filter(
            self._view_id,
            name,
            name_prefix,
            start_specification,
            end_specification,
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
        property: ShopScenarioSetFields,
        interval: float,
        query: str | None = None,
        search_property: ShopScenarioSetTextFields | SequenceNotStr[ShopScenarioSetTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        start_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        end_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for shop scenario sets

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            start_specification: The start specification to filter on.
            end_specification: The end specification to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop scenario sets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_shop_scenario_set_filter(
            self._view_id,
            name,
            name_prefix,
            start_specification,
            end_specification,
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

    def query(self) -> ShopScenarioSetQuery:
        """Start a query for shop scenario sets."""
        warnings.warn("This method is renamed to .select", UserWarning, stacklevel=2)
        return ShopScenarioSetQuery(self._client)

    def select(self) -> ShopScenarioSetQuery:
        """Start selecting from shop scenario sets."""
        warnings.warn("The .select is in alpha and is subject to breaking changes without notice.", UserWarning, stacklevel=2)
        return ShopScenarioSetQuery(self._client)

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        start_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        end_specification: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ShopScenarioSetFields | Sequence[ShopScenarioSetFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ShopScenarioSetList:
        """List/filter shop scenario sets

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            start_specification: The start specification to filter on.
            end_specification: The end specification to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop scenario sets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `start_specification`, `end_specification` and `scenarios` for the shop scenario sets. Defaults to 'skip'.
                'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested shop scenario sets

        Examples:

            List shop scenario sets and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_scenario_sets = client.shop_scenario_set.list(limit=5)

        """
        filter_ = _create_shop_scenario_set_filter(
            self._view_id,
            name,
            name_prefix,
            start_specification,
            end_specification,
            external_id_prefix,
            space,
            filter,
        )

        if retrieve_connections == "skip":
                return self._list(
                limit=limit,
                filter=filter_,
                sort_by=sort_by,  # type: ignore[arg-type]
                direction=direction,
                sort=sort,
            )

        builder = DataClassQueryBuilder(ShopScenarioSetList)
        has_data = dm.filters.HasData(views=[self._view_id])
        builder.append(
            NodeQueryStep(
                builder.create_name(None),
                dm.query.NodeResultSetExpression(
                    filter=dm.filters.And(filter_, has_data) if filter_ else has_data,
                    sort=self._create_sort(sort_by, direction, sort),  # type: ignore[arg-type]
                ),
                ShopScenarioSet,
                max_retrieve_limit=limit,
                raw_filter=filter_,
            )
        )
        from_root = builder.get_from()
        edge_scenarios = builder.create_name(from_root)
        builder.append(
            EdgeQueryStep(
                edge_scenarios,
                dm.query.EdgeResultSetExpression(
                    from_=from_root,
                    direction="outwards",
                    chain_to="destination",
                ),
            )
        )
        if retrieve_connections == "full":
            builder.append(
                NodeQueryStep(
                    builder.create_name( edge_scenarios),
                    dm.query.NodeResultSetExpression(
                        from_= edge_scenarios,
                        filter=dm.filters.HasData(views=[ShopScenario._view_id]),
                    ),
                    ShopScenario,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[DateSpecification._view_id]),
                        direction="outwards",
                        through=self._view_id.as_property_ref("startSpecification"),
                    ),
                    DateSpecification,
                )
            )
            builder.append(
                NodeQueryStep(
                    builder.create_name(from_root),
                    dm.query.NodeResultSetExpression(
                        from_=from_root,
                        filter=dm.filters.HasData(views=[DateSpecification._view_id]),
                        direction="outwards",
                        through=self._view_id.as_property_ref("endSpecification"),
                    ),
                    DateSpecification,
                )
            )
        # We know that that all nodes are connected as it is not possible to filter on connections
        builder.execute_query(self._client, remove_not_connected=False)
        return builder.unpack()
