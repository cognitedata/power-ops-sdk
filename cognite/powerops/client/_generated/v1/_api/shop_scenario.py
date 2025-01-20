from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite.powerops.client._generated.v1._api._core import (
    DEFAULT_LIMIT_READ,
    instantiate_classes,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite.powerops.client._generated.v1.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    QueryStepFactory,
    QueryBuilder,
    QueryUnpacker,
    ViewPropertyId,
)
from cognite.powerops.client._generated.v1.data_classes._shop_scenario import (
    ShopScenarioQuery,
    _SHOPSCENARIO_PROPERTIES_BY_FIELD,
    _create_shop_scenario_filter,
)
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    ShopScenario,
    ShopScenarioWrite,
    ShopScenarioFields,
    ShopScenarioList,
    ShopScenarioWriteList,
    ShopScenarioTextFields,
    ShopAttributeMapping,
    ShopCommands,
    ShopModel,
    ShopOutputTimeSeriesDefinition,
    ShopTimeResolution,
)
from cognite.powerops.client._generated.v1._api.shop_scenario_output_definition import ShopScenarioOutputDefinitionAPI
from cognite.powerops.client._generated.v1._api.shop_scenario_attribute_mappings_override import ShopScenarioAttributeMappingsOverrideAPI
from cognite.powerops.client._generated.v1._api.shop_scenario_query import ShopScenarioQueryAPI


class ShopScenarioAPI(NodeAPI[ShopScenario, ShopScenarioWrite, ShopScenarioList, ShopScenarioWriteList]):
    _view_id = dm.ViewId("power_ops_core", "ShopScenario", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _SHOPSCENARIO_PROPERTIES_BY_FIELD
    _class_type = ShopScenario
    _class_list = ShopScenarioList
    _class_write_list = ShopScenarioWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.output_definition_edge = ShopScenarioOutputDefinitionAPI(client)
        self.attribute_mappings_override_edge = ShopScenarioAttributeMappingsOverrideAPI(client)

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        commands: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        time_resolution: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> ShopScenarioQueryAPI[ShopScenario, ShopScenarioList]:
        """Query starting at shop scenarios.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            model: The model to filter on.
            commands: The command to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            time_resolution: The time resolution to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop scenarios to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for shop scenarios.

        """
        warnings.warn(
            "This method is deprecated and will soon be removed. "
            "Use the .select() method instead.",
            UserWarning,
            stacklevel=2,
        )
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_shop_scenario_filter(
            self._view_id,
            name,
            name_prefix,
            model,
            commands,
            source,
            source_prefix,
            time_resolution,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        return ShopScenarioQueryAPI(
            self._client, QueryBuilder(), self._class_type, self._class_list, None, filter_, limit
        )

    def apply(
        self,
        shop_scenario: ShopScenarioWrite | Sequence[ShopScenarioWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) shop scenarios.

        Args:
            shop_scenario: Shop scenario or
                sequence of shop scenarios to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and
                existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)?
                Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None.
                However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new shop_scenario:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import ShopScenarioWrite
                >>> client = PowerOpsModelsV1Client()
                >>> shop_scenario = ShopScenarioWrite(
                ...     external_id="my_shop_scenario", ...
                ... )
                >>> result = client.shop_scenario.apply(shop_scenario)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.shop_scenario.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(shop_scenario, replace, write_none)

    def delete(self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> dm.InstancesDeleteResult:
        """Delete one or more shop scenario.

        Args:
            external_id: External id of the shop scenario to delete.
            space: The space where all the shop scenario are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete shop_scenario by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.shop_scenario.delete("my_shop_scenario")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.shop_scenario.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ShopScenario | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ShopScenarioList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ShopScenario | ShopScenarioList | None:
        """Retrieve one or more shop scenarios by id(s).

        Args:
            external_id: External id or list of external ids of the shop scenarios.
            space: The space where all the shop scenarios are located.
            retrieve_connections: Whether to retrieve `model`, `commands`, `time_resolution`, `output_definition` and
            `attribute_mappings_override` for the shop scenarios. Defaults to 'skip'.'skip' will not retrieve any
            connections, 'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve
            the full connected items.

        Returns:
            The requested shop scenarios.

        Examples:

            Retrieve shop_scenario by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_scenario = client.shop_scenario.retrieve(
                ...     "my_shop_scenario"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_connections=retrieve_connections,
        )

    def search(
        self,
        query: str,
        properties: ShopScenarioTextFields | SequenceNotStr[ShopScenarioTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        commands: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        time_resolution: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ShopScenarioFields | SequenceNotStr[ShopScenarioFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> ShopScenarioList:
        """Search shop scenarios

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            model: The model to filter on.
            commands: The command to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            time_resolution: The time resolution to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop scenarios to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results shop scenarios matching the query.

        Examples:

           Search for 'my_shop_scenario' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_scenarios = client.shop_scenario.search(
                ...     'my_shop_scenario'
                ... )

        """
        filter_ = _create_shop_scenario_filter(
            self._view_id,
            name,
            name_prefix,
            model,
            commands,
            source,
            source_prefix,
            time_resolution,
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
        property: ShopScenarioFields | SequenceNotStr[ShopScenarioFields] | None = None,
        query: str | None = None,
        search_property: ShopScenarioTextFields | SequenceNotStr[ShopScenarioTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        commands: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        time_resolution: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
        property: ShopScenarioFields | SequenceNotStr[ShopScenarioFields] | None = None,
        query: str | None = None,
        search_property: ShopScenarioTextFields | SequenceNotStr[ShopScenarioTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        commands: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        time_resolution: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
        group_by: ShopScenarioFields | SequenceNotStr[ShopScenarioFields],
        property: ShopScenarioFields | SequenceNotStr[ShopScenarioFields] | None = None,
        query: str | None = None,
        search_property: ShopScenarioTextFields | SequenceNotStr[ShopScenarioTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        commands: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        time_resolution: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
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
        group_by: ShopScenarioFields | SequenceNotStr[ShopScenarioFields] | None = None,
        property: ShopScenarioFields | SequenceNotStr[ShopScenarioFields] | None = None,
        query: str | None = None,
        search_property: ShopScenarioTextFields | SequenceNotStr[ShopScenarioTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        commands: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        time_resolution: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across shop scenarios

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            model: The model to filter on.
            commands: The command to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            time_resolution: The time resolution to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop scenarios to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count shop scenarios in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.shop_scenario.aggregate("count", space="my_space")

        """

        filter_ = _create_shop_scenario_filter(
            self._view_id,
            name,
            name_prefix,
            model,
            commands,
            source,
            source_prefix,
            time_resolution,
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
        property: ShopScenarioFields,
        interval: float,
        query: str | None = None,
        search_property: ShopScenarioTextFields | SequenceNotStr[ShopScenarioTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        commands: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        time_resolution: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for shop scenarios

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            model: The model to filter on.
            commands: The command to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            time_resolution: The time resolution to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop scenarios to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_shop_scenario_filter(
            self._view_id,
            name,
            name_prefix,
            model,
            commands,
            source,
            source_prefix,
            time_resolution,
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

    def select(self) -> ShopScenarioQuery:
        """Start selecting from shop scenarios."""
        return ShopScenarioQuery(self._client)

    def _query(
        self,
        filter_: dm.Filter | None,
        limit: int,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
    ) -> list[dict[str, Any]]:
        builder = QueryBuilder()
        factory = QueryStepFactory(builder.create_name, view_id=self._view_id, edge_connection_property="end_node")
        builder.append(factory.root(
            filter=filter_,
            sort=sort,
            limit=limit,
            has_container_fields=True,
        ))
        builder.extend(
            factory.from_edge(
                ShopOutputTimeSeriesDefinition._view_id,
                "outwards",
                ViewPropertyId(self._view_id, "outputDefinition"),
                include_end_node=retrieve_connections == "full",
                has_container_fields=True,
            )
        )
        builder.extend(
            factory.from_edge(
                ShopAttributeMapping._view_id,
                "outwards",
                ViewPropertyId(self._view_id, "attributeMappingsOverride"),
                include_end_node=retrieve_connections == "full",
                has_container_fields=True,
            )
        )
        if retrieve_connections == "full":
            builder.extend(
                factory.from_direct_relation(
                    ShopModel._view_id,
                    ViewPropertyId(self._view_id, "model"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    ShopCommands._view_id,
                    ViewPropertyId(self._view_id, "commands"),
                    has_container_fields=True,
                )
            )
            builder.extend(
                factory.from_direct_relation(
                    ShopTimeResolution._view_id,
                    ViewPropertyId(self._view_id, "timeResolution"),
                    has_container_fields=True,
                )
            )
        unpack_edges: Literal["skip", "identifier"] = "identifier" if retrieve_connections == "identifier" else "skip"
        builder.execute_query(self._client, remove_not_connected=True if unpack_edges == "skip" else False)
        return QueryUnpacker(builder, edges=unpack_edges).unpack()


    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        commands: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        time_resolution: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: ShopScenarioFields | Sequence[ShopScenarioFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> ShopScenarioList:
        """List/filter shop scenarios

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            model: The model to filter on.
            commands: The command to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            time_resolution: The time resolution to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of shop scenarios to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `model`, `commands`, `time_resolution`, `output_definition` and
            `attribute_mappings_override` for the shop scenarios. Defaults to 'skip'.'skip' will not retrieve any
            connections, 'identifier' will only retrieve the identifier of the connected items, and 'full' will retrieve
            the full connected items.

        Returns:
            List of requested shop scenarios

        Examples:

            List shop scenarios and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> shop_scenarios = client.shop_scenario.list(limit=5)

        """
        filter_ = _create_shop_scenario_filter(
            self._view_id,
            name,
            name_prefix,
            model,
            commands,
            source,
            source_prefix,
            time_resolution,
            external_id_prefix,
            space,
            filter,
        )
        sort_input =  self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        if retrieve_connections == "skip":
            return self._list(limit=limit,  filter=filter_, sort=sort_input)
        values = self._query(filter_, limit, retrieve_connections, sort_input)
        return self._class_list(instantiate_classes(self._class_type, values, "list"))
