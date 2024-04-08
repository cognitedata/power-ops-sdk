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
    Scenario,
    ScenarioWrite,
    ScenarioFields,
    ScenarioList,
    ScenarioWriteList,
    ScenarioTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._scenario import (
    _SCENARIO_PROPERTIES_BY_FIELD,
    _create_scenario_filter,
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
from .scenario_mappings_override import ScenarioMappingsOverrideAPI
from .scenario_query import ScenarioQueryAPI


class ScenarioAPI(NodeAPI[Scenario, ScenarioWrite, ScenarioList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[Scenario]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Scenario,
            class_list=ScenarioList,
            class_write_list=ScenarioWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.mappings_override_edge = ScenarioMappingsOverrideAPI(client)

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model_template: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> ScenarioQueryAPI[ScenarioList]:
        """Query starting at scenarios.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            model_template: The model template to filter on.
            commands: The command to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of scenarios to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for scenarios.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_scenario_filter(
            self._view_id,
            name,
            name_prefix,
            model_template,
            commands,
            source,
            source_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(ScenarioList)
        return ScenarioQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        scenario: ScenarioWrite | Sequence[ScenarioWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) scenarios.

        Note: This method iterates through all nodes and timeseries linked to scenario and creates them including the edges
        between the nodes. For example, if any of `mappings_override` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            scenario: Scenario or sequence of scenarios to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new scenario:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import ScenarioWrite
                >>> client = PowerOpsModelsV1Client()
                >>> scenario = ScenarioWrite(external_id="my_scenario", ...)
                >>> result = client.scenario.apply(scenario)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.scenario.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(scenario, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more scenario.

        Args:
            external_id: External id of the scenario to delete.
            space: The space where all the scenario are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete scenario by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.scenario.delete("my_scenario")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.scenario.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> Scenario | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> ScenarioList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Scenario | ScenarioList | None:
        """Retrieve one or more scenarios by id(s).

        Args:
            external_id: External id or list of external ids of the scenarios.
            space: The space where all the scenarios are located.

        Returns:
            The requested scenarios.

        Examples:

            Retrieve scenario by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> scenario = client.scenario.retrieve("my_scenario")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.mappings_override_edge,
                    "mappings_override",
                    dm.DirectRelationReference("sp_powerops_types_temp", "Mapping"),
                    "outwards",
                    dm.ViewId("sp_powerops_models_temp", "Mapping", "1"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: ScenarioTextFields | Sequence[ScenarioTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model_template: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> ScenarioList:
        """Search scenarios

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            model_template: The model template to filter on.
            commands: The command to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of scenarios to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results scenarios matching the query.

        Examples:

           Search for 'my_scenario' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> scenarios = client.scenario.search('my_scenario')

        """
        filter_ = _create_scenario_filter(
            self._view_id,
            name,
            name_prefix,
            model_template,
            commands,
            source,
            source_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _SCENARIO_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: ScenarioFields | Sequence[ScenarioFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: ScenarioTextFields | Sequence[ScenarioTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model_template: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
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
        property: ScenarioFields | Sequence[ScenarioFields] | None = None,
        group_by: ScenarioFields | Sequence[ScenarioFields] = None,
        query: str | None = None,
        search_properties: ScenarioTextFields | Sequence[ScenarioTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model_template: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
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
        property: ScenarioFields | Sequence[ScenarioFields] | None = None,
        group_by: ScenarioFields | Sequence[ScenarioFields] | None = None,
        query: str | None = None,
        search_property: ScenarioTextFields | Sequence[ScenarioTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model_template: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across scenarios

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            model_template: The model template to filter on.
            commands: The command to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of scenarios to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count scenarios in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.scenario.aggregate("count", space="my_space")

        """

        filter_ = _create_scenario_filter(
            self._view_id,
            name,
            name_prefix,
            model_template,
            commands,
            source,
            source_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _SCENARIO_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: ScenarioFields,
        interval: float,
        query: str | None = None,
        search_property: ScenarioTextFields | Sequence[ScenarioTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model_template: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for scenarios

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            model_template: The model template to filter on.
            commands: The command to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of scenarios to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_scenario_filter(
            self._view_id,
            name,
            name_prefix,
            model_template,
            commands,
            source,
            source_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _SCENARIO_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model_template: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        commands: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> ScenarioList:
        """List/filter scenarios

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            model_template: The model template to filter on.
            commands: The command to filter on.
            source: The source to filter on.
            source_prefix: The prefix of the source to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of scenarios to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `mappings_override` external ids for the scenarios. Defaults to True.

        Returns:
            List of requested scenarios

        Examples:

            List scenarios and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> scenarios = client.scenario.list(limit=5)

        """
        filter_ = _create_scenario_filter(
            self._view_id,
            name,
            name_prefix,
            model_template,
            commands,
            source,
            source_prefix,
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
                    self.mappings_override_edge,
                    "mappings_override",
                    dm.DirectRelationReference("sp_powerops_types_temp", "Mapping"),
                    "outwards",
                    dm.ViewId("sp_powerops_models_temp", "Mapping", "1"),
                ),
            ],
        )
