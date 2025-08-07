from __future__ import annotations

import warnings
from collections.abc import Iterator, Sequence
from typing import Any, ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite.powerops.client._generated.v1._api._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_CHUNK_SIZE,
    instantiate_classes,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite.powerops.client._generated.v1.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    QueryBuildStepFactory,
    QueryBuilder,
    QueryExecutor,
    QueryUnpacker,
    ViewPropertyId,
)
from cognite.powerops.client._generated.v1.data_classes._plant_water_value_based import (
    PlantWaterValueBasedQuery,
    _PLANTWATERVALUEBASED_PROPERTIES_BY_FIELD,
    _create_plant_water_value_based_filter,
)
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    PlantWaterValueBased,
    PlantWaterValueBasedWrite,
    PlantWaterValueBasedFields,
    PlantWaterValueBasedList,
    PlantWaterValueBasedWriteList,
    PlantWaterValueBasedTextFields,
    Generator,
    PlantInformation,
)
from cognite.powerops.client._generated.v1._api.plant_water_value_based_generators import PlantWaterValueBasedGeneratorsAPI


class PlantWaterValueBasedAPI(NodeAPI[PlantWaterValueBased, PlantWaterValueBasedWrite, PlantWaterValueBasedList, PlantWaterValueBasedWriteList]):
    _view_id = dm.ViewId("power_ops_core", "PlantWaterValueBased", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _PLANTWATERVALUEBASED_PROPERTIES_BY_FIELD
    _direct_children_by_external_id: ClassVar[dict[str, type[DomainModel]]] = {
        "PlantInformation": PlantInformation,
    }
    _class_type = PlantWaterValueBased
    _class_list = PlantWaterValueBasedList
    _class_write_list = PlantWaterValueBasedWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.generators_edge = PlantWaterValueBasedGeneratorsAPI(client)

    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: SequenceNotStr[Literal["PlantInformation"]] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> PlantWaterValueBased | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: SequenceNotStr[Literal["PlantInformation"]] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> PlantWaterValueBasedList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: SequenceNotStr[Literal["PlantInformation"]] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> PlantWaterValueBased | PlantWaterValueBasedList | None:
        """Retrieve one or more plant water value baseds by id(s).

        Args:
            external_id: External id or list of external ids of the plant water value baseds.
            space: The space where all the plant water value baseds are located.
            as_child_class: If you want to retrieve the plant water value baseds as a child class,
                you can specify the child class here. Note that if one node has properties in
                multiple child classes, you will get duplicate nodes in the result.
            retrieve_connections: Whether to retrieve `generators` for the plant water value baseds. Defaults to
            'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the
            connected items, and 'full' will retrieve the full connected items.

        Returns:
            The requested plant water value baseds.

        Examples:

            Retrieve plant_water_value_based by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> plant_water_value_based = client.plant_water_value_based.retrieve(
                ...     "my_plant_water_value_based"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_connections=retrieve_connections,
            as_child_class=as_child_class
        )

    def search(
        self,
        query: str,
        properties: PlantWaterValueBasedTextFields | SequenceNotStr[PlantWaterValueBasedTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        min_head_loss_factor: float | None = None,
        max_head_loss_factor: float | None = None,
        min_outlet_level: float | None = None,
        max_outlet_level: float | None = None,
        min_production_max: float | None = None,
        max_production_max: float | None = None,
        min_production_min: float | None = None,
        max_production_min: float | None = None,
        min_connection_losses: float | None = None,
        max_connection_losses: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: PlantWaterValueBasedFields | SequenceNotStr[PlantWaterValueBasedFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> PlantWaterValueBasedList:
        """Search plant water value baseds

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_ordering: The minimum value of the ordering to filter on.
            max_ordering: The maximum value of the ordering to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            min_head_loss_factor: The minimum value of the head loss factor to filter on.
            max_head_loss_factor: The maximum value of the head loss factor to filter on.
            min_outlet_level: The minimum value of the outlet level to filter on.
            max_outlet_level: The maximum value of the outlet level to filter on.
            min_production_max: The minimum value of the production max to filter on.
            max_production_max: The maximum value of the production max to filter on.
            min_production_min: The minimum value of the production min to filter on.
            max_production_min: The maximum value of the production min to filter on.
            min_connection_losses: The minimum value of the connection loss to filter on.
            max_connection_losses: The maximum value of the connection loss to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of plant water value baseds to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results plant water value baseds matching the query.

        Examples:

           Search for 'my_plant_water_value_based' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> plant_water_value_baseds = client.plant_water_value_based.search(
                ...     'my_plant_water_value_based'
                ... )

        """
        filter_ = _create_plant_water_value_based_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            asset_type,
            asset_type_prefix,
            min_head_loss_factor,
            max_head_loss_factor,
            min_outlet_level,
            max_outlet_level,
            min_production_max,
            max_production_max,
            min_production_min,
            max_production_min,
            min_connection_losses,
            max_connection_losses,
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
        property: PlantWaterValueBasedFields | SequenceNotStr[PlantWaterValueBasedFields] | None = None,
        query: str | None = None,
        search_property: PlantWaterValueBasedTextFields | SequenceNotStr[PlantWaterValueBasedTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        min_head_loss_factor: float | None = None,
        max_head_loss_factor: float | None = None,
        min_outlet_level: float | None = None,
        max_outlet_level: float | None = None,
        min_production_max: float | None = None,
        max_production_max: float | None = None,
        min_production_min: float | None = None,
        max_production_min: float | None = None,
        min_connection_losses: float | None = None,
        max_connection_losses: float | None = None,
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
        property: PlantWaterValueBasedFields | SequenceNotStr[PlantWaterValueBasedFields] | None = None,
        query: str | None = None,
        search_property: PlantWaterValueBasedTextFields | SequenceNotStr[PlantWaterValueBasedTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        min_head_loss_factor: float | None = None,
        max_head_loss_factor: float | None = None,
        min_outlet_level: float | None = None,
        max_outlet_level: float | None = None,
        min_production_max: float | None = None,
        max_production_max: float | None = None,
        min_production_min: float | None = None,
        max_production_min: float | None = None,
        min_connection_losses: float | None = None,
        max_connection_losses: float | None = None,
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
        group_by: PlantWaterValueBasedFields | SequenceNotStr[PlantWaterValueBasedFields],
        property: PlantWaterValueBasedFields | SequenceNotStr[PlantWaterValueBasedFields] | None = None,
        query: str | None = None,
        search_property: PlantWaterValueBasedTextFields | SequenceNotStr[PlantWaterValueBasedTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        min_head_loss_factor: float | None = None,
        max_head_loss_factor: float | None = None,
        min_outlet_level: float | None = None,
        max_outlet_level: float | None = None,
        min_production_max: float | None = None,
        max_production_max: float | None = None,
        min_production_min: float | None = None,
        max_production_min: float | None = None,
        min_connection_losses: float | None = None,
        max_connection_losses: float | None = None,
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
        group_by: PlantWaterValueBasedFields | SequenceNotStr[PlantWaterValueBasedFields] | None = None,
        property: PlantWaterValueBasedFields | SequenceNotStr[PlantWaterValueBasedFields] | None = None,
        query: str | None = None,
        search_property: PlantWaterValueBasedTextFields | SequenceNotStr[PlantWaterValueBasedTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        min_head_loss_factor: float | None = None,
        max_head_loss_factor: float | None = None,
        min_outlet_level: float | None = None,
        max_outlet_level: float | None = None,
        min_production_max: float | None = None,
        max_production_max: float | None = None,
        min_production_min: float | None = None,
        max_production_min: float | None = None,
        min_connection_losses: float | None = None,
        max_connection_losses: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across plant water value baseds

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_ordering: The minimum value of the ordering to filter on.
            max_ordering: The maximum value of the ordering to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            min_head_loss_factor: The minimum value of the head loss factor to filter on.
            max_head_loss_factor: The maximum value of the head loss factor to filter on.
            min_outlet_level: The minimum value of the outlet level to filter on.
            max_outlet_level: The maximum value of the outlet level to filter on.
            min_production_max: The minimum value of the production max to filter on.
            max_production_max: The maximum value of the production max to filter on.
            min_production_min: The minimum value of the production min to filter on.
            max_production_min: The maximum value of the production min to filter on.
            min_connection_losses: The minimum value of the connection loss to filter on.
            max_connection_losses: The maximum value of the connection loss to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of plant water value baseds to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count plant water value baseds in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.plant_water_value_based.aggregate("count", space="my_space")

        """

        filter_ = _create_plant_water_value_based_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            asset_type,
            asset_type_prefix,
            min_head_loss_factor,
            max_head_loss_factor,
            min_outlet_level,
            max_outlet_level,
            min_production_max,
            max_production_max,
            min_production_min,
            max_production_min,
            min_connection_losses,
            max_connection_losses,
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
        property: PlantWaterValueBasedFields,
        interval: float,
        query: str | None = None,
        search_property: PlantWaterValueBasedTextFields | SequenceNotStr[PlantWaterValueBasedTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        min_head_loss_factor: float | None = None,
        max_head_loss_factor: float | None = None,
        min_outlet_level: float | None = None,
        max_outlet_level: float | None = None,
        min_production_max: float | None = None,
        max_production_max: float | None = None,
        min_production_min: float | None = None,
        max_production_min: float | None = None,
        min_connection_losses: float | None = None,
        max_connection_losses: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for plant water value baseds

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_ordering: The minimum value of the ordering to filter on.
            max_ordering: The maximum value of the ordering to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            min_head_loss_factor: The minimum value of the head loss factor to filter on.
            max_head_loss_factor: The maximum value of the head loss factor to filter on.
            min_outlet_level: The minimum value of the outlet level to filter on.
            max_outlet_level: The maximum value of the outlet level to filter on.
            min_production_max: The minimum value of the production max to filter on.
            max_production_max: The maximum value of the production max to filter on.
            min_production_min: The minimum value of the production min to filter on.
            max_production_min: The maximum value of the production min to filter on.
            min_connection_losses: The minimum value of the connection loss to filter on.
            max_connection_losses: The maximum value of the connection loss to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of plant water value baseds to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_plant_water_value_based_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            asset_type,
            asset_type_prefix,
            min_head_loss_factor,
            max_head_loss_factor,
            min_outlet_level,
            max_outlet_level,
            min_production_max,
            max_production_max,
            min_production_min,
            max_production_min,
            min_connection_losses,
            max_connection_losses,
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

    def select(self) -> PlantWaterValueBasedQuery:
        """Start selecting from plant water value baseds."""
        return PlantWaterValueBasedQuery(self._client)

    def _build(
        self,
        filter_: dm.Filter | None,
        limit: int | None,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
        chunk_size: int | None = None,
    ) -> QueryExecutor:
        builder = QueryBuilder()
        factory = QueryBuildStepFactory(builder.create_name, view_id=self._view_id, edge_connection_property="end_node")
        builder.append(factory.root(
            filter=filter_,
            sort=sort,
            limit=limit,
            max_retrieve_batch_limit=chunk_size,
            has_container_fields=True,
        ))
        if retrieve_connections == "identifier" or retrieve_connections == "full":
            builder.extend(
                factory.from_edge(
                    Generator._view_id,
                    "outwards",
                    ViewPropertyId(self._view_id, "generators"),
                    include_end_node=retrieve_connections == "full",
                    has_container_fields=True,
                )
            )
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        min_head_loss_factor: float | None = None,
        max_head_loss_factor: float | None = None,
        min_outlet_level: float | None = None,
        max_outlet_level: float | None = None,
        min_production_max: float | None = None,
        max_production_max: float | None = None,
        min_production_min: float | None = None,
        max_production_min: float | None = None,
        min_connection_losses: float | None = None,
        max_connection_losses: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[PlantWaterValueBasedList]:
        """Iterate over plant water value baseds

        Args:
            chunk_size: The number of plant water value baseds to return in each iteration. Defaults to 100.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_ordering: The minimum value of the ordering to filter on.
            max_ordering: The maximum value of the ordering to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            min_head_loss_factor: The minimum value of the head loss factor to filter on.
            max_head_loss_factor: The maximum value of the head loss factor to filter on.
            min_outlet_level: The minimum value of the outlet level to filter on.
            max_outlet_level: The maximum value of the outlet level to filter on.
            min_production_max: The minimum value of the production max to filter on.
            max_production_max: The maximum value of the production max to filter on.
            min_production_min: The minimum value of the production min to filter on.
            max_production_min: The maximum value of the production min to filter on.
            min_connection_losses: The minimum value of the connection loss to filter on.
            max_connection_losses: The maximum value of the connection loss to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            retrieve_connections: Whether to retrieve `generators` for the plant water value baseds. Defaults to
            'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the
            connected items, and 'full' will retrieve the full connected items.
            limit: Maximum number of plant water value baseds to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of plant water value baseds

        Examples:

            Iterate plant water value baseds in chunks of 100 up to 2000 items:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for plant_water_value_baseds in client.plant_water_value_based.iterate(chunk_size=100, limit=2000):
                ...     for plant_water_value_based in plant_water_value_baseds:
                ...         print(plant_water_value_based.external_id)

            Iterate plant water value baseds in chunks of 100 sorted by external_id in descending order:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for plant_water_value_baseds in client.plant_water_value_based.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for plant_water_value_based in plant_water_value_baseds:
                ...         print(plant_water_value_based.external_id)

            Iterate plant water value baseds in chunks of 100 and use cursors to resume the iteration:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for first_iteration in client.plant_water_value_based.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for plant_water_value_baseds in client.plant_water_value_based.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for plant_water_value_based in plant_water_value_baseds:
                ...         print(plant_water_value_based.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_plant_water_value_based_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            asset_type,
            asset_type_prefix,
            min_head_loss_factor,
            max_head_loss_factor,
            min_outlet_level,
            max_outlet_level,
            min_production_max,
            max_production_max,
            min_production_min,
            max_production_min,
            min_connection_losses,
            max_connection_losses,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, retrieve_connections, cursors=cursors)

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        min_head_loss_factor: float | None = None,
        max_head_loss_factor: float | None = None,
        min_outlet_level: float | None = None,
        max_outlet_level: float | None = None,
        min_production_max: float | None = None,
        max_production_max: float | None = None,
        min_production_min: float | None = None,
        max_production_min: float | None = None,
        min_connection_losses: float | None = None,
        max_connection_losses: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: PlantWaterValueBasedFields | Sequence[PlantWaterValueBasedFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_connections: Literal["skip", "identifier", "full"] = "skip",
    ) -> PlantWaterValueBasedList:
        """List/filter plant water value baseds

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_ordering: The minimum value of the ordering to filter on.
            max_ordering: The maximum value of the ordering to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            min_head_loss_factor: The minimum value of the head loss factor to filter on.
            max_head_loss_factor: The maximum value of the head loss factor to filter on.
            min_outlet_level: The minimum value of the outlet level to filter on.
            max_outlet_level: The maximum value of the outlet level to filter on.
            min_production_max: The minimum value of the production max to filter on.
            max_production_max: The maximum value of the production max to filter on.
            min_production_min: The minimum value of the production min to filter on.
            max_production_min: The maximum value of the production min to filter on.
            min_connection_losses: The minimum value of the connection loss to filter on.
            max_connection_losses: The maximum value of the connection loss to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of plant water value baseds to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_connections: Whether to retrieve `generators` for the plant water value baseds. Defaults to
            'skip'.'skip' will not retrieve any connections, 'identifier' will only retrieve the identifier of the
            connected items, and 'full' will retrieve the full connected items.

        Returns:
            List of requested plant water value baseds

        Examples:

            List plant water value baseds and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> plant_water_value_baseds = client.plant_water_value_based.list(limit=5)

        """
        filter_ = _create_plant_water_value_based_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            asset_type,
            asset_type_prefix,
            min_head_loss_factor,
            max_head_loss_factor,
            min_outlet_level,
            max_outlet_level,
            min_production_max,
            max_production_max,
            min_production_min,
            max_production_min,
            min_connection_losses,
            max_connection_losses,
            external_id_prefix,
            space,
            filter,
        )
        sort_input =  self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        if retrieve_connections == "skip":
            return self._list(limit=limit,  filter=filter_, sort=sort_input)
        return self._query(filter_, limit, retrieve_connections, sort_input, "list")
