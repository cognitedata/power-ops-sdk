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
from cognite.powerops.client._generated.v1.data_classes._plant import (
    PlantQuery,
    _PLANT_PROPERTIES_BY_FIELD,
    _create_plant_filter,
)
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    Plant,
    PlantWrite,
    PlantFields,
    PlantList,
    PlantWriteList,
    PlantTextFields,
    PlantWaterValueBased,
)


class PlantAPI(NodeAPI[Plant, PlantWrite, PlantList, PlantWriteList]):
    _view_id = dm.ViewId("power_ops_core", "Plant", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _PLANT_PROPERTIES_BY_FIELD
    _direct_children_by_external_id: ClassVar[dict[str, type[DomainModel]]] = {
        "PlantWaterValueBased": PlantWaterValueBased,
    }
    _class_type = Plant
    _class_list = PlantList
    _class_write_list = PlantWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)


    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: SequenceNotStr[Literal["PlantWaterValueBased"]] | None = None,
    ) -> Plant | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: SequenceNotStr[Literal["PlantWaterValueBased"]] | None = None,
    ) -> PlantList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
        as_child_class: SequenceNotStr[Literal["PlantWaterValueBased"]] | None = None,
    ) -> Plant | PlantList | None:
        """Retrieve one or more plants by id(s).

        Args:
            external_id: External id or list of external ids of the plants.
            space: The space where all the plants are located.
            as_child_class: If you want to retrieve the plants as a child class,
                you can specify the child class here. Note that if one node has properties in
                multiple child classes, you will get duplicate nodes in the result.

        Returns:
            The requested plants.

        Examples:

            Retrieve plant by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> plant = client.plant.retrieve(
                ...     "my_plant"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
            as_child_class=as_child_class
        )

    def search(
        self,
        query: str,
        properties: PlantTextFields | SequenceNotStr[PlantTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: PlantFields | SequenceNotStr[PlantFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> PlantList:
        """Search plants

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
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of plants to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results plants matching the query.

        Examples:

           Search for 'my_plant' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> plants = client.plant.search(
                ...     'my_plant'
                ... )

        """
        filter_ = _create_plant_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            asset_type,
            asset_type_prefix,
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
        property: PlantFields | SequenceNotStr[PlantFields] | None = None,
        query: str | None = None,
        search_property: PlantTextFields | SequenceNotStr[PlantTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
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
        property: PlantFields | SequenceNotStr[PlantFields] | None = None,
        query: str | None = None,
        search_property: PlantTextFields | SequenceNotStr[PlantTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
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
        group_by: PlantFields | SequenceNotStr[PlantFields],
        property: PlantFields | SequenceNotStr[PlantFields] | None = None,
        query: str | None = None,
        search_property: PlantTextFields | SequenceNotStr[PlantTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
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
        group_by: PlantFields | SequenceNotStr[PlantFields] | None = None,
        property: PlantFields | SequenceNotStr[PlantFields] | None = None,
        query: str | None = None,
        search_property: PlantTextFields | SequenceNotStr[PlantTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across plants

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
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of plants to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count plants in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.plant.aggregate("count", space="my_space")

        """

        filter_ = _create_plant_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            asset_type,
            asset_type_prefix,
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
        property: PlantFields,
        interval: float,
        query: str | None = None,
        search_property: PlantTextFields | SequenceNotStr[PlantTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for plants

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
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of plants to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_plant_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            asset_type,
            asset_type_prefix,
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

    def select(self) -> PlantQuery:
        """Start selecting from plants."""
        return PlantQuery(self._client)

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
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[PlantList]:
        """Iterate over plants

        Args:
            chunk_size: The number of plants to return in each iteration. Defaults to 100.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_ordering: The minimum value of the ordering to filter on.
            max_ordering: The maximum value of the ordering to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of plants to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of plants

        Examples:

            Iterate plants in chunks of 100 up to 2000 items:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for plants in client.plant.iterate(chunk_size=100, limit=2000):
                ...     for plant in plants:
                ...         print(plant.external_id)

            Iterate plants in chunks of 100 sorted by external_id in descending order:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for plants in client.plant.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for plant in plants:
                ...         print(plant.external_id)

            Iterate plants in chunks of 100 and use cursors to resume the iteration:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for first_iteration in client.plant.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for plants in client.plant.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for plant in plants:
                ...         print(plant.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_plant_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            asset_type,
            asset_type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, "skip", cursors=cursors)

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
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: PlantFields | Sequence[PlantFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> PlantList:
        """List/filter plants

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_ordering: The minimum value of the ordering to filter on.
            max_ordering: The maximum value of the ordering to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of plants to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested plants

        Examples:

            List plants and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> plants = client.plant.list(limit=5)

        """
        filter_ = _create_plant_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            asset_type,
            asset_type_prefix,
            external_id_prefix,
            space,
            filter,
        )
        sort_input =  self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        return self._list(limit=limit,  filter=filter_, sort=sort_input)
