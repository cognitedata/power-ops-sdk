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
from cognite.powerops.client._generated.v1.data_classes._date_specification import (
    DateSpecificationQuery,
    _DATESPECIFICATION_PROPERTIES_BY_FIELD,
    _create_date_specification_filter,
)
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    DateSpecification,
    DateSpecificationWrite,
    DateSpecificationFields,
    DateSpecificationList,
    DateSpecificationWriteList,
    DateSpecificationTextFields,
)


class DateSpecificationAPI(NodeAPI[DateSpecification, DateSpecificationWrite, DateSpecificationList, DateSpecificationWriteList]):
    _view_id = dm.ViewId("power_ops_core", "DateSpecification", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _DATESPECIFICATION_PROPERTIES_BY_FIELD
    _class_type = DateSpecification
    _class_list = DateSpecificationList
    _class_write_list = DateSpecificationWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)


    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> DateSpecification | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> DateSpecificationList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> DateSpecification | DateSpecificationList | None:
        """Retrieve one or more date specifications by id(s).

        Args:
            external_id: External id or list of external ids of the date specifications.
            space: The space where all the date specifications are located.

        Returns:
            The requested date specifications.

        Examples:

            Retrieve date_specification by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> date_specification = client.date_specification.retrieve(
                ...     "my_date_specification"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
        )

    def search(
        self,
        query: str,
        properties: DateSpecificationTextFields | SequenceNotStr[DateSpecificationTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        processing_timezone: str | list[str] | None = None,
        processing_timezone_prefix: str | None = None,
        resulting_timezone: str | list[str] | None = None,
        resulting_timezone_prefix: str | None = None,
        floor_frame: str | list[str] | None = None,
        floor_frame_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: DateSpecificationFields | SequenceNotStr[DateSpecificationFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> DateSpecificationList:
        """Search date specifications

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            processing_timezone: The processing timezone to filter on.
            processing_timezone_prefix: The prefix of the processing timezone to filter on.
            resulting_timezone: The resulting timezone to filter on.
            resulting_timezone_prefix: The prefix of the resulting timezone to filter on.
            floor_frame: The floor frame to filter on.
            floor_frame_prefix: The prefix of the floor frame to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of date specifications to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results date specifications matching the query.

        Examples:

           Search for 'my_date_specification' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> date_specifications = client.date_specification.search(
                ...     'my_date_specification'
                ... )

        """
        filter_ = _create_date_specification_filter(
            self._view_id,
            name,
            name_prefix,
            processing_timezone,
            processing_timezone_prefix,
            resulting_timezone,
            resulting_timezone_prefix,
            floor_frame,
            floor_frame_prefix,
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
        property: DateSpecificationFields | SequenceNotStr[DateSpecificationFields] | None = None,
        query: str | None = None,
        search_property: DateSpecificationTextFields | SequenceNotStr[DateSpecificationTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        processing_timezone: str | list[str] | None = None,
        processing_timezone_prefix: str | None = None,
        resulting_timezone: str | list[str] | None = None,
        resulting_timezone_prefix: str | None = None,
        floor_frame: str | list[str] | None = None,
        floor_frame_prefix: str | None = None,
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
        property: DateSpecificationFields | SequenceNotStr[DateSpecificationFields] | None = None,
        query: str | None = None,
        search_property: DateSpecificationTextFields | SequenceNotStr[DateSpecificationTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        processing_timezone: str | list[str] | None = None,
        processing_timezone_prefix: str | None = None,
        resulting_timezone: str | list[str] | None = None,
        resulting_timezone_prefix: str | None = None,
        floor_frame: str | list[str] | None = None,
        floor_frame_prefix: str | None = None,
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
        group_by: DateSpecificationFields | SequenceNotStr[DateSpecificationFields],
        property: DateSpecificationFields | SequenceNotStr[DateSpecificationFields] | None = None,
        query: str | None = None,
        search_property: DateSpecificationTextFields | SequenceNotStr[DateSpecificationTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        processing_timezone: str | list[str] | None = None,
        processing_timezone_prefix: str | None = None,
        resulting_timezone: str | list[str] | None = None,
        resulting_timezone_prefix: str | None = None,
        floor_frame: str | list[str] | None = None,
        floor_frame_prefix: str | None = None,
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
        group_by: DateSpecificationFields | SequenceNotStr[DateSpecificationFields] | None = None,
        property: DateSpecificationFields | SequenceNotStr[DateSpecificationFields] | None = None,
        query: str | None = None,
        search_property: DateSpecificationTextFields | SequenceNotStr[DateSpecificationTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        processing_timezone: str | list[str] | None = None,
        processing_timezone_prefix: str | None = None,
        resulting_timezone: str | list[str] | None = None,
        resulting_timezone_prefix: str | None = None,
        floor_frame: str | list[str] | None = None,
        floor_frame_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across date specifications

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            processing_timezone: The processing timezone to filter on.
            processing_timezone_prefix: The prefix of the processing timezone to filter on.
            resulting_timezone: The resulting timezone to filter on.
            resulting_timezone_prefix: The prefix of the resulting timezone to filter on.
            floor_frame: The floor frame to filter on.
            floor_frame_prefix: The prefix of the floor frame to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of date specifications to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count date specifications in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.date_specification.aggregate("count", space="my_space")

        """

        filter_ = _create_date_specification_filter(
            self._view_id,
            name,
            name_prefix,
            processing_timezone,
            processing_timezone_prefix,
            resulting_timezone,
            resulting_timezone_prefix,
            floor_frame,
            floor_frame_prefix,
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
        property: DateSpecificationFields,
        interval: float,
        query: str | None = None,
        search_property: DateSpecificationTextFields | SequenceNotStr[DateSpecificationTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        processing_timezone: str | list[str] | None = None,
        processing_timezone_prefix: str | None = None,
        resulting_timezone: str | list[str] | None = None,
        resulting_timezone_prefix: str | None = None,
        floor_frame: str | list[str] | None = None,
        floor_frame_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for date specifications

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            processing_timezone: The processing timezone to filter on.
            processing_timezone_prefix: The prefix of the processing timezone to filter on.
            resulting_timezone: The resulting timezone to filter on.
            resulting_timezone_prefix: The prefix of the resulting timezone to filter on.
            floor_frame: The floor frame to filter on.
            floor_frame_prefix: The prefix of the floor frame to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of date specifications to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_date_specification_filter(
            self._view_id,
            name,
            name_prefix,
            processing_timezone,
            processing_timezone_prefix,
            resulting_timezone,
            resulting_timezone_prefix,
            floor_frame,
            floor_frame_prefix,
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

    def select(self) -> DateSpecificationQuery:
        """Start selecting from date specifications."""
        return DateSpecificationQuery(self._client)

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
        processing_timezone: str | list[str] | None = None,
        processing_timezone_prefix: str | None = None,
        resulting_timezone: str | list[str] | None = None,
        resulting_timezone_prefix: str | None = None,
        floor_frame: str | list[str] | None = None,
        floor_frame_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[DateSpecificationList]:
        """Iterate over date specifications

        Args:
            chunk_size: The number of date specifications to return in each iteration. Defaults to 100.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            processing_timezone: The processing timezone to filter on.
            processing_timezone_prefix: The prefix of the processing timezone to filter on.
            resulting_timezone: The resulting timezone to filter on.
            resulting_timezone_prefix: The prefix of the resulting timezone to filter on.
            floor_frame: The floor frame to filter on.
            floor_frame_prefix: The prefix of the floor frame to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of date specifications to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of date specifications

        Examples:

            Iterate date specifications in chunks of 100 up to 2000 items:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for date_specifications in client.date_specification.iterate(chunk_size=100, limit=2000):
                ...     for date_specification in date_specifications:
                ...         print(date_specification.external_id)

            Iterate date specifications in chunks of 100 sorted by external_id in descending order:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for date_specifications in client.date_specification.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for date_specification in date_specifications:
                ...         print(date_specification.external_id)

            Iterate date specifications in chunks of 100 and use cursors to resume the iteration:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for first_iteration in client.date_specification.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for date_specifications in client.date_specification.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for date_specification in date_specifications:
                ...         print(date_specification.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_date_specification_filter(
            self._view_id,
            name,
            name_prefix,
            processing_timezone,
            processing_timezone_prefix,
            resulting_timezone,
            resulting_timezone_prefix,
            floor_frame,
            floor_frame_prefix,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, "skip", cursors=cursors)

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        processing_timezone: str | list[str] | None = None,
        processing_timezone_prefix: str | None = None,
        resulting_timezone: str | list[str] | None = None,
        resulting_timezone_prefix: str | None = None,
        floor_frame: str | list[str] | None = None,
        floor_frame_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: DateSpecificationFields | Sequence[DateSpecificationFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> DateSpecificationList:
        """List/filter date specifications

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            processing_timezone: The processing timezone to filter on.
            processing_timezone_prefix: The prefix of the processing timezone to filter on.
            resulting_timezone: The resulting timezone to filter on.
            resulting_timezone_prefix: The prefix of the resulting timezone to filter on.
            floor_frame: The floor frame to filter on.
            floor_frame_prefix: The prefix of the floor frame to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of date specifications to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested date specifications

        Examples:

            List date specifications and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> date_specifications = client.date_specification.list(limit=5)

        """
        filter_ = _create_date_specification_filter(
            self._view_id,
            name,
            name_prefix,
            processing_timezone,
            processing_timezone_prefix,
            resulting_timezone,
            resulting_timezone_prefix,
            floor_frame,
            floor_frame_prefix,
            external_id_prefix,
            space,
            filter,
        )
        sort_input =  self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        return self._list(limit=limit,  filter=filter_, sort=sort_input)
