from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE
from cognite.powerops.client._generated.v1.data_classes import (
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
from cognite.powerops.client._generated.v1.data_classes._date_specification import (
    _DATESPECIFICATION_PROPERTIES_BY_FIELD,
    _create_date_specification_filter,
)
from ._core import DEFAULT_LIMIT_READ, DEFAULT_QUERY_LIMIT, Aggregations, NodeAPI, SequenceNotStr, QueryStep, QueryBuilder
from .date_specification_query import DateSpecificationQueryAPI


class DateSpecificationAPI(NodeAPI[DateSpecification, DateSpecificationWrite, DateSpecificationList, DateSpecificationWriteList]):
    _view_id = dm.ViewId("power_ops_core", "DateSpecification", "1")
    _properties_by_field = _DATESPECIFICATION_PROPERTIES_BY_FIELD
    _class_type = DateSpecification
    _class_list = DateSpecificationList
    _class_write_list = DateSpecificationWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)


    def __call__(
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
            limit: int = DEFAULT_QUERY_LIMIT,
            filter: dm.Filter | None = None,
    ) -> DateSpecificationQueryAPI[DateSpecificationList]:
        """Query starting at date specifications.

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
            limit: Maximum number of date specifications to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for date specifications.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
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
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(DateSpecificationList)
        return DateSpecificationQueryAPI(self._client, builder, filter_, limit)


    def apply(
        self,
        date_specification: DateSpecificationWrite | Sequence[DateSpecificationWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) date specifications.

        Args:
            date_specification: Date specification or sequence of date specifications to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new date_specification:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import DateSpecificationWrite
                >>> client = PowerOpsModelsV1Client()
                >>> date_specification = DateSpecificationWrite(external_id="my_date_specification", ...)
                >>> result = client.date_specification.apply(date_specification)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.date_specification.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(date_specification, replace, write_none)

    def delete(self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> dm.InstancesDeleteResult:
        """Delete one or more date specification.

        Args:
            external_id: External id of the date specification to delete.
            space: The space where all the date specification are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete date_specification by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.date_specification.delete("my_date_specification")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.date_specification.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> DateSpecification | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> DateSpecificationList:
        ...

    def retrieve(self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> DateSpecification | DateSpecificationList | None:
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
                >>> date_specification = client.date_specification.retrieve("my_date_specification")

        """
        return self._retrieve(external_id, space)

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
            limit: Maximum number of date specifications to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results date specifications matching the query.

        Examples:

           Search for 'my_date_specification' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> date_specifications = client.date_specification.search('my_date_specification')

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
    ) -> dm.aggregations.AggregatedNumberedValue:
        ...

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
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

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
    ) -> InstanceAggregationResultList:
        ...

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
            limit: Maximum number of date specifications to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

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
            limit: Maximum number of date specifications to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

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
            limit: Maximum number of date specifications to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
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
        return self._list(
            limit=limit,
            filter=filter_,
            sort_by=sort_by,  # type: ignore[arg-type]
            direction=direction,
            sort=sort,
        )
