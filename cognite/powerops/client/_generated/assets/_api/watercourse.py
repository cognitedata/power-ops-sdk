from __future__ import annotations

from collections.abc import Sequence
from typing import overload
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.assets.data_classes._core import DEFAULT_INSTANCE_SPACE
from cognite.powerops.client._generated.assets.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    Watercourse,
    WatercourseWrite,
    WatercourseFields,
    WatercourseList,
    WatercourseWriteList,
    WatercourseTextFields,
)
from cognite.powerops.client._generated.assets.data_classes._watercourse import (
    _WATERCOURSE_PROPERTIES_BY_FIELD,
    _create_watercourse_filter,
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
from .watercourse_plants import WatercoursePlantsAPI
from .watercourse_production_obligation import WatercourseProductionObligationAPI
from .watercourse_query import WatercourseQueryAPI


class WatercourseAPI(NodeAPI[Watercourse, WatercourseWrite, WatercourseList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[Watercourse]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Watercourse,
            class_list=WatercourseList,
            class_write_list=WatercourseWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.plants_edge = WatercoursePlantsAPI(client)
        self.production_obligation = WatercourseProductionObligationAPI(client, view_id)

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> WatercourseQueryAPI[WatercourseList]:
        """Query starting at watercourses.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_penalty_limit: The minimum value of the penalty limit to filter on.
            max_penalty_limit: The maximum value of the penalty limit to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of watercourses to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for watercourses.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_watercourse_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_penalty_limit,
            max_penalty_limit,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(WatercourseList)
        return WatercourseQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        watercourse: WatercourseWrite | Sequence[WatercourseWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) watercourses.

        Note: This method iterates through all nodes and timeseries linked to watercourse and creates them including the edges
        between the nodes. For example, if any of `plants` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            watercourse: Watercourse or sequence of watercourses to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new watercourse:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> from cognite.powerops.client._generated.assets.data_classes import WatercourseWrite
                >>> client = PowerAssetAPI()
                >>> watercourse = WatercourseWrite(external_id="my_watercourse", ...)
                >>> result = client.watercourse.apply(watercourse)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.watercourse.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(watercourse, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more watercourse.

        Args:
            external_id: External id of the watercourse to delete.
            space: The space where all the watercourse are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete watercourse by id:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> client.watercourse.delete("my_watercourse")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.watercourse.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> Watercourse | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> WatercourseList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Watercourse | WatercourseList | None:
        """Retrieve one or more watercourses by id(s).

        Args:
            external_id: External id or list of external ids of the watercourses.
            space: The space where all the watercourses are located.

        Returns:
            The requested watercourses.

        Examples:

            Retrieve watercourse by id:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> watercourse = client.watercourse.retrieve("my_watercourse")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.plants_edge,
                    "plants",
                    dm.DirectRelationReference("power-ops-types", "isSubAssetOf"),
                    "outwards",
                    dm.ViewId("power-ops-assets", "Plant", "1"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: WatercourseTextFields | Sequence[WatercourseTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> WatercourseList:
        """Search watercourses

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_penalty_limit: The minimum value of the penalty limit to filter on.
            max_penalty_limit: The maximum value of the penalty limit to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of watercourses to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results watercourses matching the query.

        Examples:

           Search for 'my_watercourse' in all text properties:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> watercourses = client.watercourse.search('my_watercourse')

        """
        filter_ = _create_watercourse_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_penalty_limit,
            max_penalty_limit,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _WATERCOURSE_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: WatercourseFields | Sequence[WatercourseFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: WatercourseTextFields | Sequence[WatercourseTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
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
        property: WatercourseFields | Sequence[WatercourseFields] | None = None,
        group_by: WatercourseFields | Sequence[WatercourseFields] = None,
        query: str | None = None,
        search_properties: WatercourseTextFields | Sequence[WatercourseTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
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
        property: WatercourseFields | Sequence[WatercourseFields] | None = None,
        group_by: WatercourseFields | Sequence[WatercourseFields] | None = None,
        query: str | None = None,
        search_property: WatercourseTextFields | Sequence[WatercourseTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across watercourses

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_penalty_limit: The minimum value of the penalty limit to filter on.
            max_penalty_limit: The maximum value of the penalty limit to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of watercourses to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count watercourses in space `my_space`:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> result = client.watercourse.aggregate("count", space="my_space")

        """

        filter_ = _create_watercourse_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_penalty_limit,
            max_penalty_limit,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _WATERCOURSE_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: WatercourseFields,
        interval: float,
        query: str | None = None,
        search_property: WatercourseTextFields | Sequence[WatercourseTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for watercourses

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_penalty_limit: The minimum value of the penalty limit to filter on.
            max_penalty_limit: The maximum value of the penalty limit to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of watercourses to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_watercourse_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_penalty_limit,
            max_penalty_limit,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _WATERCOURSE_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> WatercourseList:
        """List/filter watercourses

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_penalty_limit: The minimum value of the penalty limit to filter on.
            max_penalty_limit: The maximum value of the penalty limit to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of watercourses to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `plants` external ids for the watercourses. Defaults to True.

        Returns:
            List of requested watercourses

        Examples:

            List watercourses and limit to 5:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> watercourses = client.watercourse.list(limit=5)

        """
        filter_ = _create_watercourse_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_penalty_limit,
            max_penalty_limit,
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
                    self.plants_edge,
                    "plants",
                    dm.DirectRelationReference("power-ops-types", "isSubAssetOf"),
                    "outwards",
                    dm.ViewId("power-ops-assets", "Plant", "1"),
                ),
            ],
        )
