from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.production.data_classes._core import DEFAULT_INSTANCE_SPACE
from cognite.powerops.client._generated.production.data_classes import (
    DomainModelCore,
    DomainModelApply,
    ResourcesApplyResult,
    PriceArea,
    PriceAreaApply,
    PriceAreaFields,
    PriceAreaList,
    PriceAreaApplyList,
    PriceAreaTextFields,
)
from cognite.powerops.client._generated.production.data_classes._price_area import (
    _PRICEAREA_PROPERTIES_BY_FIELD,
    _create_price_area_filter,
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
from .price_area_plants import PriceAreaPlantsAPI
from .price_area_watercourses import PriceAreaWatercoursesAPI
from .price_area_dayahead_price_time_series import PriceAreaDayaheadPriceTimeSeriesAPI
from .price_area_query import PriceAreaQueryAPI


class PriceAreaAPI(NodeAPI[PriceArea, PriceAreaApply, PriceAreaList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[PriceArea]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=PriceArea,
            class_list=PriceAreaList,
            class_apply_list=PriceAreaApplyList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.plants_edge = PriceAreaPlantsAPI(client)
        self.watercourses_edge = PriceAreaWatercoursesAPI(client)
        self.dayahead_price_time_series = PriceAreaDayaheadPriceTimeSeriesAPI(client, view_id)

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> PriceAreaQueryAPI[PriceAreaList]:
        """Query starting at price areas.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of price areas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for price areas.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_price_area_filter(
            self._view_id,
            name,
            name_prefix,
            description,
            description_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(PriceAreaList)
        return PriceAreaQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self, price_area: PriceAreaApply | Sequence[PriceAreaApply], replace: bool = False
    ) -> ResourcesApplyResult:
        """Add or update (upsert) price areas.

        Note: This method iterates through all nodes and timeseries linked to price_area and creates them including the edges
        between the nodes. For example, if any of `plants` or `watercourses` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            price_area: Price area or sequence of price areas to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new price_area:

                >>> from cognite.powerops.client._generated.production import ProductionModelAPI
                >>> from cognite.powerops.client._generated.production.data_classes import PriceAreaApply
                >>> client = ProductionModelAPI()
                >>> price_area = PriceAreaApply(external_id="my_price_area", ...)
                >>> result = client.price_area.apply(price_area)

        """
        return self._apply(price_area, replace)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more price area.

        Args:
            external_id: External id of the price area to delete.
            space: The space where all the price area are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete price_area by id:

                >>> from cognite.powerops.client._generated.production import ProductionModelAPI
                >>> client = ProductionModelAPI()
                >>> client.price_area.delete("my_price_area")
        """
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> PriceArea | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> PriceAreaList:
        ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> PriceArea | PriceAreaList | None:
        """Retrieve one or more price areas by id(s).

        Args:
            external_id: External id or list of external ids of the price areas.
            space: The space where all the price areas are located.

        Returns:
            The requested price areas.

        Examples:

            Retrieve price_area by id:

                >>> from cognite.powerops.client._generated.production import ProductionModelAPI
                >>> client = ProductionModelAPI()
                >>> price_area = client.price_area.retrieve("my_price_area")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_quad=[
                (
                    self.plants_edge,
                    "plants",
                    dm.DirectRelationReference("power-ops", "PriceArea.plants"),
                    "outwards",
                ),
                (
                    self.watercourses_edge,
                    "watercourses",
                    dm.DirectRelationReference("power-ops", "PriceArea.watercourses"),
                    "outwards",
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: PriceAreaTextFields | Sequence[PriceAreaTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PriceAreaList:
        """Search price areas

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of price areas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results price areas matching the query.

        Examples:

           Search for 'my_price_area' in all text properties:

                >>> from cognite.powerops.client._generated.production import ProductionModelAPI
                >>> client = ProductionModelAPI()
                >>> price_areas = client.price_area.search('my_price_area')

        """
        filter_ = _create_price_area_filter(
            self._view_id,
            name,
            name_prefix,
            description,
            description_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _PRICEAREA_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: PriceAreaFields | Sequence[PriceAreaFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: PriceAreaTextFields | Sequence[PriceAreaTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregations: Aggregations
        | dm.aggregations.MetricAggregation
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: PriceAreaFields | Sequence[PriceAreaFields] | None = None,
        group_by: PriceAreaFields | Sequence[PriceAreaFields] = None,
        query: str | None = None,
        search_properties: PriceAreaTextFields | Sequence[PriceAreaTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
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
        | Sequence[Aggregations]
        | Sequence[dm.aggregations.MetricAggregation],
        property: PriceAreaFields | Sequence[PriceAreaFields] | None = None,
        group_by: PriceAreaFields | Sequence[PriceAreaFields] | None = None,
        query: str | None = None,
        search_property: PriceAreaTextFields | Sequence[PriceAreaTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across price areas

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of price areas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count price areas in space `my_space`:

                >>> from cognite.powerops.client._generated.production import ProductionModelAPI
                >>> client = ProductionModelAPI()
                >>> result = client.price_area.aggregate("count", space="my_space")

        """

        filter_ = _create_price_area_filter(
            self._view_id,
            name,
            name_prefix,
            description,
            description_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _PRICEAREA_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: PriceAreaFields,
        interval: float,
        query: str | None = None,
        search_property: PriceAreaTextFields | Sequence[PriceAreaTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for price areas

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of price areas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_price_area_filter(
            self._view_id,
            name,
            name_prefix,
            description,
            description_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _PRICEAREA_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        description: str | list[str] | None = None,
        description_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> PriceAreaList:
        """List/filter price areas

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            description: The description to filter on.
            description_prefix: The prefix of the description to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of price areas to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `plants` or `watercourses` external ids for the price areas. Defaults to True.

        Returns:
            List of requested price areas

        Examples:

            List price areas and limit to 5:

                >>> from cognite.powerops.client._generated.production import ProductionModelAPI
                >>> client = ProductionModelAPI()
                >>> price_areas = client.price_area.list(limit=5)

        """
        filter_ = _create_price_area_filter(
            self._view_id,
            name,
            name_prefix,
            description,
            description_prefix,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            retrieve_edges=retrieve_edges,
            edge_api_name_type_direction_quad=[
                (
                    self.plants_edge,
                    "plants",
                    dm.DirectRelationReference("power-ops", "PriceArea.plants"),
                    "outwards",
                ),
                (
                    self.watercourses_edge,
                    "watercourses",
                    dm.DirectRelationReference("power-ops", "PriceArea.watercourses"),
                    "outwards",
                ),
            ],
        )
