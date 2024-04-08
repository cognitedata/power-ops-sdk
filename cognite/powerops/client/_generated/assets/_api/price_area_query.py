from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.assets.data_classes import (
    DomainModelCore,
    PriceArea,
    BidMethod,
)
from cognite.powerops.client._generated.assets.data_classes._plant import (
    Plant,
    _create_plant_filter,
)
from cognite.powerops.client._generated.assets.data_classes._watercourse import (
    Watercourse,
    _create_watercourse_filter,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .plant_query import PlantQueryAPI
    from .watercourse_query import WatercourseQueryAPI


class PriceAreaQueryAPI(QueryAPI[T_DomainModelList]):
    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder[T_DomainModelList],
        view_by_read_class: dict[type[DomainModelCore], dm.ViewId],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder, view_by_read_class)

        self._builder.append(
            QueryStep(
                name=self._builder.next_name("price_area"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_read_class[PriceArea], ["*"])]),
                result_cls=PriceArea,
                max_retrieve_limit=limit,
            )
        )

    def plants(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        min_head_loss_factor: float | None = None,
        max_head_loss_factor: float | None = None,
        min_outlet_level: float | None = None,
        max_outlet_level: float | None = None,
        min_p_max: float | None = None,
        max_p_max: float | None = None,
        min_p_min: float | None = None,
        max_p_min: float | None = None,
        watercourse: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_connection_losses: float | None = None,
        max_connection_losses: float | None = None,
        inlet_reservoir: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_default_method_day_ahead: bool = False,
    ) -> PlantQueryAPI[T_DomainModelList]:
        """Query along the plant edges of the price area.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_ordering: The minimum value of the ordering to filter on.
            max_ordering: The maximum value of the ordering to filter on.
            min_head_loss_factor: The minimum value of the head loss factor to filter on.
            max_head_loss_factor: The maximum value of the head loss factor to filter on.
            min_outlet_level: The minimum value of the outlet level to filter on.
            max_outlet_level: The maximum value of the outlet level to filter on.
            min_p_max: The minimum value of the p max to filter on.
            max_p_max: The maximum value of the p max to filter on.
            min_p_min: The minimum value of the p min to filter on.
            max_p_min: The maximum value of the p min to filter on.
            watercourse: The watercourse to filter on.
            min_connection_losses: The minimum value of the connection loss to filter on.
            max_connection_losses: The maximum value of the connection loss to filter on.
            inlet_reservoir: The inlet reservoir to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of plant edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.
            retrieve_default_method_day_ahead: Whether to retrieve the default method day ahead for each price area or not.

        Returns:
            PlantQueryAPI: The query API for the plant.
        """
        from .plant_query import PlantQueryAPI

        from_ = self._builder[-1].name
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power-ops-types", "isSubAssetOf"),
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("plants"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )

        view_id = self._view_by_read_class[Plant]
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_plant_filter(
            view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            min_head_loss_factor,
            max_head_loss_factor,
            min_outlet_level,
            max_outlet_level,
            min_p_max,
            max_p_max,
            min_p_min,
            max_p_min,
            watercourse,
            min_connection_losses,
            max_connection_losses,
            inlet_reservoir,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        if retrieve_default_method_day_ahead:
            self._query_append_default_method_day_ahead(from_)
        return PlantQueryAPI(self._client, self._builder, self._view_by_read_class, node_filer, limit)

    def watercourses(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_penalty_limit: float | None = None,
        max_penalty_limit: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_default_method_day_ahead: bool = False,
    ) -> WatercourseQueryAPI[T_DomainModelList]:
        """Query along the watercourse edges of the price area.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_penalty_limit: The minimum value of the penalty limit to filter on.
            max_penalty_limit: The maximum value of the penalty limit to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of watercourse edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.
            retrieve_default_method_day_ahead: Whether to retrieve the default method day ahead for each price area or not.

        Returns:
            WatercourseQueryAPI: The query API for the watercourse.
        """
        from .watercourse_query import WatercourseQueryAPI

        from_ = self._builder[-1].name
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power-ops-types", "isSubAssetOf"),
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("watercourses"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )

        view_id = self._view_by_read_class[Watercourse]
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_watercourse_filter(
            view_id,
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
        if retrieve_default_method_day_ahead:
            self._query_append_default_method_day_ahead(from_)
        return WatercourseQueryAPI(self._client, self._builder, self._view_by_read_class, node_filer, limit)

    def query(
        self,
        retrieve_default_method_day_ahead: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_default_method_day_ahead: Whether to retrieve the default method day ahead for each price area or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_default_method_day_ahead:
            self._query_append_default_method_day_ahead(from_)
        return self._query()

    def _query_append_default_method_day_ahead(self, from_: str) -> None:
        view_id = self._view_by_read_class[BidMethod]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("default_method_day_ahead"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[PriceArea].as_property_ref("defaultMethodDayAhead"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=BidMethod,
                is_single_direct_relation=True,
            ),
        )
