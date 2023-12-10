from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.assets.data_classes import (
    DomainModelApply,
    PriceArea,
    PriceAreaApply,
    BidMethod,
    BidMethodApply,
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
        view_by_write_class: dict[type[DomainModelApply], dm.ViewId],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder, view_by_write_class)

        self._builder.append(
            QueryStep(
                name=self._builder.next_name("price_area"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_write_class[PriceAreaApply], ["*"])]),
                result_cls=PriceArea,
                max_retrieve_limit=limit,
            )
        )

    def plants(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_default_method_day_ahead: bool = False,
    ) -> PlantQueryAPI[T_DomainModelList]:
        """Query along the plant edges of the price area.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of plant edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_default_method_day_ahead: Whether to retrieve the default method day ahead for each price area or not.

        Returns:
            PlantQueryAPI: The query API for the plant.
        """
        from .plant_query import PlantQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power-ops-types", "isSubAssetOf"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("plants"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_default_method_day_ahead:
            self._query_append_default_method_day_ahead(from_)
        return PlantQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def watercourses(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_default_method_day_ahead: bool = False,
    ) -> WatercourseQueryAPI[T_DomainModelList]:
        """Query along the watercourse edges of the price area.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of watercourse edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_default_method_day_ahead: Whether to retrieve the default method day ahead for each price area or not.

        Returns:
            WatercourseQueryAPI: The query API for the watercourse.
        """
        from .watercourse_query import WatercourseQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power-ops-types", "isSubAssetOf"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("watercourses"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_default_method_day_ahead:
            self._query_append_default_method_day_ahead(from_)
        return WatercourseQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

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
        view_id = self._view_by_write_class[BidMethodApply]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("default_method_day_ahead"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_write_class[PriceAreaApply].as_property_ref("default_method_day_ahead"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=BidMethod,
            ),
        )
