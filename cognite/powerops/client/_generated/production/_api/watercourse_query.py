from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.production.data_classes import (
    DomainModelCore,
    Watercourse,
    WatercourseShop,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .plant_query import PlantQueryAPI


class WatercourseQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("watercourse"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_read_class[Watercourse], ["*"])]),
                result_cls=Watercourse,
                max_retrieve_limit=limit,
            )
        )

    def plants(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_shop: bool = False,
    ) -> PlantQueryAPI[T_DomainModelList]:
        """Query along the plant edges of the watercourse.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of plant edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_shop: Whether to retrieve the shop for each watercourse or not.

        Returns:
            PlantQueryAPI: The query API for the plant.
        """
        from .plant_query import PlantQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power-ops", "Watercourse.plants"),
            external_id_prefix=external_id_prefix,
            space=space,
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
        if retrieve_shop:
            self._query_append_shop(from_)
        return PlantQueryAPI(self._client, self._builder, self._view_by_read_class, None, limit)

    def query(
        self,
        retrieve_shop: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_shop: Whether to retrieve the shop for each watercourse or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_shop:
            self._query_append_shop(from_)
        return self._query()

    def _query_append_shop(self, from_: str) -> None:
        view_id = self._view_by_read_class[WatercourseShop]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("shop"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[Watercourse].as_property_ref("shop"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=WatercourseShop,
            ),
        )
