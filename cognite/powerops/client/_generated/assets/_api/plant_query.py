from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.assets.data_classes import (
    DomainModelApply,
    Plant,
    PlantApply,
    Watercourse,
    WatercourseApply,
    Reservoir,
    ReservoirApply,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .generator_query import GeneratorQueryAPI


class PlantQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("plant"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_write_class[PlantApply], ["*"])]),
                result_cls=Plant,
                max_retrieve_limit=limit,
            )
        )

    def generators(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_watercourse: bool = False,
        retrieve_inlet_reservoir: bool = False,
    ) -> GeneratorQueryAPI[T_DomainModelList]:
        """Query along the generator edges of the plant.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of generator edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_watercourse: Whether to retrieve the watercourse for each plant or not.
            retrieve_inlet_reservoir: Whether to retrieve the inlet reservoir for each plant or not.

        Returns:
            GeneratorQueryAPI: The query API for the generator.
        """
        from .generator_query import GeneratorQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power-ops-types", "isSubAssetOf"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("generators"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_watercourse:
            self._query_append_watercourse(from_)
        if retrieve_inlet_reservoir:
            self._query_append_inlet_reservoir(from_)
        return GeneratorQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

    def query(
        self,
        retrieve_watercourse: bool = False,
        retrieve_inlet_reservoir: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_watercourse: Whether to retrieve the watercourse for each plant or not.
            retrieve_inlet_reservoir: Whether to retrieve the inlet reservoir for each plant or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_watercourse:
            self._query_append_watercourse(from_)
        if retrieve_inlet_reservoir:
            self._query_append_inlet_reservoir(from_)
        return self._query()

    def _query_append_watercourse(self, from_: str) -> None:
        view_id = self._view_by_write_class[WatercourseApply]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("watercourse"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_write_class[PlantApply].as_property_ref("watercourse"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=Watercourse,
            ),
        )

    def _query_append_inlet_reservoir(self, from_: str) -> None:
        view_id = self._view_by_write_class[ReservoirApply]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("inlet_reservoir"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_write_class[PlantApply].as_property_ref("inlet_reservoir"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=Reservoir,
            ),
        )
