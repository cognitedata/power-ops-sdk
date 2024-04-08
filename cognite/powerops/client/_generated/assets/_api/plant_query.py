from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.assets.data_classes import (
    DomainModelCore,
    Plant,
    Watercourse,
    Reservoir,
)
from cognite.powerops.client._generated.assets.data_classes._generator import (
    Generator,
    _create_generator_filter,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .generator_query import GeneratorQueryAPI


class PlantQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("plant"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_read_class[Plant], ["*"])]),
                result_cls=Plant,
                max_retrieve_limit=limit,
            )
        )

    def generators(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_p_min: float | None = None,
        max_p_min: float | None = None,
        min_penstock: int | None = None,
        max_penstock: int | None = None,
        min_start_cost: float | None = None,
        max_start_cost: float | None = None,
        efficiency_curve: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_watercourse: bool = False,
        retrieve_inlet_reservoir: bool = False,
    ) -> GeneratorQueryAPI[T_DomainModelList]:
        """Query along the generator edges of the plant.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_p_min: The minimum value of the p min to filter on.
            max_p_min: The maximum value of the p min to filter on.
            min_penstock: The minimum value of the penstock to filter on.
            max_penstock: The maximum value of the penstock to filter on.
            min_start_cost: The minimum value of the start cost to filter on.
            max_start_cost: The maximum value of the start cost to filter on.
            efficiency_curve: The efficiency curve to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of generator edges to return. Defaults to 3. Set to -1, float("inf") or None
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
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("generators"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )

        view_id = self._view_by_read_class[Generator]
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_generator_filter(
            view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_p_min,
            max_p_min,
            min_penstock,
            max_penstock,
            min_start_cost,
            max_start_cost,
            efficiency_curve,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        if retrieve_watercourse:
            self._query_append_watercourse(from_)
        if retrieve_inlet_reservoir:
            self._query_append_inlet_reservoir(from_)
        return GeneratorQueryAPI(self._client, self._builder, self._view_by_read_class, node_filer, limit)

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
        view_id = self._view_by_read_class[Watercourse]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("watercourse"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[Plant].as_property_ref("watercourse"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=Watercourse,
                is_single_direct_relation=True,
            ),
        )

    def _query_append_inlet_reservoir(self, from_: str) -> None:
        view_id = self._view_by_read_class[Reservoir]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("inlet_reservoir"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[Plant].as_property_ref("inletReservoir"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=Reservoir,
                is_single_direct_relation=True,
            ),
        )
