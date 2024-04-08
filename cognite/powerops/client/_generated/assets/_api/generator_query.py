from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.assets.data_classes import (
    DomainModelCore,
    Generator,
    GeneratorEfficiencyCurve,
)
from cognite.powerops.client._generated.assets.data_classes._turbine_efficiency_curve import (
    TurbineEfficiencyCurve,
    _create_turbine_efficiency_curve_filter,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .turbine_efficiency_curve_query import TurbineEfficiencyCurveQueryAPI


class GeneratorQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("generator"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_read_class[Generator], ["*"])]),
                result_cls=Generator,
                max_retrieve_limit=limit,
            )
        )

    def turbine_curves(
        self,
        min_head: float | None = None,
        max_head: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_efficiency_curve: bool = False,
    ) -> TurbineEfficiencyCurveQueryAPI[T_DomainModelList]:
        """Query along the turbine curve edges of the generator.

        Args:
            min_head: The minimum value of the head to filter on.
            max_head: The maximum value of the head to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of turbine curve edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.
            retrieve_efficiency_curve: Whether to retrieve the efficiency curve for each generator or not.

        Returns:
            TurbineEfficiencyCurveQueryAPI: The query API for the turbine efficiency curve.
        """
        from .turbine_efficiency_curve_query import TurbineEfficiencyCurveQueryAPI

        from_ = self._builder[-1].name
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power-ops-types", "isSubAssetOf"),
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("turbine_curves"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )

        view_id = self._view_by_read_class[TurbineEfficiencyCurve]
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_turbine_efficiency_curve_filter(
            view_id,
            min_head,
            max_head,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        if retrieve_efficiency_curve:
            self._query_append_efficiency_curve(from_)
        return TurbineEfficiencyCurveQueryAPI(self._client, self._builder, self._view_by_read_class, node_filer, limit)

    def query(
        self,
        retrieve_efficiency_curve: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_efficiency_curve: Whether to retrieve the efficiency curve for each generator or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_efficiency_curve:
            self._query_append_efficiency_curve(from_)
        return self._query()

    def _query_append_efficiency_curve(self, from_: str) -> None:
        view_id = self._view_by_read_class[GeneratorEfficiencyCurve]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("efficiency_curve"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[Generator].as_property_ref("efficiencyCurve"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=GeneratorEfficiencyCurve,
                is_single_direct_relation=True,
            ),
        )
