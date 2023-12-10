from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.assets.data_classes import (
    DomainModelApply,
    Generator,
    GeneratorApply,
    GeneratorEfficiencyCurve,
    GeneratorEfficiencyCurveApply,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .turbine_efficiency_curve_query import TurbineEfficiencyCurveQueryAPI


class GeneratorQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("generator"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_write_class[GeneratorApply], ["*"])]),
                result_cls=Generator,
                max_retrieve_limit=limit,
            )
        )

    def turbine_curves(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_efficiency_curve: bool = False,
    ) -> TurbineEfficiencyCurveQueryAPI[T_DomainModelList]:
        """Query along the turbine curve edges of the generator.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of turbine curve edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_efficiency_curve: Whether to retrieve the efficiency curve for each generator or not.

        Returns:
            TurbineEfficiencyCurveQueryAPI: The query API for the turbine efficiency curve.
        """
        from .turbine_efficiency_curve_query import TurbineEfficiencyCurveQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power-ops-types", "isSubAssetOf"),
            external_id_prefix=external_id_prefix,
            space=space,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("turbine_curves"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )
        if retrieve_efficiency_curve:
            self._query_append_efficiency_curve(from_)
        return TurbineEfficiencyCurveQueryAPI(self._client, self._builder, self._view_by_write_class, None, limit)

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
        view_id = self._view_by_write_class[GeneratorEfficiencyCurveApply]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("efficiency_curve"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_write_class[GeneratorApply].as_property_ref("efficiency_curve"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=GeneratorEfficiencyCurve,
            ),
        )
