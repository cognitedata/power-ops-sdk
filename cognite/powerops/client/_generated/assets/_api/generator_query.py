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
    TurbineEfficiencyCurve,
    TurbineEfficiencyCurveApply,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter


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

    def query(
        self,
        retrieve_generator_efficiency_curve: bool = False,
        retrieve_turbine_efficiency_curve: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_generator_efficiency_curve: Whether to retrieve the generator efficiency curve for each generator or not.
            retrieve_turbine_efficiency_curve: Whether to retrieve the turbine efficiency curve for each generator or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_generator_efficiency_curve:
            self._query_append_generator_efficiency_curve(from_)
        if retrieve_turbine_efficiency_curve:
            self._query_append_turbine_efficiency_curve(from_)
        return self._query()

    def _query_append_generator_efficiency_curve(self, from_: str) -> None:
        view_id = self._view_by_write_class[GeneratorEfficiencyCurveApply]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("generator_efficiency_curve"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_write_class[GeneratorApply].as_property_ref("generator_efficiency_curve"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=GeneratorEfficiencyCurve,
            ),
        )

    def _query_append_turbine_efficiency_curve(self, from_: str) -> None:
        view_id = self._view_by_write_class[TurbineEfficiencyCurveApply]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("turbine_efficiency_curve"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_write_class[GeneratorApply].as_property_ref("turbine_efficiency_curve"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=TurbineEfficiencyCurve,
            ),
        )
