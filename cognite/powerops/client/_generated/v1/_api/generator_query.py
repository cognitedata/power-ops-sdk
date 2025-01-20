from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    Generator,
    GeneratorEfficiencyCurve,
)
from cognite.powerops.client._generated.v1.data_classes._turbine_efficiency_curve import (
    TurbineEfficiencyCurve,
    _create_turbine_efficiency_curve_filter,
)
from cognite.powerops.client._generated.v1._api._core import (
    DEFAULT_QUERY_LIMIT,
    EdgeQueryStep,
    NodeQueryStep,
    DataClassQueryBuilder,
    QueryAPI,
    T_DomainModelList,
    _create_edge_filter,
)

if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1._api.turbine_efficiency_curve_query import TurbineEfficiencyCurveQueryAPI


class GeneratorQueryAPI(QueryAPI[T_DomainModelList]):
    _view_id = dm.ViewId("power_ops_core", "Generator", "1")

    def __init__(
        self,
        client: CogniteClient,
        builder: DataClassQueryBuilder[T_DomainModelList],
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder)
        from_ = self._builder.get_from()
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    filter=filter_,
                ),
                result_cls=Generator,
                max_retrieve_limit=limit,
            )
        )
    def turbine_efficiency_curves(
        self,
        min_head: float | None = None,
        max_head: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        retrieve_generator_efficiency_curve: bool = False,
    ) -> TurbineEfficiencyCurveQueryAPI[T_DomainModelList]:
        """Query along the turbine efficiency curve edges of the generator.

        Args:
            min_head: The minimum value of the head to filter on.
            max_head: The maximum value of the head to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of turbine efficiency curve edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.
            retrieve_generator_efficiency_curve: Whether to retrieve the generator efficiency curve for each generator or not.

        Returns:
            TurbineEfficiencyCurveQueryAPI: The query API for the turbine efficiency curve.
        """
        from .turbine_efficiency_curve_query import TurbineEfficiencyCurveQueryAPI

        # from is a string as we added a node query step in the __init__ method
        from_ = cast(str, self._builder.get_from())
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "isSubAssetOf"),
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            EdgeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                max_retrieve_limit=limit,
            )
        )

        view_id = TurbineEfficiencyCurveQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_turbine_efficiency_curve_filter(
            view_id,
            min_head,
            max_head,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        if retrieve_generator_efficiency_curve:
            self._query_append_generator_efficiency_curve(from_)
        return TurbineEfficiencyCurveQueryAPI(self._client, self._builder, node_filer, limit)

    def query(
        self,
        retrieve_generator_efficiency_curve: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_generator_efficiency_curve: Whether to retrieve the generator efficiency curve for each generator or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_generator_efficiency_curve:
            self._query_append_generator_efficiency_curve(from_)
        return self._query()

    def _query_append_generator_efficiency_curve(self, from_: str) -> None:
        self._builder.append(
            NodeQueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("generatorEfficiencyCurve"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[GeneratorEfficiencyCurve._view_id]),
                ),
                result_cls=GeneratorEfficiencyCurve,
            ),
        )
