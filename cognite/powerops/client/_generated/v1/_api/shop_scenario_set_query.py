from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    ShopScenarioSet,
    DateSpecification,
    DateSpecification,
)
from cognite.powerops.client._generated.v1.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    ViewPropertyId,
    T_DomainModel,
    T_DomainModelList,
    QueryBuilder,
    QueryStep,
)
from cognite.powerops.client._generated.v1.data_classes._shop_scenario import (
    _create_shop_scenario_filter,
)
from cognite.powerops.client._generated.v1._api._core import (
    QueryAPI,
    _create_edge_filter,
)

if TYPE_CHECKING:
    from cognite.powerops.client._generated.v1._api.shop_scenario_query import ShopScenarioQueryAPI


class ShopScenarioSetQueryAPI(QueryAPI[T_DomainModel, T_DomainModelList]):
    _view_id = dm.ViewId("power_ops_core", "ShopScenarioSet", "1")

    def __init__(
        self,
        client: CogniteClient,
        builder: QueryBuilder,
        result_cls: type[T_DomainModel],
        result_list_cls: type[T_DomainModelList],
        connection_property: ViewPropertyId | None = None,
        filter_: dm.filters.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
    ):
        super().__init__(client, builder, result_cls, result_list_cls)
        from_ = self._builder.get_from()
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    filter=filter_,
                ),
                max_retrieve_limit=limit,
                view_id=self._view_id,
                connection_property=connection_property,
            )
        )
    def scenarios(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        model: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        commands: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        time_resolution: str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference | Sequence[str | tuple[str, str] | dm.NodeId | dm.DirectRelationReference] | None = None,
        source: str | list[str] | None = None,
        source_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int = DEFAULT_QUERY_LIMIT,
        retrieve_start_specification: bool = False,
        retrieve_end_specification: bool = False,
    ) -> ShopScenarioQueryAPI[T_DomainModel, T_DomainModelList]:
        """Query along the scenario edges of the shop scenario set.

        Args:
            name:
            name_prefix:
            model:
            commands:
            time_resolution:
            source:
            source_prefix:
            external_id_prefix:
            space:
            external_id_prefix_edge:
            space_edge:
            filter: (Advanced) Filter applied to node. If the filtering available in the
                above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of scenario edges to return.
                Defaults to 3. Set to -1, float("inf") or None to return all items.
            retrieve_start_specification: Whether to retrieve the start specification
                for each shop scenario set or not.
            retrieve_end_specification: Whether to retrieve the end specification
                for each shop scenario set or not.

        Returns:
            ShopScenarioQueryAPI: The query API for the shop scenario.
        """
        from .shop_scenario_query import ShopScenarioQueryAPI

        # from is a string as we added a node query step in the __init__ method
        from_ = cast(str, self._builder.get_from())
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("power_ops_types", "ShopScenarioSet.scenarios"),
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                max_retrieve_limit=limit,
                connection_property=ViewPropertyId(self._view_id, "scenarios"),
            )
        )

        view_id = ShopScenarioQueryAPI._view_id
        has_data = dm.filters.HasData(views=[view_id])
        node_filter = _create_shop_scenario_filter(
            view_id,
            name,
            name_prefix,
            model,
            commands,
            time_resolution,
            source,
            source_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        if retrieve_start_specification:
            self._query_append_start_specification(from_)
        if retrieve_end_specification:
            self._query_append_end_specification(from_)
        return (ShopScenarioQueryAPI(
            self._client,
            self._builder,
            self._result_cls,
            self._result_list_cls,
            ViewPropertyId(self._view_id, "end_node"),
            node_filter,
            limit,
        ))

    def query(
        self,
        retrieve_start_specification: bool = False,
        retrieve_end_specification: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_start_specification: Whether to retrieve the
                start specification for each
                shop scenario set or not.
            retrieve_end_specification: Whether to retrieve the
                end specification for each
                shop scenario set or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_start_specification:
            self._query_append_start_specification(from_)
        if retrieve_end_specification:
            self._query_append_end_specification(from_)
        return self._query()

    def _query_append_start_specification(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("startSpecification"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[DateSpecification._view_id]),
                ),
                view_id=DateSpecification._view_id,
                connection_property=ViewPropertyId(self._view_id, "startSpecification"),
            ),
        )
    def _query_append_end_specification(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("endSpecification"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[DateSpecification._view_id]),
                ),
                view_id=DateSpecification._view_id,
                connection_property=ViewPropertyId(self._view_id, "endSpecification"),
            ),
        )
