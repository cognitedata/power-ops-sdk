from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    Scenario,
    ModelTemplate,
    Commands,
)
from cognite.powerops.client._generated.v1.data_classes._mapping import (
    Mapping,
    _create_mapping_filter,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .mapping_query import MappingQueryAPI


class ScenarioQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("scenario"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_read_class[Scenario], ["*"])]),
                result_cls=Scenario,
                max_retrieve_limit=limit,
            )
        )

    def mappings_override(
        self,
        shop_path: str | list[str] | None = None,
        shop_path_prefix: str | None = None,
        retrieve: str | list[str] | None = None,
        retrieve_prefix: str | None = None,
        aggregation: str | list[str] | None = None,
        aggregation_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        external_id_prefix_edge: str | None = None,
        space_edge: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_model_template: bool = False,
        retrieve_commands: bool = False,
    ) -> MappingQueryAPI[T_DomainModelList]:
        """Query along the mappings override edges of the scenario.

        Args:
            shop_path: The shop path to filter on.
            shop_path_prefix: The prefix of the shop path to filter on.
            retrieve: The retrieve to filter on.
            retrieve_prefix: The prefix of the retrieve to filter on.
            aggregation: The aggregation to filter on.
            aggregation_prefix: The prefix of the aggregation to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            external_id_prefix_edge: The prefix of the external ID to filter on.
            space_edge: The space to filter on.
            filter: (Advanced) Filter applied to node. If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of mappings override edges to return. Defaults to 3. Set to -1, float("inf") or None
                to return all items.
            retrieve_model_template: Whether to retrieve the model template for each scenario or not.
            retrieve_commands: Whether to retrieve the command for each scenario or not.

        Returns:
            MappingQueryAPI: The query API for the mapping.
        """
        from .mapping_query import MappingQueryAPI

        from_ = self._builder[-1].name
        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("sp_powerops_types_temp", "Mapping"),
            external_id_prefix=external_id_prefix_edge,
            space=space_edge,
        )
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("mappings_override"),
                expression=dm.query.EdgeResultSetExpression(
                    filter=edge_filter,
                    from_=from_,
                    direction="outwards",
                ),
                select=dm.query.Select(),
                max_retrieve_limit=limit,
            )
        )

        view_id = self._view_by_read_class[Mapping]
        has_data = dm.filters.HasData(views=[view_id])
        node_filer = _create_mapping_filter(
            view_id,
            shop_path,
            shop_path_prefix,
            retrieve,
            retrieve_prefix,
            aggregation,
            aggregation_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        if retrieve_model_template:
            self._query_append_model_template(from_)
        if retrieve_commands:
            self._query_append_commands(from_)
        return MappingQueryAPI(self._client, self._builder, self._view_by_read_class, node_filer, limit)

    def query(
        self,
        retrieve_model_template: bool = False,
        retrieve_commands: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_model_template: Whether to retrieve the model template for each scenario or not.
            retrieve_commands: Whether to retrieve the command for each scenario or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_model_template:
            self._query_append_model_template(from_)
        if retrieve_commands:
            self._query_append_commands(from_)
        return self._query()

    def _query_append_model_template(self, from_: str) -> None:
        view_id = self._view_by_read_class[ModelTemplate]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("model_template"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[Scenario].as_property_ref("modelTemplate"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=ModelTemplate,
                is_single_direct_relation=True,
            ),
        )

    def _query_append_commands(self, from_: str) -> None:
        view_id = self._view_by_read_class[Commands]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("commands"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[Scenario].as_property_ref("commands"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=Commands,
                is_single_direct_relation=True,
            ),
        )
