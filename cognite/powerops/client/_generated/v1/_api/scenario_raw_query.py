from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    ScenarioRaw,
    ModelTemplate,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter

if TYPE_CHECKING:
    from .mapping_query import MappingQueryAPI


class ScenarioRawQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("scenario_raw"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_read_class[ScenarioRaw], ["*"])]),
                result_cls=ScenarioRaw,
                max_retrieve_limit=limit,
            )
        )

    def mappings_override(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        retrieve_model_template: bool = False,
    ) -> MappingQueryAPI[T_DomainModelList]:
        """Query along the mappings override edges of the scenario raw.

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of mappings override edges to return. Defaults to 25. Set to -1, float("inf") or None
                to return all items.
            retrieve_model_template: Whether to retrieve the model template for each scenario raw or not.

        Returns:
            MappingQueryAPI: The query API for the mapping.
        """
        from .mapping_query import MappingQueryAPI

        from_ = self._builder[-1].name

        edge_filter = _create_edge_filter(
            dm.DirectRelationReference("sp_powerops_types", "Mapping"),
            external_id_prefix=external_id_prefix,
            space=space,
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
        if retrieve_model_template:
            self._query_append_model_template(from_)
        return MappingQueryAPI(self._client, self._builder, self._view_by_read_class, None, limit)

    def query(
        self,
        retrieve_model_template: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_model_template: Whether to retrieve the model template for each scenario raw or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_model_template:
            self._query_append_model_template(from_)
        return self._query()

    def _query_append_model_template(self, from_: str) -> None:
        view_id = self._view_by_read_class[ModelTemplate]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("model_template"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[ScenarioRaw].as_property_ref("modelTemplate"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=ModelTemplate,
            ),
        )
