from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import TYPE_CHECKING, cast

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    ShopTriggerInput,
    ShopCase,
    ShopPreprocessorInput,
)
from cognite.powerops.client._generated.v1.data_classes._core import (
    DEFAULT_QUERY_LIMIT,
    ViewPropertyId,
    T_DomainModel,
    T_DomainModelList,
    QueryBuilder,
    QueryStep,
)
from cognite.powerops.client._generated.v1._api._core import (
    QueryAPI,
    _create_edge_filter,
)



class ShopTriggerInputQueryAPI(QueryAPI[T_DomainModel, T_DomainModelList]):
    _view_id = dm.ViewId("power_ops_core", "ShopTriggerInput", "1")

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

    def query(
        self,
        retrieve_case: bool = False,
        retrieve_preprocessor_input: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_case: Whether to retrieve the
                case for each
                shop trigger input or not.
            retrieve_preprocessor_input: Whether to retrieve the
                preprocessor input for each
                shop trigger input or not.

        Returns:
            The list of the source nodes of the query.

        """
        from_ = self._builder[-1].name
        if retrieve_case:
            self._query_append_case(from_)
        if retrieve_preprocessor_input:
            self._query_append_preprocessor_input(from_)
        return self._query()

    def _query_append_case(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("case"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[ShopCase._view_id]),
                ),
                view_id=ShopCase._view_id,
                connection_property=ViewPropertyId(self._view_id, "case"),
            ),
        )
    def _query_append_preprocessor_input(self, from_: str) -> None:
        self._builder.append(
            QueryStep(
                name=self._builder.create_name(from_),
                expression=dm.query.NodeResultSetExpression(
                    from_=from_,
                    through=self._view_id.as_property_ref("preprocessorInput"),
                    direction="outwards",
                    filter=dm.filters.HasData(views=[ShopPreprocessorInput._view_id]),
                ),
                view_id=ShopPreprocessorInput._view_id,
                connection_property=ViewPropertyId(self._view_id, "preprocessorInput"),
            ),
        )
