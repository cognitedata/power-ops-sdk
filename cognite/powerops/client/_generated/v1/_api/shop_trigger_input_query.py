from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from cognite.client import data_modeling as dm, CogniteClient

from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    ShopTriggerInput,
    ShopCase,
    ShopPreprocessorInput,
)
from ._core import DEFAULT_QUERY_LIMIT, QueryBuilder, QueryStep, QueryAPI, T_DomainModelList, _create_edge_filter


class ShopTriggerInputQueryAPI(QueryAPI[T_DomainModelList]):
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
                name=self._builder.next_name("shop_trigger_input"),
                expression=dm.query.NodeResultSetExpression(
                    from_=self._builder[-1].name if self._builder else None,
                    filter=filter_,
                ),
                select=dm.query.Select([dm.query.SourceSelector(self._view_by_read_class[ShopTriggerInput], ["*"])]),
                result_cls=ShopTriggerInput,
                max_retrieve_limit=limit,
            )
        )

    def query(
        self,
        retrieve_case: bool = False,
        retrieve_preprocessor_input: bool = False,
    ) -> T_DomainModelList:
        """Execute query and return the result.

        Args:
            retrieve_case: Whether to retrieve the case for each shop trigger input or not.
            retrieve_preprocessor_input: Whether to retrieve the preprocessor input for each shop trigger input or not.

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
        view_id = self._view_by_read_class[ShopCase]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("case"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[ShopTriggerInput].as_property_ref("case"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=ShopCase,
                is_single_direct_relation=True,
            ),
        )

    def _query_append_preprocessor_input(self, from_: str) -> None:
        view_id = self._view_by_read_class[ShopPreprocessorInput]
        self._builder.append(
            QueryStep(
                name=self._builder.next_name("preprocessor_input"),
                expression=dm.query.NodeResultSetExpression(
                    filter=dm.filters.HasData(views=[view_id]),
                    from_=from_,
                    through=self._view_by_read_class[ShopTriggerInput].as_property_ref("preprocessorInput"),
                    direction="outwards",
                ),
                select=dm.query.Select([dm.query.SourceSelector(view_id, ["*"])]),
                max_retrieve_limit=-1,
                result_cls=ShopPreprocessorInput,
                is_single_direct_relation=True,
            ),
        )
