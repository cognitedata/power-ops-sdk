from __future__ import annotations

import difflib
import warnings
from typing import (
    cast,
    ClassVar,
    Generic,
    Any,
    TypeVar,
    Union,
    Literal,
)

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.v1.data_classes._core.query.filter_classes import Filtering
from cognite.powerops.client._generated.v1.data_classes._core.base import (
    DomainModelList,
    T_DomainList,
    DomainRelationList,
    DomainModelCore,
    T_DomainModelList,
    DomainRelation,
    DomainModel,
)
from cognite.powerops.client._generated.v1.data_classes._core.constants import DEFAULT_QUERY_LIMIT
from cognite.powerops.client._generated.v1.data_classes._core.query.builder import QueryBuilder
from cognite.powerops.client._generated.v1.data_classes._core.query.processing import QueryUnpacker
from cognite.powerops.client._generated.v1.data_classes._core.query.step import QueryStep, ViewPropertyId


T_DomainListEnd = TypeVar("T_DomainListEnd", bound=Union[DomainModelList, DomainRelationList], covariant=True)


class QueryCore(Generic[T_DomainList, T_DomainListEnd]):
    _view_id: ClassVar[dm.ViewId]
    _result_list_cls_end: type[T_DomainListEnd]
    _result_cls: ClassVar[type[DomainModelCore]]

    def __init__(
        self,
        created_types: set[type],
        creation_path: "list[QueryCore]",
        client: CogniteClient,
        result_list_cls: type[T_DomainList],
        expression: dm.query.ResultSetExpression | None = None,
        view_filter: dm.filters.Filter | None = None,
        connection_name: str | None = None,
        connection_property: ViewPropertyId | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        reverse_expression: dm.query.ResultSetExpression | None = None,
    ):
        created_types.add(type(self))
        self._creation_path = creation_path[:] + [self]
        self._client = client
        self._result_list_cls = result_list_cls
        self._view_filter = view_filter
        self._expression = expression or dm.query.NodeResultSetExpression()
        self._reverse_expression = reverse_expression
        self._connection_name = connection_name
        self._connection_property = connection_property
        self._connection_type = connection_type
        self._filter_classes: list[Filtering] = []

    @property
    def _connection_names(self) -> set[str]:
        return {step._connection_name for step in self._creation_path if step._connection_name}

    @property
    def _is_reverseable(self) -> bool:
        return self._reverse_expression is not None

    def __getattr__(self, item: str) -> Any:
        if item in self._connection_names:
            nodes = [step._result_cls.__name__ for step in self._creation_path]
            raise ValueError(f"Circular reference detected. Cannot query a circular reference: {nodes}")
        elif self._connection_type == "reverse-list":
            raise ValueError(f"Cannot query across a reverse-list connection.")
        error_message = f"'{self.__class__.__name__}' object has no attribute '{item}'"
        attributes = [name for name in vars(self).keys() if not name.startswith("_")]
        if matches := difflib.get_close_matches(item, attributes):
            error_message += f". Did you mean one of: {matches}?"
        raise AttributeError(error_message)

    def _assemble_filter(self) -> dm.filters.Filter | None:
        filters: list[dm.filters.Filter] = [self._view_filter] if self._view_filter else []
        for filter_cls in self._filter_classes:
            if item := filter_cls._as_filter():
                filters.append(item)
        return dm.filters.And(*filters) if filters else None

    def _create_sort(self) -> list[dm.InstanceSort] | None:
        filters: list[tuple[dm.InstanceSort, int]] = []
        for filter_cls in self._filter_classes:
            item, priority = filter_cls._as_sort()
            if item:
                filters.append((item, priority))
        return [item for item, _ in sorted(filters, key=lambda x: x[1])] if filters else None

    def _has_limit_1(self) -> bool:
        return any(filter_cls._has_limit_1 for filter_cls in self._filter_classes)

    def _repr_html_(self) -> str:
        nodes = [step._result_cls.__name__ for step in self._creation_path]
        edges = [step._connection_name or "missing" for step in self._creation_path[1:]]
        last_connection_name = self._connection_name or "missing"
        w = 120
        h = 40
        circles = "    \n".join(f'<circle cx="{i * w + 40}" cy="{h}" r="2" />' for i in range(len(nodes)))
        circle_text = "    \n".join(
            f'<text x="{i * w + 40}" y="{h}" dy="-10">{node}</text>' for i, node in enumerate(nodes)
        )
        arrows = "    \n".join(
            '<path id="arrow-line"  marker-end="url(#head)" '
            f'stroke-width="2" fill="none" stroke="black" d="M{i*w+40},{h}, {i*w + 150} {h}" />'
            for i in range(len(edges))
        )
        arrow_text = "    \n".join(
            f'<text x="{i*w+40+120/2}" y="{h}" dy="-5">{edge}</text>' for i, edge in enumerate(edges)
        )

        return f"""<h5>Query</h5>
<div>
<svg height="50" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <marker
      id='head'
      orient="auto"
      markerWidth='3'
      markerHeight='4'
      refX='0.1'
      refY='2'
    >
      <path d='M0,0 V4 L2,2 Z' fill="black" />
    </marker>
  </defs>

    {arrows}

<g stroke="black" stroke-width="3" fill="black">
    {circles}
</g>
<g font-size="10" font-family="sans-serif" text-anchor="middle">
    {arrow_text}
</g>
<g font-size="10" font-family="sans-serif" text-anchor="middle">
    {circle_text}
</g>
</svg>
</div>
<p>Call <em>.list_full()</em> to return a list of {nodes[0].title()} and
<em>.list_{last_connection_name}()</em> to return a list of {nodes[-1].title()}.</p>
"""


class NodeQueryCore(QueryCore[T_DomainModelList, T_DomainListEnd]):
    _result_cls: ClassVar[type[DomainModel]]

    def list_full(self, limit: int = DEFAULT_QUERY_LIMIT) -> T_DomainModelList:
        builder = self._create_query(limit, return_step="first", try_reverse=True)
        builder.execute_query(self._client, remove_not_connected=True)
        unpacked = QueryUnpacker(builder).unpack()
        cls_ = self._creation_path[0]._result_cls
        return self._result_list_cls([cls_.model_validate(item) for item in unpacked])

    def _list(self, limit: int = DEFAULT_QUERY_LIMIT) -> T_DomainListEnd:
        builder = self._create_query(limit, return_step="last")
        for step in builder[:-1]:
            step.select = None
        builder.execute_query(self._client, remove_not_connected=False)
        unpacked = QueryUnpacker(builder[-1:]).unpack()
        return self._result_list_cls_end([self._result_cls.model_validate(item) for item in unpacked])  # type: ignore[return-value]

    def _dump_yaml(self) -> str:
        return self._create_query(DEFAULT_QUERY_LIMIT)._dump_yaml()

    def _create_query(
        self,
        limit: int,
        return_step: Literal["first", "last"] | None = None,
        try_reverse: bool = False,
    ) -> QueryBuilder:
        builder = QueryBuilder()
        from_: str | None = None
        is_last_reverse_list = False
        for item in self._creation_path:
            if is_last_reverse_list:
                raise ValueError(
                    "Cannot traverse past reverse direct relation of list. "
                    "This is a limitation with the modeling implementation of your data model."
                    "To do this query, you need to reimplement the data model and use an edge to "
                    "implement this connection instead of a reverse direct relation"
                )

            max_retrieve_limit = self._get_max_retrieve_limit(item, limit, return_step)
            name = builder.create_name(from_)
            if isinstance(item, NodeQueryCore) and isinstance(item._expression, dm.query.NodeResultSetExpression):
                # Root step or direct/reverse direct step
                step = QueryStep(
                    name=name,
                    expression=item._expression,
                    max_retrieve_limit=max_retrieve_limit,
                    connection_type=item._connection_type,
                    view_id=item._view_id,
                )
                if not item is self._creation_path[0]:
                    if not item._connection_property:
                        raise ValueError("Bug in pygen. Connection name is missing when building a query")
                    step.connection_property = item._connection_property
                step.expression.from_ = from_
                step.expression.filter = item._assemble_filter()
                step.expression.sort = item._create_sort()
                builder.append(step)
            elif isinstance(item, NodeQueryCore) and isinstance(item._expression, dm.query.EdgeResultSetExpression):
                # Edge without properties
                edge_name = name
                if not item._connection_property:
                    raise ValueError("Bug in pygen. Connection name is missing when building a query")
                connection_property = item._connection_property
                step = QueryStep(
                    name=edge_name,
                    expression=item._expression,
                    max_retrieve_limit=max_retrieve_limit,
                    connection_property=connection_property,
                )
                step.expression.from_ = from_
                builder.append(step)

                name = builder.create_name(edge_name)
                node_step = QueryStep(
                    name=name,
                    expression=dm.query.NodeResultSetExpression(
                        from_=edge_name,
                        filter=item._assemble_filter(),
                        sort=item._create_sort(),
                    ),
                    connection_property=ViewPropertyId(view=item._view_id, property="end_node"),
                    view_id=item._view_id,
                )
                builder.append(node_step)
            elif isinstance(item, EdgeQueryCore):
                # Edge with properties
                step = QueryStep(
                    name=name,
                    expression=cast(dm.query.EdgeResultSetExpression, item._expression),
                    connection_property=item._connection_property,
                    view_id=item._view_id,
                )
                step.expression.from_ = from_
                step.expression.filter = item._assemble_filter()
                step.expression.sort = item._create_sort()
                builder.append(step)
            else:
                raise TypeError(f"Unsupported query step type: {type(item._expression)}")

            is_last_reverse_list = item._connection_type == "reverse-list"
            from_ = name
        return builder

    def _get_max_retrieve_limit(
        self, item: QueryCore, limit: int, return_step: Literal["first", "last"] | None = None
    ) -> int:
        is_first = item is self._creation_path[0]
        if return_step == "first":
            if is_first and item._has_limit_1():
                if limit != DEFAULT_QUERY_LIMIT:
                    warnings.warn(
                        "When selecting earliest and latest, the limit is ignored.", UserWarning, stacklevel=2
                    )
                return 1
            elif is_first:
                return limit
            else:
                return -1
        elif return_step == "last":
            is_last = item is self._creation_path[-1]
            if is_last and item._has_limit_1():
                if limit != DEFAULT_QUERY_LIMIT:
                    warnings.warn(
                        "When selecting earliest and latest, the limit is ignored.", UserWarning, stacklevel=2
                    )
                return 1
            elif is_last:
                return limit
            else:
                return -1
        raise ValueError("Bug in Pygen. Invalid return_step. Please report")


class EdgeQueryCore(QueryCore[T_DomainList, T_DomainListEnd]):
    _result_cls: ClassVar[type[DomainRelation]]
