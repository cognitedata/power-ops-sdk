from __future__ import annotations

import datetime
import math
import time
import warnings
from abc import ABC
from collections import defaultdict
from collections.abc import Collection, MutableSequence, Iterable, Sequence
from contextlib import suppress
from dataclasses import dataclass, field
from typing import (
    cast,
    ClassVar,
    Generic,
    Any,
    Iterator,
    TypeVar,
    overload,
    Union,
    SupportsIndex,
    Literal,
)

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes._base import CogniteObject
from cognite.client.data_classes.aggregations import Count
from cognite.client.data_classes.data_modeling.instances import Instance
from cognite.client.exceptions import CogniteAPIError

from cognite.powerops.client._generated.v1.data_classes._core.base import (
    DomainModelList,
    T_DomainList,
    DomainRelationList,
    DomainModelCore,
    T_DomainModelList,
    DomainRelation,
    DomainModel,
)
from cognite.powerops.client._generated.v1.data_classes._core.constants import (
    _NotSetSentinel,
    DEFAULT_QUERY_LIMIT,
    DEFAULT_INSTANCE_SPACE,
    ACTUAL_INSTANCE_QUERY_LIMIT,
    INSTANCE_QUERY_LIMIT,
    IN_FILTER_CHUNK_SIZE,
    MINIMUM_ESTIMATED_SECONDS_BEFORE_PRINT_PROGRESS,
    PRINT_PROGRESS_PER_N_NODES,
    SEARCH_LIMIT,
)
from cognite.powerops.client._generated.v1.data_classes._core.helpers import as_node_id


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
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{item}'")

    def _assemble_filter(self) -> dm.filters.Filter | None:
        filters: list[dm.filters.Filter] = [self._view_filter] if self._view_filter else []
        for filter_cls in self._filter_classes:
            if item := filter_cls._as_filter():
                filters.append(item)
        return dm.filters.And(*filters) if filters else None

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
            f'<path id="arrow-line"  marker-end="url(#head)" stroke-width="2" fill="none" stroke="black" d="M{i*w+40},{h}, {i*w + 150} {h}" />'
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
        builder = self._create_query(limit, self._result_list_cls, return_step="first", try_reverse=True)
        builder.execute_query(self._client, remove_not_connected=True)
        return builder.unpack()

    def _list(self, limit: int = DEFAULT_QUERY_LIMIT) -> T_DomainListEnd:
        builder = self._create_query(limit, cast(type[DomainModelList], self._result_list_cls_end), return_step="last")
        for step in builder[:-1]:
            step.select = None
        builder[-1].max_retrieve_limit = limit
        builder.execute_query(self._client, remove_not_connected=False)
        return builder.unpack()

    def _dump_yaml(self) -> str:
        return self._create_query(DEFAULT_QUERY_LIMIT, self._result_list_cls)._dump_yaml()

    def _create_query(
        self,
        limit: int,
        result_list_cls: type[DomainModelList],
        return_step: Literal["first", "last"] | None = None,
        try_reverse: bool = False,
    ) -> DataClassQueryBuilder:
        builder = DataClassQueryBuilder(result_list_cls, return_step=return_step)
        from_: str | None = None
        first: bool = True
        is_last_reverse_list = False
        for item in self._creation_path:
            if is_last_reverse_list:
                raise ValueError(
                    "Cannot traverse past reverse direct relation of list. "
                    "This is a limitation of the modeling implementation in your data model."
                    "To do this query, you need to reimplement the data model and use an edge to "
                    "implement this connection instead of a reverse direct relation"
                )
            name = builder.create_name(from_)
            max_retrieve_limit = limit if first else -1
            step: QueryStep
            if isinstance(item, NodeQueryCore) and isinstance(item._expression, dm.query.NodeResultSetExpression):
                step = NodeQueryStep(
                    name=name,
                    expression=item._expression,
                    result_cls=item._result_cls,
                    max_retrieve_limit=max_retrieve_limit,
                    connection_type=item._connection_type,
                )
                step.expression.from_ = from_
                step.expression.filter = item._assemble_filter()
                builder.append(step)
            elif isinstance(item, NodeQueryCore) and isinstance(item._expression, dm.query.EdgeResultSetExpression):
                edge_name = name
                step = EdgeQueryStep(name=edge_name, expression=item._expression, max_retrieve_limit=max_retrieve_limit)
                step.expression.from_ = from_
                builder.append(step)

                name = builder.create_name(edge_name)
                node_step = NodeQueryStep(
                    name=name,
                    expression=dm.query.NodeResultSetExpression(
                        from_=edge_name,
                        filter=item._assemble_filter(),
                    ),
                    result_cls=item._result_cls,
                )
                builder.append(node_step)
            elif isinstance(item, EdgeQueryCore):
                step = EdgeQueryStep(
                    name=name,
                    expression=cast(dm.query.EdgeResultSetExpression, item._expression),
                    result_cls=item._result_cls,
                )
                step.expression.from_ = from_
                step.expression.filter = item._assemble_filter()
                builder.append(step)
            else:
                raise TypeError(f"Unsupported query step type: {type(item._expression)}")

            is_last_reverse_list = item._connection_type == "reverse-list"
            first = False
            from_ = name
        return builder


class EdgeQueryCore(QueryCore[T_DomainList, T_DomainListEnd]):
    _result_cls: ClassVar[type[DomainRelation]]


@dataclass(frozen=True)
class ViewPropertyId(CogniteObject):
    view: dm.ViewId
    property: str

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> "ViewPropertyId":
        return cls(
            view=dm.ViewId.load(resource["view"]),
            property=resource["identifier"],
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        return {
            "view": self.view.dump(camel_case=camel_case, include_type=False),
            "identifier": self.property,
        }


class QueryReducingBatchSize(UserWarning):
    """Raised when a query is too large and the batch size must be reduced."""

    ...


def chunker(sequence: Sequence, chunk_size: int) -> Iterator[Sequence]:
    """
    Split a sequence into chunks of size chunk_size.

    Args:
        sequence: The sequence to split.
        chunk_size: The size of each chunk.

    Returns:
        An iterator over the chunks.

    """
    for i in range(0, len(sequence), chunk_size):
        yield sequence[i : i + chunk_size]


@dataclass
class Progress:
    total: float | None
    _last_print: float = field(default=0.0, init=False)
    _estimated_nodes_per_second: float = field(default=0.0, init=False)
    _is_large_query: bool = field(default=False, init=False)

    def _update_nodes_per_second(self, last_node_count: int, last_execution_time: float) -> None:
        # Estimate the number of nodes per second using exponential moving average
        last_batch_nodes_per_second = last_node_count / last_execution_time
        if self._estimated_nodes_per_second == 0.0:
            self._estimated_nodes_per_second = last_batch_nodes_per_second
        else:
            self._estimated_nodes_per_second = (
                0.1 * last_batch_nodes_per_second + 0.9 * self._estimated_nodes_per_second
            )

    def log(self, last_node_count: int, last_execution_time: float, total_retrieved: int) -> None:
        if self.total is None:
            return
        self._update_nodes_per_second(last_node_count, last_execution_time)
        # Estimate the time to completion
        remaining_nodes = self.total - total_retrieved
        remaining_time = remaining_nodes / self._estimated_nodes_per_second
        if self._is_large_query and (total_retrieved - self._last_print) > PRINT_PROGRESS_PER_N_NODES:
            estimate = datetime.timedelta(seconds=round(remaining_time, 0))
            print(
                f"Progress: {total_retrieved:,}/{self.total:,} nodes retrieved. "
                f"Estimated time to completion: {estimate}"
            )
            self._last_print = total_retrieved
        if self._is_large_query is False and remaining_time > MINIMUM_ESTIMATED_SECONDS_BEFORE_PRINT_PROGRESS:
            self._is_large_query = True
            print("Large query detected. Will print progress.")


class QueryStep:
    def __init__(
        self,
        name: str,
        expression: dm.query.ResultSetExpression,
        view_id: dm.ViewId | None = None,
        max_retrieve_limit: int = -1,
        select: dm.query.Select | None | type[_NotSetSentinel] = _NotSetSentinel,
        raw_filter: dm.Filter | None = None,
        connection_type: Literal["reverse-list"] | None = None,
        view_property: ViewPropertyId | None = None,
        selected_properties: list[str] | None = None,
    ):
        self.name = name
        self.expression = expression
        self.view_id = view_id
        self.max_retrieve_limit = max_retrieve_limit
        self.select: dm.query.Select | None
        if select is _NotSetSentinel:
            try:
                self.select = self._default_select()
            except NotImplementedError:
                raise ValueError(f"You need to provide a select to instantiate a {type(self).__name__}") from None
        else:
            self.select = select  # type: ignore[assignment]
        self.raw_filter = raw_filter
        self.connection_type = connection_type
        self.view_property = view_property
        self.selected_properties = selected_properties
        self._max_retrieve_batch_limit = ACTUAL_INSTANCE_QUERY_LIMIT
        self.cursor: str | None = None
        self.total_retrieved: int = 0
        self.last_batch_count: int = 0
        self.results: list[Instance] = []

    def _default_select(self) -> dm.query.Select:
        if self.view_id is None:
            return dm.query.Select()
        else:
            return dm.query.Select([dm.query.SourceSelector(self.view_id, ["*"])])

    @property
    def is_queryable(self) -> bool:
        # We cannot query across reverse-list connections
        return self.connection_type != "reverse-list"

    @property
    def from_(self) -> str | None:
        return self.expression.from_

    @property
    def is_single_direct_relation(self) -> bool:
        return isinstance(self.expression, dm.query.NodeResultSetExpression) and self.expression.through is not None

    @property
    def node_expression(self) -> dm.query.NodeResultSetExpression | None:
        if isinstance(self.expression, dm.query.NodeResultSetExpression):
            return self.expression
        return None

    @property
    def edge_expression(self) -> dm.query.EdgeResultSetExpression | None:
        if isinstance(self.expression, dm.query.EdgeResultSetExpression):
            return self.expression
        return None

    @property
    def node_results(self) -> Iterable[dm.Node]:
        return (item for item in self.results if isinstance(item, dm.Node))

    @property
    def edge_results(self) -> Iterable[dm.Edge]:
        return (item for item in self.results if isinstance(item, dm.Edge))

    def update_expression_limit(self) -> None:
        if self.is_unlimited:
            self.expression.limit = self._max_retrieve_batch_limit
        else:
            self.expression.limit = max(min(INSTANCE_QUERY_LIMIT, self.max_retrieve_limit - self.total_retrieved), 0)

    def reduce_max_batch_limit(self) -> bool:
        self._max_retrieve_batch_limit = max(1, self._max_retrieve_batch_limit // 2)
        return self._max_retrieve_batch_limit > 1

    @property
    def is_unlimited(self) -> bool:
        return self.max_retrieve_limit in {None, -1, math.inf}

    @property
    def is_finished(self) -> bool:
        return (
            (not self.is_unlimited and self.total_retrieved >= self.max_retrieve_limit)
            or self.cursor is None
            or self.last_batch_count == 0
        )

    def count_total(self, cognite_client: CogniteClient) -> float | None:
        if self.view_id is None:
            # Cannot count the total without a view
            return None
        try:
            return cognite_client.data_modeling.instances.aggregate(
                self.view_id, Count("externalId"), filter=self.raw_filter
            ).value
        except CogniteAPIError:
            return None

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r}, from={self.from_!r}, results={len(self.results)})"


class QueryBuilder(list, MutableSequence[QueryStep]):
    """This is a helper class to build and execute a query. It is responsible for
    doing the paging of the query and keeping track of the results."""

    def __init__(self, steps: Collection[QueryStep] | None = None):
        super().__init__(steps or [])

    def _reset(self):
        for expression in self:
            expression.total_retrieved = 0
            expression.cursor = None
            expression.results = []

    def _update_expression_limits(self) -> None:
        for expression in self:
            expression.update_expression_limit()

    def _build(self) -> tuple[dm.query.Query, list[QueryStep], set[str]]:
        with_ = {step.name: step.expression for step in self if step.is_queryable}
        select = {step.name: step.select for step in self if step.select is not None and step.is_queryable}
        cursors = self._cursors

        step_by_name = {step.name: step for step in self}
        search: list[QueryStep] = []
        temporary_select: set[str] = set()
        for step in self:
            if step.is_queryable:
                continue
            if step.node_expression is not None:
                search.append(step)
                # Ensure that select is set for the parent
                if step.from_ in select or step.from_ is None:
                    continue
                view_id = step_by_name[step.from_].view_id
                if view_id is None:
                    continue
                select[step.from_] = dm.query.Select([dm.query.SourceSelector(view_id, ["*"])])
                temporary_select.add(step.from_)
        return dm.query.Query(with_=with_, select=select, cursors=cursors), search, temporary_select

    def _dump_yaml(self) -> str:
        return self._build()[0].dump_yaml()

    @property
    def _cursors(self) -> dict[str, str | None]:
        return {expression.name: expression.cursor for expression in self if expression.is_queryable}

    def _update(self, batch: dm.query.QueryResult):
        for expression in self:
            if expression.name not in batch:
                continue
            expression.last_batch_count = len(batch[expression.name])
            expression.total_retrieved += expression.last_batch_count
            expression.cursor = batch.cursors.get(expression.name)
            expression.results.extend(batch[expression.name].data)

    def _reduce_max_batch_limit(self) -> bool:
        for expression in self:
            if not expression.reduce_max_batch_limit():
                return False
        return True

    def execute_query(self, client: CogniteClient, remove_not_connected: bool = False) -> dict[str, list[Instance]]:
        self._reset()
        query, to_search, temp_select = self._build()

        if not self:
            raise ValueError("No query steps to execute")

        select_step = next((step for step in self if step.select is not None), None)
        if select_step is None:
            raise ValueError("No select step found in the query")

        total = select_step.count_total(client)

        progress = Progress(total)
        while True:
            self._update_expression_limits()
            query.cursors = self._cursors
            t0 = time.time()
            try:
                batch = client.data_modeling.instances.query(query)
            except CogniteAPIError as e:
                if e.code == 408:
                    # Too big query, try to reduce the limit
                    if self._reduce_max_batch_limit():
                        continue
                    new_limit = select_step._max_retrieve_batch_limit
                    warnings.warn(
                        f"Query is too large, reducing batch size to {new_limit:,}, and trying again",
                        QueryReducingBatchSize,
                        stacklevel=2,
                    )

                raise e

            self._fetch_reverse_direct_relation_of_lists(client, to_search, batch)

            for name in temp_select:
                batch.pop(name, None)

            last_execution_time = time.time() - t0

            self._update(batch)
            if remove_not_connected and len(self) > 1:
                removed = _QueryResultCleaner(self).clean()
                for step in self:
                    step.total_retrieved -= removed.get(step.name, 0)

            if select_step.is_finished:
                break

            progress.log(len(batch[select_step.name]), last_execution_time, select_step.total_retrieved)

        return {step.name: step.results for step in self}

    @staticmethod
    def _fetch_reverse_direct_relation_of_lists(
        client: CogniteClient, to_search: list[QueryStep], batch: dm.query.QueryResult
    ) -> None:
        """Reverse direct relations for lists are not supported by the query API.
        This method fetches them separately."""
        for step in to_search:
            if step.from_ is None or step.from_ not in batch:
                continue
            item_ids = [node.as_id() for node in batch[step.from_].data]
            if not item_ids:
                continue

            view_id = step.view_id
            expression = step.node_expression
            if view_id is None or expression is None:
                raise ValueError(
                    "Invalid state of the query. Search should always be a node expression with view properties"
                )
            if expression.through is None:
                raise ValueError("Missing through set in a reverse-list query")
            limit = SEARCH_LIMIT if step.is_unlimited else min(step.max_retrieve_limit, SEARCH_LIMIT)

            step_result = dm.NodeList[dm.Node]([])
            for item_ids_chunk in chunker(item_ids, IN_FILTER_CHUNK_SIZE):
                is_items = dm.filters.In(view_id.as_property_ref(expression.through.property), item_ids_chunk)
                is_selected = is_items if step.raw_filter is None else dm.filters.And(is_items, step.raw_filter)

                chunk_result = client.data_modeling.instances.search(
                    view_id, properties=None, filter=is_selected, limit=limit
                )
                step_result.extend(chunk_result)

            batch[step.name] = dm.NodeListWithCursor(step_result, None)
        return None

    def get_from(self) -> str | None:
        if len(self) == 0:
            return None
        return self[-1].name

    def create_name(self, from_: str | None) -> str:
        if from_ is None:
            return "0"
        return f"{from_}_{len(self)}"

    def append(self, __object: QueryStep, /) -> None:
        # Extra validation to ensure all assumptions are met
        if len(self) == 0:
            if __object.from_ is not None:
                raise ValueError("The first step should not have a 'from_' value")
        else:
            if __object.from_ is None:
                raise ValueError("The 'from_' value should be set")
        super().append(__object)

    def extend(self, __iterable: Iterable[QueryStep], /) -> None:
        for item in __iterable:
            self.append(item)

    # The implementations below are to get proper type hints
    def __iter__(self) -> Iterator[QueryStep]:
        return super().__iter__()

    @overload
    def __getitem__(self, item: SupportsIndex) -> QueryStep: ...

    @overload
    def __getitem__(self, item: slice) -> "QueryBuilder": ...

    def __getitem__(self, item: SupportsIndex | slice) -> "QueryStep | QueryBuilder":
        value = super().__getitem__(item)
        if isinstance(item, slice):
            return QueryBuilder(value)  # type: ignore[arg-type]
        return cast(QueryStep, value)


class _QueryResultCleaner:
    """Remove nodes and edges that are not connected through the entire query"""

    def __init__(self, steps: list[QueryStep]):
        self._tree = self._create_tree(steps)
        self._root = steps[0]

    @classmethod
    def _create_tree(cls, steps: list[QueryStep]) -> dict[str, list[QueryStep]]:
        tree: dict[str, list[QueryStep]] = defaultdict(list)
        for step in steps:
            if step.from_ is None:
                continue
            tree[step.from_].append(step)
        return dict(tree)

    def clean(self) -> dict[str, int]:
        removed_by_name: dict[str, int] = defaultdict(int)
        self._clean(self._root, removed_by_name)
        return dict(removed_by_name)

    @staticmethod
    def as_node_id(direct_relation: dm.DirectRelationReference | dict[str, str]) -> dm.NodeId:
        if isinstance(direct_relation, dict):
            return dm.NodeId(direct_relation["space"], direct_relation["externalId"])

        return dm.NodeId(direct_relation.space, direct_relation.external_id)

    def _clean(self, step: QueryStep, removed_by_name: dict[str, int]) -> tuple[set[dm.NodeId], str | None]:
        if step.name not in self._tree:
            # Leaf Node
            # Nothing to clean, just return the node ids with the connection property
            direct_relation: str | None = None
            if step.node_expression and (through := step.node_expression.through) is not None:
                direct_relation = through.property
                if step.node_expression.direction == "inwards":
                    return {
                        node_id for item in step.node_results for node_id in self._get_relations(item, direct_relation)
                    }, None

            return {item.as_id() for item in step.results}, direct_relation  # type: ignore[attr-defined]

        expected_ids_by_property: dict[str | None, set[dm.NodeId]] = {}
        for child in self._tree[step.name]:
            child_ids, property_id = self._clean(child, removed_by_name)
            if property_id not in expected_ids_by_property:
                expected_ids_by_property[property_id] = child_ids
            else:
                expected_ids_by_property[property_id] |= child_ids

        if step.node_expression is not None:
            filtered_results: list[Instance] = []
            for node in step.node_results:
                if self._is_connected_node(node, expected_ids_by_property):
                    filtered_results.append(node)
                else:
                    removed_by_name[step.name] += 1
            step.results = filtered_results
            direct_relation = None if step.node_expression.through is None else step.node_expression.through.property
            return {node.as_id() for node in step.node_results}, direct_relation

        if step.edge_expression:
            if len(expected_ids_by_property) > 1 or None not in expected_ids_by_property:
                raise RuntimeError(f"Invalid state of {type(self).__name__}")
            expected_ids = expected_ids_by_property[None]
            before = len(step.results)
            if step.edge_expression.direction == "outwards":
                step.results = [edge for edge in step.edge_results if self.as_node_id(edge.end_node) in expected_ids]
                connected_node_ids = {self.as_node_id(edge.start_node) for edge in step.edge_results}
            else:  # inwards
                step.results = [edge for edge in step.edge_results if self.as_node_id(edge.start_node) in expected_ids]
                connected_node_ids = {self.as_node_id(edge.end_node) for edge in step.edge_results}
            removed_by_name[step.name] += before - len(step.results)
            return connected_node_ids, None

        raise TypeError(f"Unsupported query step type: {type(step)}")

    @classmethod
    def _is_connected_node(cls, node: dm.Node, expected_ids_by_property: dict[str | None, set[dm.NodeId]]) -> bool:
        if not expected_ids_by_property:
            return True
        if None in expected_ids_by_property:
            if node.as_id() in expected_ids_by_property[None]:
                return True
            if len(expected_ids_by_property) == 1:
                return False
        node_properties = next(iter(node.properties.values()))
        for property_id, expected_ids in expected_ids_by_property.items():
            if property_id is None:
                continue
            value = node_properties.get(property_id)
            if value is None:
                continue
            elif isinstance(value, list):
                if {cls.as_node_id(item) for item in value if isinstance(item, dict)} & expected_ids:
                    return True
            elif isinstance(value, dict) and cls.as_node_id(value) in expected_ids:
                return True
        return False

    @classmethod
    def _get_relations(cls, node: dm.Node, property_id: str) -> Iterable[dm.NodeId]:
        if property_id is None:
            return {node.as_id()}
        value = next(iter(node.properties.values())).get(property_id)
        if isinstance(value, list):
            return [cls.as_node_id(item) for item in value if isinstance(item, dict)]
        elif isinstance(value, dict):
            return [cls.as_node_id(value)]
        return []


class NodeQueryStep(QueryStep):
    def __init__(
        self,
        name: str,
        expression: dm.query.NodeResultSetExpression,
        result_cls: type[DomainModel],
        max_retrieve_limit: int = -1,
        select: dm.query.Select | None | type[_NotSetSentinel] = _NotSetSentinel,
        raw_filter: dm.Filter | None = None,
        connection_type: Literal["reverse-list"] | None = None,
    ):
        self.result_cls = result_cls
        super().__init__(name, expression, result_cls._view_id, max_retrieve_limit, select, raw_filter, connection_type)

    def unpack(self) -> dict[dm.NodeId | str, DomainModel]:
        return {
            (
                instance.as_id() if instance.space != DEFAULT_INSTANCE_SPACE else instance.external_id
            ): self.result_cls.from_instance(instance)
            for instance in cast(list[dm.Node], self.results)
        }

    @property
    def node_results(self) -> list[dm.Node]:
        return cast(list[dm.Node], self.results)

    @property
    def node_expression(self) -> dm.query.NodeResultSetExpression:
        return cast(dm.query.NodeResultSetExpression, self.expression)


class EdgeQueryStep(QueryStep):
    def __init__(
        self,
        name: str,
        expression: dm.query.EdgeResultSetExpression,
        result_cls: type[DomainRelation] | None = None,
        max_retrieve_limit: int = -1,
        select: dm.query.Select | None | type[_NotSetSentinel] = _NotSetSentinel,
        raw_filter: dm.Filter | None = None,
    ):
        self.result_cls = result_cls
        view_id = result_cls._view_id if result_cls is not None else None
        super().__init__(name, expression, view_id, max_retrieve_limit, select, raw_filter, None)

    def unpack(self) -> dict[dm.NodeId, list[dm.Edge | DomainRelation]]:
        output: dict[dm.NodeId, list[dm.Edge | DomainRelation]] = defaultdict(list)
        for edge in cast(list[dm.Edge], self.results):
            edge_source = edge.start_node if self.expression.direction == "outwards" else edge.end_node
            value = self.result_cls.from_instance(edge) if self.result_cls is not None else edge
            output[as_node_id(edge_source)].append(value)  # type: ignore[arg-type]
        return output

    @property
    def edge_results(self) -> list[dm.Edge]:
        return cast(list[dm.Edge], self.results)

    @property
    def edge_expression(self) -> dm.query.EdgeResultSetExpression:
        return cast(dm.query.EdgeResultSetExpression, self.expression)


class DataClassQueryBuilder(QueryBuilder, Generic[T_DomainModelList]):
    """This is a helper class to build and execute a query. It is responsible for
    doing the paging of the query and keeping track of the results."""

    def __init__(
        self,
        result_cls: type[T_DomainModelList] | None,
        steps: Collection[QueryStep] | None = None,
        return_step: Literal["first", "last"] | None = None,
    ):
        super().__init__(steps or [])
        self._result_list_cls = result_cls
        self._return_step: Literal["first", "last"] | None = return_step

    def unpack(self) -> T_DomainModelList:
        if self._result_list_cls is None:
            raise ValueError("No result class set, unable to unpack results")
        selected = [step for step in self if step.select is not None]
        if len(selected) == 0:
            return self._result_list_cls([])
        elif len(selected) == 1:
            # Validated in the append method
            if self._return_step == "first":
                selected_step = cast(NodeQueryStep, self[0])
            elif self._return_step == "last":
                selected_step = cast(NodeQueryStep, self[-1])
            else:
                raise ValueError(f"Invalid return_step: {self._return_step}")
            return self._result_list_cls(selected_step.unpack().values())
        # More than one step, we need to unpack the nodes and edges
        nodes_by_from: dict[str | None, dict[dm.NodeId | str, DomainModel]] = defaultdict(dict)
        edges_by_from: dict[str, dict[dm.NodeId, list[dm.Edge | DomainRelation]]] = defaultdict(dict)
        for step in reversed(self):
            # Validated in the append method
            from_ = cast(str, step.from_)
            if isinstance(step, EdgeQueryStep):
                edges_by_from[from_].update(step.unpack())
                if step.name in nodes_by_from:
                    nodes_by_from[from_].update(nodes_by_from[step.name])
                    del nodes_by_from[step.name]
            elif isinstance(step, NodeQueryStep):
                unpacked = step.unpack()
                nodes_by_from[from_].update(unpacked)  # type: ignore[arg-type]
                if step.name in nodes_by_from or step.name in edges_by_from:
                    step.result_cls._update_connections(
                        unpacked,  # type: ignore[arg-type]
                        nodes_by_from.get(step.name, {}),  # type: ignore[arg-type]
                        edges_by_from.get(step.name, {}),
                    )
        if self._return_step == "first":
            return self._result_list_cls(nodes_by_from[None].values())
        elif self._return_step == "last" and self[-1].from_ in nodes_by_from:
            return self._result_list_cls(nodes_by_from[self[-1].from_].values())
        elif self._return_step == "last":
            raise ValueError("Cannot return the last step when the last step is an edge query")
        else:
            raise ValueError(f"Invalid return_step: {self._return_step}")

    def append(self, __object: QueryStep, /) -> None:
        # Extra validation to ensure all assumptions are met
        if len(self) == 0:
            if __object.from_ is not None:
                raise ValueError("The first step should not have a 'from_' value")
            if self._result_list_cls is None:
                if self._return_step is None:
                    self._return_step = "first"
            else:
                if not isinstance(__object, NodeQueryStep):
                    raise ValueError("The first step should be a NodeQueryStep")
                # If the first step is a NodeQueryStep, and matches the instance
                # in the result_list_cls we can return the result from the first step
                # Alternative is result_cls is not set, then we also assume that the first step
                if self._return_step is None:
                    if __object.result_cls is self._result_list_cls._INSTANCE:
                        self._return_step = "first"
                    else:
                        # If not, we assume that the last step is the one we want to return
                        self._return_step = "last"
        else:
            if __object.from_ is None:
                raise ValueError("The 'from_' value should be set")
        super().append(__object)

    def extend(self, __iterable: Iterable[QueryStep], /) -> None:
        for item in __iterable:
            self.append(item)

    # The implementations below are to get proper type hints
    def __iter__(self) -> Iterator[QueryStep]:
        return super().__iter__()

    @overload
    def __getitem__(self, item: SupportsIndex) -> QueryStep: ...

    @overload
    def __getitem__(self, item: slice) -> DataClassQueryBuilder[T_DomainModelList]: ...

    def __getitem__(self, item: SupportsIndex | slice) -> QueryStep | DataClassQueryBuilder[T_DomainModelList]:
        value = super().__getitem__(item)
        if isinstance(item, slice):
            return DataClassQueryBuilder(self._result_list_cls, value)  # type: ignore[arg-type]
        return cast(QueryStep, value)


T_QueryCore = TypeVar("T_QueryCore")


class Filtering(Generic[T_QueryCore], ABC):
    def __init__(self, query: T_QueryCore, prop_path: list[str] | tuple[str, ...]):
        self._query = query
        self._prop_path = prop_path
        self._filter: dm.Filter | None = None

    def _raise_if_filter_set(self):
        if self._filter is not None:
            raise ValueError("Filter has already been set")

    def _as_filter(self) -> dm.Filter | None:
        return self._filter


class StringFilter(Filtering[T_QueryCore]):
    def equals(self, value: str) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Equals(self._prop_path, value)
        return self._query

    def prefix(self, prefix: str) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Prefix(self._prop_path, prefix)
        return self._query

    def in_(self, values: list[str]) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.In(self._prop_path, values)
        return self._query


class BooleanFilter(Filtering[T_QueryCore]):
    def equals(self, value: bool) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Equals(self._prop_path, value)
        return self._query


class IntFilter(Filtering[T_QueryCore]):
    def range(self, gte: int | None, lte: int | None) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Range(self._prop_path, gte=gte, lte=lte)
        return self._query


class FloatFilter(Filtering[T_QueryCore]):
    def range(self, gte: float | None, lte: float | None) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Range(self._prop_path, gte=gte, lte=lte)
        return self._query


class TimestampFilter(Filtering[T_QueryCore]):
    def range(self, gte: datetime.datetime | None, lte: datetime.datetime | None) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Range(
            self._prop_path,
            gte=gte.isoformat(timespec="milliseconds") if gte else None,
            lte=lte.isoformat(timespec="milliseconds") if lte else None,
        )
        return self._query


class DateFilter(Filtering[T_QueryCore]):
    def range(self, gte: datetime.date | None, lte: datetime.date | None) -> T_QueryCore:
        self._raise_if_filter_set()
        self._filter = dm.filters.Range(
            self._prop_path,
            gte=gte.isoformat() if gte else None,
            lte=lte.isoformat() if lte else None,
        )
        return self._query
