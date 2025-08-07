from __future__ import annotations

import warnings
from collections.abc import Iterator, Sequence
from typing import Any, ClassVar, Literal, overload

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite.powerops.client._generated.v1._api._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_CHUNK_SIZE,
    instantiate_classes,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
)
from cognite.powerops.client._generated.v1.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DEFAULT_QUERY_LIMIT,
    QueryBuildStepFactory,
    QueryBuilder,
    QueryExecutor,
    QueryUnpacker,
    ViewPropertyId,
)
from cognite.powerops.client._generated.v1.data_classes._generator_efficiency_curve import (
    GeneratorEfficiencyCurveQuery,
    _GENERATOREFFICIENCYCURVE_PROPERTIES_BY_FIELD,
    _create_generator_efficiency_curve_filter,
)
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModel,
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    GeneratorEfficiencyCurve,
    GeneratorEfficiencyCurveWrite,
    GeneratorEfficiencyCurveFields,
    GeneratorEfficiencyCurveList,
    GeneratorEfficiencyCurveWriteList,
    GeneratorEfficiencyCurveTextFields,
)


class GeneratorEfficiencyCurveAPI(NodeAPI[GeneratorEfficiencyCurve, GeneratorEfficiencyCurveWrite, GeneratorEfficiencyCurveList, GeneratorEfficiencyCurveWriteList]):
    _view_id = dm.ViewId("power_ops_core", "GeneratorEfficiencyCurve", "1")
    _properties_by_field: ClassVar[dict[str, str]] = _GENERATOREFFICIENCYCURVE_PROPERTIES_BY_FIELD
    _class_type = GeneratorEfficiencyCurve
    _class_list = GeneratorEfficiencyCurveList
    _class_write_list = GeneratorEfficiencyCurveWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)


    @overload
    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> GeneratorEfficiencyCurve | None: ...

    @overload
    def retrieve(
        self,
        external_id: SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> GeneratorEfficiencyCurveList: ...

    def retrieve(
        self,
        external_id: str | dm.NodeId | tuple[str, str] | SequenceNotStr[str | dm.NodeId | tuple[str, str]],
        space: str = DEFAULT_INSTANCE_SPACE,
    ) -> GeneratorEfficiencyCurve | GeneratorEfficiencyCurveList | None:
        """Retrieve one or more generator efficiency curves by id(s).

        Args:
            external_id: External id or list of external ids of the generator efficiency curves.
            space: The space where all the generator efficiency curves are located.

        Returns:
            The requested generator efficiency curves.

        Examples:

            Retrieve generator_efficiency_curve by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> generator_efficiency_curve = client.generator_efficiency_curve.retrieve(
                ...     "my_generator_efficiency_curve"
                ... )

        """
        return self._retrieve(
            external_id,
            space,
        )

    def search(
        self,
        query: str,
        properties: GeneratorEfficiencyCurveTextFields | SequenceNotStr[GeneratorEfficiencyCurveTextFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: GeneratorEfficiencyCurveFields | SequenceNotStr[GeneratorEfficiencyCurveFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> GeneratorEfficiencyCurveList:
        """Search generator efficiency curves

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of generator efficiency curves to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allows you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results generator efficiency curves matching the query.

        Examples:

           Search for 'my_generator_efficiency_curve' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> generator_efficiency_curves = client.generator_efficiency_curve.search(
                ...     'my_generator_efficiency_curve'
                ... )

        """
        filter_ = _create_generator_efficiency_curve_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(
            query=query,
            properties=properties,
            filter_=filter_,
            limit=limit,
            sort_by=sort_by,  # type: ignore[arg-type]
            direction=direction,
            sort=sort,
        )

    @overload
    def aggregate(
        self,
        aggregate: Aggregations | dm.aggregations.MetricAggregation,
        group_by: None = None,
        property: GeneratorEfficiencyCurveFields | SequenceNotStr[GeneratorEfficiencyCurveFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.AggregatedNumberedValue: ...

    @overload
    def aggregate(
        self,
        aggregate: SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: None = None,
        property: GeneratorEfficiencyCurveFields | SequenceNotStr[GeneratorEfficiencyCurveFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]: ...

    @overload
    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: GeneratorEfficiencyCurveFields | SequenceNotStr[GeneratorEfficiencyCurveFields],
        property: GeneratorEfficiencyCurveFields | SequenceNotStr[GeneratorEfficiencyCurveFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList: ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: GeneratorEfficiencyCurveFields | SequenceNotStr[GeneratorEfficiencyCurveFields] | None = None,
        property: GeneratorEfficiencyCurveFields | SequenceNotStr[GeneratorEfficiencyCurveFields] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across generator efficiency curves

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of generator efficiency curves to return. Defaults to 25.
                Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write
                your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count generator efficiency curves in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.generator_efficiency_curve.aggregate("count", space="my_space")

        """

        filter_ = _create_generator_efficiency_curve_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            aggregate=aggregate,
            group_by=group_by,  # type: ignore[arg-type]
            properties=property,  # type: ignore[arg-type]
            query=None,
            search_properties=None,
            limit=limit,
            filter=filter_,
        )

    def histogram(
        self,
        property: GeneratorEfficiencyCurveFields,
        interval: float,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for generator efficiency curves

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of generator efficiency curves to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_generator_efficiency_curve_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            property,
            interval,
            None,
            None,
            limit,
            filter_,
        )

    def select(self) -> GeneratorEfficiencyCurveQuery:
        """Start selecting from generator efficiency curves."""
        return GeneratorEfficiencyCurveQuery(self._client)

    def _build(
        self,
        filter_: dm.Filter | None,
        limit: int | None,
        retrieve_connections: Literal["skip", "identifier", "full"],
        sort: list[InstanceSort] | None = None,
        chunk_size: int | None = None,
    ) -> QueryExecutor:
        builder = QueryBuilder()
        factory = QueryBuildStepFactory(builder.create_name, view_id=self._view_id, edge_connection_property="end_node")
        builder.append(factory.root(
            filter=filter_,
            sort=sort,
            limit=limit,
            max_retrieve_batch_limit=chunk_size,
            has_container_fields=True,
        ))
        return builder.build()

    def iterate(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        filter: dm.Filter | None = None,
        limit: int | None = None,
        cursors: dict[str, str | None] | None = None,
    ) -> Iterator[GeneratorEfficiencyCurveList]:
        """Iterate over generator efficiency curves

        Args:
            chunk_size: The number of generator efficiency curves to return in each iteration. Defaults to 100.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            limit: Maximum number of generator efficiency curves to return. Defaults to None, which will return all items.
            cursors: (Advanced) Cursor to use for pagination. This can be used to resume an iteration from a
                specific point. See example below for more details.

        Returns:
            Iteration of generator efficiency curves

        Examples:

            Iterate generator efficiency curves in chunks of 100 up to 2000 items:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for generator_efficiency_curves in client.generator_efficiency_curve.iterate(chunk_size=100, limit=2000):
                ...     for generator_efficiency_curve in generator_efficiency_curves:
                ...         print(generator_efficiency_curve.external_id)

            Iterate generator efficiency curves in chunks of 100 sorted by external_id in descending order:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for generator_efficiency_curves in client.generator_efficiency_curve.iterate(
                ...     chunk_size=100,
                ...     sort_by="external_id",
                ...     direction="descending",
                ... ):
                ...     for generator_efficiency_curve in generator_efficiency_curves:
                ...         print(generator_efficiency_curve.external_id)

            Iterate generator efficiency curves in chunks of 100 and use cursors to resume the iteration:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> for first_iteration in client.generator_efficiency_curve.iterate(chunk_size=100, limit=2000):
                ...     print(first_iteration)
                ...     break
                >>> for generator_efficiency_curves in client.generator_efficiency_curve.iterate(
                ...     chunk_size=100,
                ...     limit=2000,
                ...     cursors=first_iteration.cursors,
                ... ):
                ...     for generator_efficiency_curve in generator_efficiency_curves:
                ...         print(generator_efficiency_curve.external_id)

        """
        warnings.warn(
            "The `iterate` method is in alpha and is subject to breaking changes without prior notice.", stacklevel=2
        )
        filter_ = _create_generator_efficiency_curve_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        yield from self._iterate(chunk_size, filter_, limit, "skip", cursors=cursors)

    def list(
        self,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: GeneratorEfficiencyCurveFields | Sequence[GeneratorEfficiencyCurveFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> GeneratorEfficiencyCurveList:
        """List/filter generator efficiency curves

        Args:
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of generator efficiency curves to return.
                Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient,
                you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            List of requested generator efficiency curves

        Examples:

            List generator efficiency curves and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> generator_efficiency_curves = client.generator_efficiency_curve.list(limit=5)

        """
        filter_ = _create_generator_efficiency_curve_filter(
            self._view_id,
            external_id_prefix,
            space,
            filter,
        )
        sort_input =  self._create_sort(sort_by, direction, sort)  # type: ignore[arg-type]
        return self._list(limit=limit,  filter=filter_, sort=sort_input)
