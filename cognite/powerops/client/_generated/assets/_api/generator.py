from __future__ import annotations

from collections.abc import Sequence
from typing import overload
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.assets.data_classes._core import DEFAULT_INSTANCE_SPACE
from cognite.powerops.client._generated.assets.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    Generator,
    GeneratorWrite,
    GeneratorFields,
    GeneratorList,
    GeneratorWriteList,
    GeneratorTextFields,
)
from cognite.powerops.client._generated.assets.data_classes._generator import (
    _GENERATOR_PROPERTIES_BY_FIELD,
    _create_generator_filter,
)
from ._core import (
    DEFAULT_LIMIT_READ,
    DEFAULT_QUERY_LIMIT,
    Aggregations,
    NodeAPI,
    SequenceNotStr,
    QueryStep,
    QueryBuilder,
)
from .generator_turbine_curves import GeneratorTurbineCurvesAPI
from .generator_start_stop_cost import GeneratorStartStopCostAPI
from .generator_is_available_time_series import GeneratorIsAvailableTimeSeriesAPI
from .generator_query import GeneratorQueryAPI


class GeneratorAPI(NodeAPI[Generator, GeneratorWrite, GeneratorList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[Generator]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Generator,
            class_list=GeneratorList,
            class_write_list=GeneratorWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.turbine_curves_edge = GeneratorTurbineCurvesAPI(client)
        self.start_stop_cost = GeneratorStartStopCostAPI(client, view_id)
        self.is_available_time_series = GeneratorIsAvailableTimeSeriesAPI(client, view_id)

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_p_min: float | None = None,
        max_p_min: float | None = None,
        min_penstock: int | None = None,
        max_penstock: int | None = None,
        min_start_cost: float | None = None,
        max_start_cost: float | None = None,
        efficiency_curve: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> GeneratorQueryAPI[GeneratorList]:
        """Query starting at generators.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_p_min: The minimum value of the p min to filter on.
            max_p_min: The maximum value of the p min to filter on.
            min_penstock: The minimum value of the penstock to filter on.
            max_penstock: The maximum value of the penstock to filter on.
            min_start_cost: The minimum value of the start cost to filter on.
            max_start_cost: The maximum value of the start cost to filter on.
            efficiency_curve: The efficiency curve to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of generators to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for generators.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_generator_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_p_min,
            max_p_min,
            min_penstock,
            max_penstock,
            min_start_cost,
            max_start_cost,
            efficiency_curve,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(GeneratorList)
        return GeneratorQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        generator: GeneratorWrite | Sequence[GeneratorWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) generators.

        Note: This method iterates through all nodes and timeseries linked to generator and creates them including the edges
        between the nodes. For example, if any of `turbine_curves` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            generator: Generator or sequence of generators to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new generator:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> from cognite.powerops.client._generated.assets.data_classes import GeneratorWrite
                >>> client = PowerAssetAPI()
                >>> generator = GeneratorWrite(external_id="my_generator", ...)
                >>> result = client.generator.apply(generator)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.generator.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(generator, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more generator.

        Args:
            external_id: External id of the generator to delete.
            space: The space where all the generator are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete generator by id:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> client.generator.delete("my_generator")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.generator.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> Generator | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> GeneratorList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Generator | GeneratorList | None:
        """Retrieve one or more generators by id(s).

        Args:
            external_id: External id or list of external ids of the generators.
            space: The space where all the generators are located.

        Returns:
            The requested generators.

        Examples:

            Retrieve generator by id:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> generator = client.generator.retrieve("my_generator")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.turbine_curves_edge,
                    "turbine_curves",
                    dm.DirectRelationReference("power-ops-types", "isSubAssetOf"),
                    "outwards",
                    dm.ViewId("power-ops-assets", "TurbineEfficiencyCurve", "1"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: GeneratorTextFields | Sequence[GeneratorTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_p_min: float | None = None,
        max_p_min: float | None = None,
        min_penstock: int | None = None,
        max_penstock: int | None = None,
        min_start_cost: float | None = None,
        max_start_cost: float | None = None,
        efficiency_curve: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> GeneratorList:
        """Search generators

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_p_min: The minimum value of the p min to filter on.
            max_p_min: The maximum value of the p min to filter on.
            min_penstock: The minimum value of the penstock to filter on.
            max_penstock: The maximum value of the penstock to filter on.
            min_start_cost: The minimum value of the start cost to filter on.
            max_start_cost: The maximum value of the start cost to filter on.
            efficiency_curve: The efficiency curve to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of generators to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results generators matching the query.

        Examples:

           Search for 'my_generator' in all text properties:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> generators = client.generator.search('my_generator')

        """
        filter_ = _create_generator_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_p_min,
            max_p_min,
            min_penstock,
            max_penstock,
            min_start_cost,
            max_start_cost,
            efficiency_curve,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _GENERATOR_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: GeneratorFields | Sequence[GeneratorFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: GeneratorTextFields | Sequence[GeneratorTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_p_min: float | None = None,
        max_p_min: float | None = None,
        min_penstock: int | None = None,
        max_penstock: int | None = None,
        min_start_cost: float | None = None,
        max_start_cost: float | None = None,
        efficiency_curve: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]: ...

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: GeneratorFields | Sequence[GeneratorFields] | None = None,
        group_by: GeneratorFields | Sequence[GeneratorFields] = None,
        query: str | None = None,
        search_properties: GeneratorTextFields | Sequence[GeneratorTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_p_min: float | None = None,
        max_p_min: float | None = None,
        min_penstock: int | None = None,
        max_penstock: int | None = None,
        min_start_cost: float | None = None,
        max_start_cost: float | None = None,
        efficiency_curve: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList: ...

    def aggregate(
        self,
        aggregate: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: GeneratorFields | Sequence[GeneratorFields] | None = None,
        group_by: GeneratorFields | Sequence[GeneratorFields] | None = None,
        query: str | None = None,
        search_property: GeneratorTextFields | Sequence[GeneratorTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_p_min: float | None = None,
        max_p_min: float | None = None,
        min_penstock: int | None = None,
        max_penstock: int | None = None,
        min_start_cost: float | None = None,
        max_start_cost: float | None = None,
        efficiency_curve: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across generators

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_p_min: The minimum value of the p min to filter on.
            max_p_min: The maximum value of the p min to filter on.
            min_penstock: The minimum value of the penstock to filter on.
            max_penstock: The maximum value of the penstock to filter on.
            min_start_cost: The minimum value of the start cost to filter on.
            max_start_cost: The maximum value of the start cost to filter on.
            efficiency_curve: The efficiency curve to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of generators to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count generators in space `my_space`:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> result = client.generator.aggregate("count", space="my_space")

        """

        filter_ = _create_generator_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_p_min,
            max_p_min,
            min_penstock,
            max_penstock,
            min_start_cost,
            max_start_cost,
            efficiency_curve,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _GENERATOR_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: GeneratorFields,
        interval: float,
        query: str | None = None,
        search_property: GeneratorTextFields | Sequence[GeneratorTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_p_min: float | None = None,
        max_p_min: float | None = None,
        min_penstock: int | None = None,
        max_penstock: int | None = None,
        min_start_cost: float | None = None,
        max_start_cost: float | None = None,
        efficiency_curve: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for generators

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_p_min: The minimum value of the p min to filter on.
            max_p_min: The maximum value of the p min to filter on.
            min_penstock: The minimum value of the penstock to filter on.
            max_penstock: The maximum value of the penstock to filter on.
            min_start_cost: The minimum value of the start cost to filter on.
            max_start_cost: The maximum value of the start cost to filter on.
            efficiency_curve: The efficiency curve to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of generators to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_generator_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_p_min,
            max_p_min,
            min_penstock,
            max_penstock,
            min_start_cost,
            max_start_cost,
            efficiency_curve,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _GENERATOR_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_p_min: float | None = None,
        max_p_min: float | None = None,
        min_penstock: int | None = None,
        max_penstock: int | None = None,
        min_start_cost: float | None = None,
        max_start_cost: float | None = None,
        efficiency_curve: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> GeneratorList:
        """List/filter generators

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_p_min: The minimum value of the p min to filter on.
            max_p_min: The maximum value of the p min to filter on.
            min_penstock: The minimum value of the penstock to filter on.
            max_penstock: The maximum value of the penstock to filter on.
            min_start_cost: The minimum value of the start cost to filter on.
            max_start_cost: The maximum value of the start cost to filter on.
            efficiency_curve: The efficiency curve to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of generators to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `turbine_curves` external ids for the generators. Defaults to True.

        Returns:
            List of requested generators

        Examples:

            List generators and limit to 5:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> generators = client.generator.list(limit=5)

        """
        filter_ = _create_generator_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_p_min,
            max_p_min,
            min_penstock,
            max_penstock,
            min_start_cost,
            max_start_cost,
            efficiency_curve,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            retrieve_edges=retrieve_edges,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.turbine_curves_edge,
                    "turbine_curves",
                    dm.DirectRelationReference("power-ops-types", "isSubAssetOf"),
                    "outwards",
                    dm.ViewId("power-ops-assets", "TurbineEfficiencyCurve", "1"),
                ),
            ],
        )
