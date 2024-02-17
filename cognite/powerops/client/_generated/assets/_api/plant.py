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
    Plant,
    PlantWrite,
    PlantFields,
    PlantList,
    PlantWriteList,
    PlantTextFields,
)
from cognite.powerops.client._generated.assets.data_classes._plant import (
    _PLANT_PROPERTIES_BY_FIELD,
    _create_plant_filter,
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
from .plant_generators import PlantGeneratorsAPI
from .plant_p_max_time_series import PlantPMaxTimeSeriesAPI
from .plant_p_min_time_series import PlantPMinTimeSeriesAPI
from .plant_water_value_time_series import PlantWaterValueTimeSeriesAPI
from .plant_feeding_fee_time_series import PlantFeedingFeeTimeSeriesAPI
from .plant_outlet_level_time_series import PlantOutletLevelTimeSeriesAPI
from .plant_inlet_level_time_series import PlantInletLevelTimeSeriesAPI
from .plant_head_direct_time_series import PlantHeadDirectTimeSeriesAPI
from .plant_query import PlantQueryAPI


class PlantAPI(NodeAPI[Plant, PlantWrite, PlantList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[Plant]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=Plant,
            class_list=PlantList,
            class_write_list=PlantWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.generators_edge = PlantGeneratorsAPI(client)
        self.p_max_time_series = PlantPMaxTimeSeriesAPI(client, view_id)
        self.p_min_time_series = PlantPMinTimeSeriesAPI(client, view_id)
        self.water_value_time_series = PlantWaterValueTimeSeriesAPI(client, view_id)
        self.feeding_fee_time_series = PlantFeedingFeeTimeSeriesAPI(client, view_id)
        self.outlet_level_time_series = PlantOutletLevelTimeSeriesAPI(client, view_id)
        self.inlet_level_time_series = PlantInletLevelTimeSeriesAPI(client, view_id)
        self.head_direct_time_series = PlantHeadDirectTimeSeriesAPI(client, view_id)

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        min_head_loss_factor: float | None = None,
        max_head_loss_factor: float | None = None,
        min_outlet_level: float | None = None,
        max_outlet_level: float | None = None,
        min_p_max: float | None = None,
        max_p_max: float | None = None,
        min_p_min: float | None = None,
        max_p_min: float | None = None,
        watercourse: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_connection_losses: float | None = None,
        max_connection_losses: float | None = None,
        inlet_reservoir: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> PlantQueryAPI[PlantList]:
        """Query starting at plants.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_ordering: The minimum value of the ordering to filter on.
            max_ordering: The maximum value of the ordering to filter on.
            min_head_loss_factor: The minimum value of the head loss factor to filter on.
            max_head_loss_factor: The maximum value of the head loss factor to filter on.
            min_outlet_level: The minimum value of the outlet level to filter on.
            max_outlet_level: The maximum value of the outlet level to filter on.
            min_p_max: The minimum value of the p max to filter on.
            max_p_max: The maximum value of the p max to filter on.
            min_p_min: The minimum value of the p min to filter on.
            max_p_min: The maximum value of the p min to filter on.
            watercourse: The watercourse to filter on.
            min_connection_losses: The minimum value of the connection loss to filter on.
            max_connection_losses: The maximum value of the connection loss to filter on.
            inlet_reservoir: The inlet reservoir to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of plants to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for plants.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_plant_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            min_head_loss_factor,
            max_head_loss_factor,
            min_outlet_level,
            max_outlet_level,
            min_p_max,
            max_p_max,
            min_p_min,
            max_p_min,
            watercourse,
            min_connection_losses,
            max_connection_losses,
            inlet_reservoir,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(PlantList)
        return PlantQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        plant: PlantWrite | Sequence[PlantWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) plants.

        Note: This method iterates through all nodes and timeseries linked to plant and creates them including the edges
        between the nodes. For example, if any of `generators` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            plant: Plant or sequence of plants to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new plant:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> from cognite.powerops.client._generated.assets.data_classes import PlantWrite
                >>> client = PowerAssetAPI()
                >>> plant = PlantWrite(external_id="my_plant", ...)
                >>> result = client.plant.apply(plant)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.plant.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(plant, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more plant.

        Args:
            external_id: External id of the plant to delete.
            space: The space where all the plant are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete plant by id:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> client.plant.delete("my_plant")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.plant.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> Plant | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> PlantList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> Plant | PlantList | None:
        """Retrieve one or more plants by id(s).

        Args:
            external_id: External id or list of external ids of the plants.
            space: The space where all the plants are located.

        Returns:
            The requested plants.

        Examples:

            Retrieve plant by id:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> plant = client.plant.retrieve("my_plant")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.generators_edge,
                    "generators",
                    dm.DirectRelationReference("power-ops-types", "isSubAssetOf"),
                    "outwards",
                    dm.ViewId("power-ops-assets", "Generator", "1"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: PlantTextFields | Sequence[PlantTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        min_head_loss_factor: float | None = None,
        max_head_loss_factor: float | None = None,
        min_outlet_level: float | None = None,
        max_outlet_level: float | None = None,
        min_p_max: float | None = None,
        max_p_max: float | None = None,
        min_p_min: float | None = None,
        max_p_min: float | None = None,
        watercourse: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_connection_losses: float | None = None,
        max_connection_losses: float | None = None,
        inlet_reservoir: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PlantList:
        """Search plants

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_ordering: The minimum value of the ordering to filter on.
            max_ordering: The maximum value of the ordering to filter on.
            min_head_loss_factor: The minimum value of the head loss factor to filter on.
            max_head_loss_factor: The maximum value of the head loss factor to filter on.
            min_outlet_level: The minimum value of the outlet level to filter on.
            max_outlet_level: The maximum value of the outlet level to filter on.
            min_p_max: The minimum value of the p max to filter on.
            max_p_max: The maximum value of the p max to filter on.
            min_p_min: The minimum value of the p min to filter on.
            max_p_min: The maximum value of the p min to filter on.
            watercourse: The watercourse to filter on.
            min_connection_losses: The minimum value of the connection loss to filter on.
            max_connection_losses: The maximum value of the connection loss to filter on.
            inlet_reservoir: The inlet reservoir to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of plants to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results plants matching the query.

        Examples:

           Search for 'my_plant' in all text properties:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> plants = client.plant.search('my_plant')

        """
        filter_ = _create_plant_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            min_head_loss_factor,
            max_head_loss_factor,
            min_outlet_level,
            max_outlet_level,
            min_p_max,
            max_p_max,
            min_p_min,
            max_p_min,
            watercourse,
            min_connection_losses,
            max_connection_losses,
            inlet_reservoir,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _PLANT_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: PlantFields | Sequence[PlantFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: PlantTextFields | Sequence[PlantTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        min_head_loss_factor: float | None = None,
        max_head_loss_factor: float | None = None,
        min_outlet_level: float | None = None,
        max_outlet_level: float | None = None,
        min_p_max: float | None = None,
        max_p_max: float | None = None,
        min_p_min: float | None = None,
        max_p_min: float | None = None,
        watercourse: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_connection_losses: float | None = None,
        max_connection_losses: float | None = None,
        inlet_reservoir: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: PlantFields | Sequence[PlantFields] | None = None,
        group_by: PlantFields | Sequence[PlantFields] = None,
        query: str | None = None,
        search_properties: PlantTextFields | Sequence[PlantTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        min_head_loss_factor: float | None = None,
        max_head_loss_factor: float | None = None,
        min_outlet_level: float | None = None,
        max_outlet_level: float | None = None,
        min_p_max: float | None = None,
        max_p_max: float | None = None,
        min_p_min: float | None = None,
        max_p_min: float | None = None,
        watercourse: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_connection_losses: float | None = None,
        max_connection_losses: float | None = None,
        inlet_reservoir: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
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
        property: PlantFields | Sequence[PlantFields] | None = None,
        group_by: PlantFields | Sequence[PlantFields] | None = None,
        query: str | None = None,
        search_property: PlantTextFields | Sequence[PlantTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        min_head_loss_factor: float | None = None,
        max_head_loss_factor: float | None = None,
        min_outlet_level: float | None = None,
        max_outlet_level: float | None = None,
        min_p_max: float | None = None,
        max_p_max: float | None = None,
        min_p_min: float | None = None,
        max_p_min: float | None = None,
        watercourse: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_connection_losses: float | None = None,
        max_connection_losses: float | None = None,
        inlet_reservoir: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across plants

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
            min_ordering: The minimum value of the ordering to filter on.
            max_ordering: The maximum value of the ordering to filter on.
            min_head_loss_factor: The minimum value of the head loss factor to filter on.
            max_head_loss_factor: The maximum value of the head loss factor to filter on.
            min_outlet_level: The minimum value of the outlet level to filter on.
            max_outlet_level: The maximum value of the outlet level to filter on.
            min_p_max: The minimum value of the p max to filter on.
            max_p_max: The maximum value of the p max to filter on.
            min_p_min: The minimum value of the p min to filter on.
            max_p_min: The maximum value of the p min to filter on.
            watercourse: The watercourse to filter on.
            min_connection_losses: The minimum value of the connection loss to filter on.
            max_connection_losses: The maximum value of the connection loss to filter on.
            inlet_reservoir: The inlet reservoir to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of plants to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count plants in space `my_space`:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> result = client.plant.aggregate("count", space="my_space")

        """

        filter_ = _create_plant_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            min_head_loss_factor,
            max_head_loss_factor,
            min_outlet_level,
            max_outlet_level,
            min_p_max,
            max_p_max,
            min_p_min,
            max_p_min,
            watercourse,
            min_connection_losses,
            max_connection_losses,
            inlet_reservoir,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _PLANT_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: PlantFields,
        interval: float,
        query: str | None = None,
        search_property: PlantTextFields | Sequence[PlantTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        min_head_loss_factor: float | None = None,
        max_head_loss_factor: float | None = None,
        min_outlet_level: float | None = None,
        max_outlet_level: float | None = None,
        min_p_max: float | None = None,
        max_p_max: float | None = None,
        min_p_min: float | None = None,
        max_p_min: float | None = None,
        watercourse: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_connection_losses: float | None = None,
        max_connection_losses: float | None = None,
        inlet_reservoir: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for plants

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_ordering: The minimum value of the ordering to filter on.
            max_ordering: The maximum value of the ordering to filter on.
            min_head_loss_factor: The minimum value of the head loss factor to filter on.
            max_head_loss_factor: The maximum value of the head loss factor to filter on.
            min_outlet_level: The minimum value of the outlet level to filter on.
            max_outlet_level: The maximum value of the outlet level to filter on.
            min_p_max: The minimum value of the p max to filter on.
            max_p_max: The maximum value of the p max to filter on.
            min_p_min: The minimum value of the p min to filter on.
            max_p_min: The maximum value of the p min to filter on.
            watercourse: The watercourse to filter on.
            min_connection_losses: The minimum value of the connection loss to filter on.
            max_connection_losses: The maximum value of the connection loss to filter on.
            inlet_reservoir: The inlet reservoir to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of plants to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_plant_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            min_head_loss_factor,
            max_head_loss_factor,
            min_outlet_level,
            max_outlet_level,
            min_p_max,
            max_p_max,
            min_p_min,
            max_p_min,
            watercourse,
            min_connection_losses,
            max_connection_losses,
            inlet_reservoir,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _PLANT_PROPERTIES_BY_FIELD,
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
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        min_head_loss_factor: float | None = None,
        max_head_loss_factor: float | None = None,
        min_outlet_level: float | None = None,
        max_outlet_level: float | None = None,
        min_p_max: float | None = None,
        max_p_max: float | None = None,
        min_p_min: float | None = None,
        max_p_min: float | None = None,
        watercourse: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        min_connection_losses: float | None = None,
        max_connection_losses: float | None = None,
        inlet_reservoir: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> PlantList:
        """List/filter plants

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_ordering: The minimum value of the ordering to filter on.
            max_ordering: The maximum value of the ordering to filter on.
            min_head_loss_factor: The minimum value of the head loss factor to filter on.
            max_head_loss_factor: The maximum value of the head loss factor to filter on.
            min_outlet_level: The minimum value of the outlet level to filter on.
            max_outlet_level: The maximum value of the outlet level to filter on.
            min_p_max: The minimum value of the p max to filter on.
            max_p_max: The maximum value of the p max to filter on.
            min_p_min: The minimum value of the p min to filter on.
            max_p_min: The maximum value of the p min to filter on.
            watercourse: The watercourse to filter on.
            min_connection_losses: The minimum value of the connection loss to filter on.
            max_connection_losses: The maximum value of the connection loss to filter on.
            inlet_reservoir: The inlet reservoir to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of plants to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `generators` external ids for the plants. Defaults to True.

        Returns:
            List of requested plants

        Examples:

            List plants and limit to 5:

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> plants = client.plant.list(limit=5)

        """
        filter_ = _create_plant_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            min_head_loss_factor,
            max_head_loss_factor,
            min_outlet_level,
            max_outlet_level,
            min_p_max,
            max_p_max,
            min_p_min,
            max_p_min,
            watercourse,
            min_connection_losses,
            max_connection_losses,
            inlet_reservoir,
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
                    self.generators_edge,
                    "generators",
                    dm.DirectRelationReference("power-ops-types", "isSubAssetOf"),
                    "outwards",
                    dm.ViewId("power-ops-assets", "Generator", "1"),
                ),
            ],
        )
