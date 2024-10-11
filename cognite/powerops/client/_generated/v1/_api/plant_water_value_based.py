from __future__ import annotations

from collections.abc import Sequence
from typing import overload, Literal
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList, InstanceSort

from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    PlantWaterValueBased,
    PlantWaterValueBasedWrite,
    PlantWaterValueBasedFields,
    PlantWaterValueBasedList,
    PlantWaterValueBasedWriteList,
    PlantWaterValueBasedTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._plant_water_value_based import (
    _PLANTWATERVALUEBASED_PROPERTIES_BY_FIELD,
    _create_plant_water_value_based_filter,
)
from ._core import DEFAULT_LIMIT_READ, DEFAULT_QUERY_LIMIT, Aggregations, NodeAPI, SequenceNotStr, QueryStep, QueryBuilder
from .plant_water_value_based_generators import PlantWaterValueBasedGeneratorsAPI
from .plant_water_value_based_production_max_time_series import PlantWaterValueBasedProductionMaxTimeSeriesAPI
from .plant_water_value_based_production_min_time_series import PlantWaterValueBasedProductionMinTimeSeriesAPI
from .plant_water_value_based_water_value_time_series import PlantWaterValueBasedWaterValueTimeSeriesAPI
from .plant_water_value_based_feeding_fee_time_series import PlantWaterValueBasedFeedingFeeTimeSeriesAPI
from .plant_water_value_based_outlet_level_time_series import PlantWaterValueBasedOutletLevelTimeSeriesAPI
from .plant_water_value_based_inlet_level_time_series import PlantWaterValueBasedInletLevelTimeSeriesAPI
from .plant_water_value_based_head_direct_time_series import PlantWaterValueBasedHeadDirectTimeSeriesAPI
from .plant_water_value_based_query import PlantWaterValueBasedQueryAPI


class PlantWaterValueBasedAPI(NodeAPI[PlantWaterValueBased, PlantWaterValueBasedWrite, PlantWaterValueBasedList, PlantWaterValueBasedWriteList]):
    _view_id = dm.ViewId("power_ops_core", "PlantWaterValueBased", "1")
    _properties_by_field = _PLANTWATERVALUEBASED_PROPERTIES_BY_FIELD
    _class_type = PlantWaterValueBased
    _class_list = PlantWaterValueBasedList
    _class_write_list = PlantWaterValueBasedWriteList

    def __init__(self, client: CogniteClient):
        super().__init__(client=client)

        self.generators_edge = PlantWaterValueBasedGeneratorsAPI(client)
        self.production_max_time_series = PlantWaterValueBasedProductionMaxTimeSeriesAPI(client, self._view_id)
        self.production_min_time_series = PlantWaterValueBasedProductionMinTimeSeriesAPI(client, self._view_id)
        self.water_value_time_series = PlantWaterValueBasedWaterValueTimeSeriesAPI(client, self._view_id)
        self.feeding_fee_time_series = PlantWaterValueBasedFeedingFeeTimeSeriesAPI(client, self._view_id)
        self.outlet_level_time_series = PlantWaterValueBasedOutletLevelTimeSeriesAPI(client, self._view_id)
        self.inlet_level_time_series = PlantWaterValueBasedInletLevelTimeSeriesAPI(client, self._view_id)
        self.head_direct_time_series = PlantWaterValueBasedHeadDirectTimeSeriesAPI(client, self._view_id)

    def __call__(
            self,
            name: str | list[str] | None = None,
            name_prefix: str | None = None,
            display_name: str | list[str] | None = None,
            display_name_prefix: str | None = None,
            min_ordering: int | None = None,
            max_ordering: int | None = None,
            asset_type: str | list[str] | None = None,
            asset_type_prefix: str | None = None,
            min_head_loss_factor: float | None = None,
            max_head_loss_factor: float | None = None,
            min_outlet_level: float | None = None,
            max_outlet_level: float | None = None,
            min_production_max: float | None = None,
            max_production_max: float | None = None,
            min_production_min: float | None = None,
            max_production_min: float | None = None,
            min_connection_losses: float | None = None,
            max_connection_losses: float | None = None,
            external_id_prefix: str | None = None,
            space: str | list[str] | None = None,
            limit: int = DEFAULT_QUERY_LIMIT,
            filter: dm.Filter | None = None,
    ) -> PlantWaterValueBasedQueryAPI[PlantWaterValueBasedList]:
        """Query starting at plant water value baseds.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_ordering: The minimum value of the ordering to filter on.
            max_ordering: The maximum value of the ordering to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            min_head_loss_factor: The minimum value of the head loss factor to filter on.
            max_head_loss_factor: The maximum value of the head loss factor to filter on.
            min_outlet_level: The minimum value of the outlet level to filter on.
            max_outlet_level: The maximum value of the outlet level to filter on.
            min_production_max: The minimum value of the production max to filter on.
            max_production_max: The maximum value of the production max to filter on.
            min_production_min: The minimum value of the production min to filter on.
            max_production_min: The maximum value of the production min to filter on.
            min_connection_losses: The minimum value of the connection loss to filter on.
            max_connection_losses: The maximum value of the connection loss to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of plant water value baseds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for plant water value baseds.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_plant_water_value_based_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            asset_type,
            asset_type_prefix,
            min_head_loss_factor,
            max_head_loss_factor,
            min_outlet_level,
            max_outlet_level,
            min_production_max,
            max_production_max,
            min_production_min,
            max_production_min,
            min_connection_losses,
            max_connection_losses,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(PlantWaterValueBasedList)
        return PlantWaterValueBasedQueryAPI(self._client, builder, filter_, limit)


    def apply(
        self,
        plant_water_value_based: PlantWaterValueBasedWrite | Sequence[PlantWaterValueBasedWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) plant water value baseds.

        Note: This method iterates through all nodes and timeseries linked to plant_water_value_based and creates them including the edges
        between the nodes. For example, if any of `generators` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            plant_water_value_based: Plant water value based or sequence of plant water value baseds to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new plant_water_value_based:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import PlantWaterValueBasedWrite
                >>> client = PowerOpsModelsV1Client()
                >>> plant_water_value_based = PlantWaterValueBasedWrite(external_id="my_plant_water_value_based", ...)
                >>> result = client.plant_water_value_based.apply(plant_water_value_based)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.plant_water_value_based.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(plant_water_value_based, replace, write_none)

    def delete(self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> dm.InstancesDeleteResult:
        """Delete one or more plant water value based.

        Args:
            external_id: External id of the plant water value based to delete.
            space: The space where all the plant water value based are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete plant_water_value_based by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.plant_water_value_based.delete("my_plant_water_value_based")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.plant_water_value_based.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> PlantWaterValueBased | None:
        ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> PlantWaterValueBasedList:
        ...

    def retrieve(self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> PlantWaterValueBased | PlantWaterValueBasedList | None:
        """Retrieve one or more plant water value baseds by id(s).

        Args:
            external_id: External id or list of external ids of the plant water value baseds.
            space: The space where all the plant water value baseds are located.

        Returns:
            The requested plant water value baseds.

        Examples:

            Retrieve plant_water_value_based by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> plant_water_value_based = client.plant_water_value_based.retrieve("my_plant_water_value_based")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.generators_edge,
                    "generators",
                    dm.DirectRelationReference("power_ops_types", "isSubAssetOf"),
                    "outwards",
                    dm.ViewId("power_ops_core", "Generator", "1"),
                ),
                                               ]
        )


    def search(
        self,
        query: str,
        properties: PlantWaterValueBasedTextFields | SequenceNotStr[PlantWaterValueBasedTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        min_head_loss_factor: float | None = None,
        max_head_loss_factor: float | None = None,
        min_outlet_level: float | None = None,
        max_outlet_level: float | None = None,
        min_production_max: float | None = None,
        max_production_max: float | None = None,
        min_production_min: float | None = None,
        max_production_min: float | None = None,
        min_connection_losses: float | None = None,
        max_connection_losses: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: PlantWaterValueBasedFields | SequenceNotStr[PlantWaterValueBasedFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
    ) -> PlantWaterValueBasedList:
        """Search plant water value baseds

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_ordering: The minimum value of the ordering to filter on.
            max_ordering: The maximum value of the ordering to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            min_head_loss_factor: The minimum value of the head loss factor to filter on.
            max_head_loss_factor: The maximum value of the head loss factor to filter on.
            min_outlet_level: The minimum value of the outlet level to filter on.
            max_outlet_level: The maximum value of the outlet level to filter on.
            min_production_max: The minimum value of the production max to filter on.
            max_production_max: The maximum value of the production max to filter on.
            min_production_min: The minimum value of the production min to filter on.
            max_production_min: The maximum value of the production min to filter on.
            min_connection_losses: The minimum value of the connection loss to filter on.
            max_connection_losses: The maximum value of the connection loss to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of plant water value baseds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.

        Returns:
            Search results plant water value baseds matching the query.

        Examples:

           Search for 'my_plant_water_value_based' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> plant_water_value_baseds = client.plant_water_value_based.search('my_plant_water_value_based')

        """
        filter_ = _create_plant_water_value_based_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            asset_type,
            asset_type_prefix,
            min_head_loss_factor,
            max_head_loss_factor,
            min_outlet_level,
            max_outlet_level,
            min_production_max,
            max_production_max,
            min_production_min,
            max_production_min,
            min_connection_losses,
            max_connection_losses,
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
        property: PlantWaterValueBasedFields | SequenceNotStr[PlantWaterValueBasedFields] | None = None,
        query: str | None = None,
        search_property: PlantWaterValueBasedTextFields | SequenceNotStr[PlantWaterValueBasedTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        min_head_loss_factor: float | None = None,
        max_head_loss_factor: float | None = None,
        min_outlet_level: float | None = None,
        max_outlet_level: float | None = None,
        min_production_max: float | None = None,
        max_production_max: float | None = None,
        min_production_min: float | None = None,
        max_production_min: float | None = None,
        min_connection_losses: float | None = None,
        max_connection_losses: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.AggregatedNumberedValue:
        ...

    @overload
    def aggregate(
        self,
        aggregate: SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: None = None,
        property: PlantWaterValueBasedFields | SequenceNotStr[PlantWaterValueBasedFields] | None = None,
        query: str | None = None,
        search_property: PlantWaterValueBasedTextFields | SequenceNotStr[PlantWaterValueBasedTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        min_head_loss_factor: float | None = None,
        max_head_loss_factor: float | None = None,
        min_outlet_level: float | None = None,
        max_outlet_level: float | None = None,
        min_production_max: float | None = None,
        max_production_max: float | None = None,
        min_production_min: float | None = None,
        max_production_min: float | None = None,
        min_connection_losses: float | None = None,
        max_connection_losses: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue]:
        ...

    @overload
    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: PlantWaterValueBasedFields | SequenceNotStr[PlantWaterValueBasedFields],
        property: PlantWaterValueBasedFields | SequenceNotStr[PlantWaterValueBasedFields] | None = None,
        query: str | None = None,
        search_property: PlantWaterValueBasedTextFields | SequenceNotStr[PlantWaterValueBasedTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        min_head_loss_factor: float | None = None,
        max_head_loss_factor: float | None = None,
        min_outlet_level: float | None = None,
        max_outlet_level: float | None = None,
        min_production_max: float | None = None,
        max_production_max: float | None = None,
        min_production_min: float | None = None,
        max_production_min: float | None = None,
        min_connection_losses: float | None = None,
        max_connection_losses: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> InstanceAggregationResultList:
        ...

    def aggregate(
        self,
        aggregate: Aggregations
        | dm.aggregations.MetricAggregation
        | SequenceNotStr[Aggregations | dm.aggregations.MetricAggregation],
        group_by: PlantWaterValueBasedFields | SequenceNotStr[PlantWaterValueBasedFields] | None = None,
        property: PlantWaterValueBasedFields | SequenceNotStr[PlantWaterValueBasedFields] | None = None,
        query: str | None = None,
        search_property: PlantWaterValueBasedTextFields | SequenceNotStr[PlantWaterValueBasedTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        min_head_loss_factor: float | None = None,
        max_head_loss_factor: float | None = None,
        min_outlet_level: float | None = None,
        max_outlet_level: float | None = None,
        min_production_max: float | None = None,
        max_production_max: float | None = None,
        min_production_min: float | None = None,
        max_production_min: float | None = None,
        min_connection_losses: float | None = None,
        max_connection_losses: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> (
        dm.aggregations.AggregatedNumberedValue
        | list[dm.aggregations.AggregatedNumberedValue]
        | InstanceAggregationResultList
    ):
        """Aggregate data across plant water value baseds

        Args:
            aggregate: The aggregation to perform.
            group_by: The property to group by when doing the aggregation.
            property: The property to perform aggregation on.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_ordering: The minimum value of the ordering to filter on.
            max_ordering: The maximum value of the ordering to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            min_head_loss_factor: The minimum value of the head loss factor to filter on.
            max_head_loss_factor: The maximum value of the head loss factor to filter on.
            min_outlet_level: The minimum value of the outlet level to filter on.
            max_outlet_level: The maximum value of the outlet level to filter on.
            min_production_max: The minimum value of the production max to filter on.
            max_production_max: The maximum value of the production max to filter on.
            min_production_min: The minimum value of the production min to filter on.
            max_production_min: The maximum value of the production min to filter on.
            min_connection_losses: The minimum value of the connection loss to filter on.
            max_connection_losses: The maximum value of the connection loss to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of plant water value baseds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count plant water value baseds in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.plant_water_value_based.aggregate("count", space="my_space")

        """

        filter_ = _create_plant_water_value_based_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            asset_type,
            asset_type_prefix,
            min_head_loss_factor,
            max_head_loss_factor,
            min_outlet_level,
            max_outlet_level,
            min_production_max,
            max_production_max,
            min_production_min,
            max_production_min,
            min_connection_losses,
            max_connection_losses,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            aggregate=aggregate,
            group_by=group_by,  # type: ignore[arg-type]
            properties=property,  # type: ignore[arg-type]
            query=query,
            search_properties=search_property,  # type: ignore[arg-type]
            limit=limit,
            filter=filter_,
        )

    def histogram(
        self,
        property: PlantWaterValueBasedFields,
        interval: float,
        query: str | None = None,
        search_property: PlantWaterValueBasedTextFields | SequenceNotStr[PlantWaterValueBasedTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        display_name: str | list[str] | None = None,
        display_name_prefix: str | None = None,
        min_ordering: int | None = None,
        max_ordering: int | None = None,
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        min_head_loss_factor: float | None = None,
        max_head_loss_factor: float | None = None,
        min_outlet_level: float | None = None,
        max_outlet_level: float | None = None,
        min_production_max: float | None = None,
        max_production_max: float | None = None,
        min_production_min: float | None = None,
        max_production_min: float | None = None,
        min_connection_losses: float | None = None,
        max_connection_losses: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for plant water value baseds

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
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            min_head_loss_factor: The minimum value of the head loss factor to filter on.
            max_head_loss_factor: The maximum value of the head loss factor to filter on.
            min_outlet_level: The minimum value of the outlet level to filter on.
            max_outlet_level: The maximum value of the outlet level to filter on.
            min_production_max: The minimum value of the production max to filter on.
            max_production_max: The maximum value of the production max to filter on.
            min_production_min: The minimum value of the production min to filter on.
            max_production_min: The maximum value of the production min to filter on.
            min_connection_losses: The minimum value of the connection loss to filter on.
            max_connection_losses: The maximum value of the connection loss to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of plant water value baseds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_plant_water_value_based_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            asset_type,
            asset_type_prefix,
            min_head_loss_factor,
            max_head_loss_factor,
            min_outlet_level,
            max_outlet_level,
            min_production_max,
            max_production_max,
            min_production_min,
            max_production_min,
            min_connection_losses,
            max_connection_losses,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            property,
            interval,
            query,
            search_property,  # type: ignore[arg-type]
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
        asset_type: str | list[str] | None = None,
        asset_type_prefix: str | None = None,
        min_head_loss_factor: float | None = None,
        max_head_loss_factor: float | None = None,
        min_outlet_level: float | None = None,
        max_outlet_level: float | None = None,
        min_production_max: float | None = None,
        max_production_max: float | None = None,
        min_production_min: float | None = None,
        max_production_min: float | None = None,
        min_connection_losses: float | None = None,
        max_connection_losses: float | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        sort_by: PlantWaterValueBasedFields | Sequence[PlantWaterValueBasedFields] | None = None,
        direction: Literal["ascending", "descending"] = "ascending",
        sort: InstanceSort | list[InstanceSort] | None = None,
        retrieve_edges: bool = True,
    ) -> PlantWaterValueBasedList:
        """List/filter plant water value baseds

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            display_name: The display name to filter on.
            display_name_prefix: The prefix of the display name to filter on.
            min_ordering: The minimum value of the ordering to filter on.
            max_ordering: The maximum value of the ordering to filter on.
            asset_type: The asset type to filter on.
            asset_type_prefix: The prefix of the asset type to filter on.
            min_head_loss_factor: The minimum value of the head loss factor to filter on.
            max_head_loss_factor: The maximum value of the head loss factor to filter on.
            min_outlet_level: The minimum value of the outlet level to filter on.
            max_outlet_level: The maximum value of the outlet level to filter on.
            min_production_max: The minimum value of the production max to filter on.
            max_production_max: The maximum value of the production max to filter on.
            min_production_min: The minimum value of the production min to filter on.
            max_production_min: The maximum value of the production min to filter on.
            min_connection_losses: The minimum value of the connection loss to filter on.
            max_connection_losses: The maximum value of the connection loss to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of plant water value baseds to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            sort_by: The property to sort by.
            direction: The direction to sort by, either 'ascending' or 'descending'.
            sort: (Advanced) If sort_by and direction are not sufficient, you can write your own sorting.
                This will override the sort_by and direction. This allowos you to sort by multiple fields and
                specify the direction for each field as well as how to handle null values.
            retrieve_edges: Whether to retrieve `generators` external ids for the plant water value baseds. Defaults to True.

        Returns:
            List of requested plant water value baseds

        Examples:

            List plant water value baseds and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> plant_water_value_baseds = client.plant_water_value_based.list(limit=5)

        """
        filter_ = _create_plant_water_value_based_filter(
            self._view_id,
            name,
            name_prefix,
            display_name,
            display_name_prefix,
            min_ordering,
            max_ordering,
            asset_type,
            asset_type_prefix,
            min_head_loss_factor,
            max_head_loss_factor,
            min_outlet_level,
            max_outlet_level,
            min_production_max,
            max_production_max,
            min_production_min,
            max_production_min,
            min_connection_losses,
            max_connection_losses,
            external_id_prefix,
            space,
            filter,
        )

        return self._list(
            limit=limit,
            filter=filter_,
            sort_by=sort_by,  # type: ignore[arg-type]
            direction=direction,
            sort=sort,
            retrieve_edges=retrieve_edges,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.generators_edge,
                    "generators",
                    dm.DirectRelationReference("power_ops_types", "isSubAssetOf"),
                    "outwards",
                    dm.ViewId("power_ops_core", "Generator", "1"),
                ),
                                               ]
        )
