from __future__ import annotations

from collections.abc import Sequence
from typing import overload
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.instances import InstanceAggregationResultList

from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    PriceAreaAsset,
    PriceAreaAssetWrite,
    PriceAreaAssetFields,
    PriceAreaAssetList,
    PriceAreaAssetWriteList,
    PriceAreaAssetTextFields,
)
from cognite.powerops.client._generated.v1.data_classes._price_area_asset import (
    _PRICEAREAASSET_PROPERTIES_BY_FIELD,
    _create_price_area_asset_filter,
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
from .price_area_asset_plants import PriceAreaAssetPlantsAPI
from .price_area_asset_watercourses import PriceAreaAssetWatercoursesAPI
from .price_area_asset_query import PriceAreaAssetQueryAPI


class PriceAreaAssetAPI(NodeAPI[PriceAreaAsset, PriceAreaAssetWrite, PriceAreaAssetList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[PriceAreaAsset]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=PriceAreaAsset,
            class_list=PriceAreaAssetList,
            class_write_list=PriceAreaAssetWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.plants_edge = PriceAreaAssetPlantsAPI(client)
        self.watercourses_edge = PriceAreaAssetWatercoursesAPI(client)

    def __call__(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> PriceAreaAssetQueryAPI[PriceAreaAssetList]:
        """Query starting at price area assets.

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of price area assets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for price area assets.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_price_area_asset_filter(
            self._view_id,
            name,
            name_prefix,
            timezone,
            timezone_prefix,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(PriceAreaAssetList)
        return PriceAreaAssetQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        price_area_asset: PriceAreaAssetWrite | Sequence[PriceAreaAssetWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) price area assets.

        Note: This method iterates through all nodes and timeseries linked to price_area_asset and creates them including the edges
        between the nodes. For example, if any of `plants` or `watercourses` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            price_area_asset: Price area asset or sequence of price area assets to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new price_area_asset:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import PriceAreaAssetWrite
                >>> client = PowerOpsModelsV1Client()
                >>> price_area_asset = PriceAreaAssetWrite(external_id="my_price_area_asset", ...)
                >>> result = client.price_area_asset.apply(price_area_asset)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.price_area_asset.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(price_area_asset, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more price area asset.

        Args:
            external_id: External id of the price area asset to delete.
            space: The space where all the price area asset are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete price_area_asset by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.price_area_asset.delete("my_price_area_asset")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.price_area_asset.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> PriceAreaAsset | None: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE) -> PriceAreaAssetList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> PriceAreaAsset | PriceAreaAssetList | None:
        """Retrieve one or more price area assets by id(s).

        Args:
            external_id: External id or list of external ids of the price area assets.
            space: The space where all the price area assets are located.

        Returns:
            The requested price area assets.

        Examples:

            Retrieve price_area_asset by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> price_area_asset = client.price_area_asset.retrieve("my_price_area_asset")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.plants_edge,
                    "plants",
                    dm.DirectRelationReference("sp_powerops_types", "isPlantOf"),
                    "outwards",
                    dm.ViewId("sp_powerops_models", "Plant", "1"),
                ),
                (
                    self.watercourses_edge,
                    "watercourses",
                    dm.DirectRelationReference("sp_powerops_types", "isWatercourseOf"),
                    "outwards",
                    dm.ViewId("sp_powerops_models", "Watercourse", "1"),
                ),
            ],
        )

    def search(
        self,
        query: str,
        properties: PriceAreaAssetTextFields | Sequence[PriceAreaAssetTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> PriceAreaAssetList:
        """Search price area assets

        Args:
            query: The search query,
            properties: The property to search, if nothing is passed all text fields will be searched.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of price area assets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Search results price area assets matching the query.

        Examples:

           Search for 'my_price_area_asset' in all text properties:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> price_area_assets = client.price_area_asset.search('my_price_area_asset')

        """
        filter_ = _create_price_area_asset_filter(
            self._view_id,
            name,
            name_prefix,
            timezone,
            timezone_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._search(self._view_id, query, _PRICEAREAASSET_PROPERTIES_BY_FIELD, properties, filter_, limit)

    @overload
    def aggregate(
        self,
        aggregations: (
            Aggregations
            | dm.aggregations.MetricAggregation
            | Sequence[Aggregations]
            | Sequence[dm.aggregations.MetricAggregation]
        ),
        property: PriceAreaAssetFields | Sequence[PriceAreaAssetFields] | None = None,
        group_by: None = None,
        query: str | None = None,
        search_properties: PriceAreaAssetTextFields | Sequence[PriceAreaAssetTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
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
        property: PriceAreaAssetFields | Sequence[PriceAreaAssetFields] | None = None,
        group_by: PriceAreaAssetFields | Sequence[PriceAreaAssetFields] = None,
        query: str | None = None,
        search_properties: PriceAreaAssetTextFields | Sequence[PriceAreaAssetTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
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
        property: PriceAreaAssetFields | Sequence[PriceAreaAssetFields] | None = None,
        group_by: PriceAreaAssetFields | Sequence[PriceAreaAssetFields] | None = None,
        query: str | None = None,
        search_property: PriceAreaAssetTextFields | Sequence[PriceAreaAssetTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> list[dm.aggregations.AggregatedNumberedValue] | InstanceAggregationResultList:
        """Aggregate data across price area assets

        Args:
            aggregate: The aggregation to perform.
            property: The property to perform aggregation on.
            group_by: The property to group by when doing the aggregation.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of price area assets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Aggregation results.

        Examples:

            Count price area assets in space `my_space`:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> result = client.price_area_asset.aggregate("count", space="my_space")

        """

        filter_ = _create_price_area_asset_filter(
            self._view_id,
            name,
            name_prefix,
            timezone,
            timezone_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._aggregate(
            self._view_id,
            aggregate,
            _PRICEAREAASSET_PROPERTIES_BY_FIELD,
            property,
            group_by,
            query,
            search_property,
            limit,
            filter_,
        )

    def histogram(
        self,
        property: PriceAreaAssetFields,
        interval: float,
        query: str | None = None,
        search_property: PriceAreaAssetTextFields | Sequence[PriceAreaAssetTextFields] | None = None,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> dm.aggregations.HistogramValue:
        """Produces histograms for price area assets

        Args:
            property: The property to use as the value in the histogram.
            interval: The interval to use for the histogram bins.
            query: The query to search for in the text field.
            search_property: The text field to search in.
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of price area assets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            Bucketed histogram results.

        """
        filter_ = _create_price_area_asset_filter(
            self._view_id,
            name,
            name_prefix,
            timezone,
            timezone_prefix,
            external_id_prefix,
            space,
            filter,
        )
        return self._histogram(
            self._view_id,
            property,
            interval,
            _PRICEAREAASSET_PROPERTIES_BY_FIELD,
            query,
            search_property,
            limit,
            filter_,
        )

    def list(
        self,
        name: str | list[str] | None = None,
        name_prefix: str | None = None,
        timezone: str | list[str] | None = None,
        timezone_prefix: str | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> PriceAreaAssetList:
        """List/filter price area assets

        Args:
            name: The name to filter on.
            name_prefix: The prefix of the name to filter on.
            timezone: The timezone to filter on.
            timezone_prefix: The prefix of the timezone to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of price area assets to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `plants` or `watercourses` external ids for the price area assets. Defaults to True.

        Returns:
            List of requested price area assets

        Examples:

            List price area assets and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> price_area_assets = client.price_area_asset.list(limit=5)

        """
        filter_ = _create_price_area_asset_filter(
            self._view_id,
            name,
            name_prefix,
            timezone,
            timezone_prefix,
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
                    self.plants_edge,
                    "plants",
                    dm.DirectRelationReference("sp_powerops_types", "isPlantOf"),
                    "outwards",
                    dm.ViewId("sp_powerops_models", "Plant", "1"),
                ),
                (
                    self.watercourses_edge,
                    "watercourses",
                    dm.DirectRelationReference("sp_powerops_types", "isWatercourseOf"),
                    "outwards",
                    dm.ViewId("sp_powerops_models", "Watercourse", "1"),
                ),
            ],
        )
