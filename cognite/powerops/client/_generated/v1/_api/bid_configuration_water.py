from __future__ import annotations

from collections.abc import Sequence
from typing import overload
import warnings

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm

from cognite.powerops.client._generated.v1.data_classes._core import DEFAULT_INSTANCE_SPACE
from cognite.powerops.client._generated.v1.data_classes import (
    DomainModelCore,
    DomainModelWrite,
    ResourcesWriteResult,
    BidConfigurationWater,
    BidConfigurationWaterWrite,
    BidConfigurationWaterList,
    BidConfigurationWaterWriteList,
)
from cognite.powerops.client._generated.v1.data_classes._bid_configuration_water import (
    _create_bid_configuration_water_filter,
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
from .bid_configuration_water_plants import BidConfigurationWaterPlantsAPI
from .bid_configuration_water_watercourses import BidConfigurationWaterWatercoursesAPI
from .bid_configuration_water_query import BidConfigurationWaterQueryAPI


class BidConfigurationWaterAPI(NodeAPI[BidConfigurationWater, BidConfigurationWaterWrite, BidConfigurationWaterList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[BidConfigurationWater]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=BidConfigurationWater,
            class_list=BidConfigurationWaterList,
            class_write_list=BidConfigurationWaterWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id
        self.plants_edge = BidConfigurationWaterPlantsAPI(client)
        self.watercourses_edge = BidConfigurationWaterWatercoursesAPI(client)

    def __call__(
        self,
        market_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> BidConfigurationWaterQueryAPI[BidConfigurationWaterList]:
        """Query starting at bid configuration waters.

        Args:
            market_configuration: The market configuration to filter on.
            method: The method to filter on.
            price_area: The price area to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid configuration waters to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for bid configuration waters.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_bid_configuration_water_filter(
            self._view_id,
            market_configuration,
            method,
            price_area,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(BidConfigurationWaterList)
        return BidConfigurationWaterQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        bid_configuration_water: BidConfigurationWaterWrite | Sequence[BidConfigurationWaterWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) bid configuration waters.

        Note: This method iterates through all nodes and timeseries linked to bid_configuration_water and creates them including the edges
        between the nodes. For example, if any of `plants` or `watercourses` are set, then these
        nodes as well as any nodes linked to them, and all the edges linking these nodes will be created.

        Args:
            bid_configuration_water: Bid configuration water or sequence of bid configuration waters to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new bid_configuration_water:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import BidConfigurationWaterWrite
                >>> client = PowerOpsModelsV1Client()
                >>> bid_configuration_water = BidConfigurationWaterWrite(external_id="my_bid_configuration_water", ...)
                >>> result = client.bid_configuration_water.apply(bid_configuration_water)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.bid_configuration_water.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(bid_configuration_water, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more bid configuration water.

        Args:
            external_id: External id of the bid configuration water to delete.
            space: The space where all the bid configuration water are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete bid_configuration_water by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.bid_configuration_water.delete("my_bid_configuration_water")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.bid_configuration_water.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> BidConfigurationWater | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> BidConfigurationWaterList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> BidConfigurationWater | BidConfigurationWaterList | None:
        """Retrieve one or more bid configuration waters by id(s).

        Args:
            external_id: External id or list of external ids of the bid configuration waters.
            space: The space where all the bid configuration waters are located.

        Returns:
            The requested bid configuration waters.

        Examples:

            Retrieve bid_configuration_water by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_configuration_water = client.bid_configuration_water.retrieve("my_bid_configuration_water")

        """
        return self._retrieve(
            external_id,
            space,
            retrieve_edges=True,
            edge_api_name_type_direction_view_id_penta=[
                (
                    self.plants_edge,
                    "plants",
                    dm.DirectRelationReference("sp_powerops_types", "BidConfiguration.plants"),
                    "outwards",
                    dm.ViewId("sp_powerops_models", "Plant", "1"),
                ),
                (
                    self.watercourses_edge,
                    "watercourses",
                    dm.DirectRelationReference("sp_powerops_types", "BidConfiguration.watercourses"),
                    "outwards",
                    dm.ViewId("sp_powerops_models", "Watercourse", "1"),
                ),
            ],
        )

    def list(
        self,
        market_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        method: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        price_area: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
        retrieve_edges: bool = True,
    ) -> BidConfigurationWaterList:
        """List/filter bid configuration waters

        Args:
            market_configuration: The market configuration to filter on.
            method: The method to filter on.
            price_area: The price area to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid configuration waters to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.
            retrieve_edges: Whether to retrieve `plants` or `watercourses` external ids for the bid configuration waters. Defaults to True.

        Returns:
            List of requested bid configuration waters

        Examples:

            List bid configuration waters and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_configuration_waters = client.bid_configuration_water.list(limit=5)

        """
        filter_ = _create_bid_configuration_water_filter(
            self._view_id,
            market_configuration,
            method,
            price_area,
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
                    dm.DirectRelationReference("sp_powerops_types", "BidConfiguration.plants"),
                    "outwards",
                    dm.ViewId("sp_powerops_models", "Plant", "1"),
                ),
                (
                    self.watercourses_edge,
                    "watercourses",
                    dm.DirectRelationReference("sp_powerops_types", "BidConfiguration.watercourses"),
                    "outwards",
                    dm.ViewId("sp_powerops_models", "Watercourse", "1"),
                ),
            ],
        )
