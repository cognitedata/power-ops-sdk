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
    BidConfiguration,
    BidConfigurationWrite,
    BidConfigurationList,
    BidConfigurationWriteList,
)
from cognite.powerops.client._generated.v1.data_classes._bid_configuration import (
    _create_bid_configuration_filter,
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
from .bid_configuration_query import BidConfigurationQueryAPI


class BidConfigurationAPI(NodeAPI[BidConfiguration, BidConfigurationWrite, BidConfigurationList]):
    def __init__(self, client: CogniteClient, view_by_read_class: dict[type[DomainModelCore], dm.ViewId]):
        view_id = view_by_read_class[BidConfiguration]
        super().__init__(
            client=client,
            sources=view_id,
            class_type=BidConfiguration,
            class_list=BidConfigurationList,
            class_write_list=BidConfigurationWriteList,
            view_by_read_class=view_by_read_class,
        )
        self._view_id = view_id

    def __call__(
        self,
        market_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_QUERY_LIMIT,
        filter: dm.Filter | None = None,
    ) -> BidConfigurationQueryAPI[BidConfigurationList]:
        """Query starting at bid configurations.

        Args:
            market_configuration: The market configuration to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid configurations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            A query API for bid configurations.

        """
        has_data = dm.filters.HasData(views=[self._view_id])
        filter_ = _create_bid_configuration_filter(
            self._view_id,
            market_configuration,
            external_id_prefix,
            space,
            (filter and dm.filters.And(filter, has_data)) or has_data,
        )
        builder = QueryBuilder(BidConfigurationList)
        return BidConfigurationQueryAPI(self._client, builder, self._view_by_read_class, filter_, limit)

    def apply(
        self,
        bid_configuration: BidConfigurationWrite | Sequence[BidConfigurationWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> ResourcesWriteResult:
        """Add or update (upsert) bid configurations.

        Args:
            bid_configuration: Bid configuration or sequence of bid configurations to upsert.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method, will by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        Examples:

            Create a new bid_configuration:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> from cognite.powerops.client._generated.v1.data_classes import BidConfigurationWrite
                >>> client = PowerOpsModelsV1Client()
                >>> bid_configuration = BidConfigurationWrite(external_id="my_bid_configuration", ...)
                >>> result = client.bid_configuration.apply(bid_configuration)

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the client instead. This means instead of "
            "`my_client.bid_configuration.apply(my_items)` please use `my_client.upsert(my_items)`."
            "The motivation is that all apply methods are the same, and having one apply method per API "
            " class encourages users to create items in small batches, which is inefficient."
            "In addition, .upsert method is more descriptive of what the method does.",
            UserWarning,
            stacklevel=2,
        )
        return self._apply(bid_configuration, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more bid configuration.

        Args:
            external_id: External id of the bid configuration to delete.
            space: The space where all the bid configuration are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete bid_configuration by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> client.bid_configuration.delete("my_bid_configuration")
        """
        warnings.warn(
            "The .delete method is deprecated and will be removed in v1.0. "
            "Please use the .delete method on the client instead. This means instead of "
            "`my_client.bid_configuration.delete(my_ids)` please use `my_client.delete(my_ids)`."
            "The motivation is that all delete methods are the same, and having one delete method per API "
            " class encourages users to delete items in small batches, which is inefficient.",
            UserWarning,
            stacklevel=2,
        )
        return self._delete(external_id, space)

    @overload
    def retrieve(self, external_id: str, space: str = DEFAULT_INSTANCE_SPACE) -> BidConfiguration | None: ...

    @overload
    def retrieve(
        self, external_id: SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> BidConfigurationList: ...

    def retrieve(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> BidConfiguration | BidConfigurationList | None:
        """Retrieve one or more bid configurations by id(s).

        Args:
            external_id: External id or list of external ids of the bid configurations.
            space: The space where all the bid configurations are located.

        Returns:
            The requested bid configurations.

        Examples:

            Retrieve bid_configuration by id:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_configuration = client.bid_configuration.retrieve("my_bid_configuration")

        """
        return self._retrieve(external_id, space)

    def list(
        self,
        market_configuration: str | tuple[str, str] | list[str] | list[tuple[str, str]] | None = None,
        external_id_prefix: str | None = None,
        space: str | list[str] | None = None,
        limit: int | None = DEFAULT_LIMIT_READ,
        filter: dm.Filter | None = None,
    ) -> BidConfigurationList:
        """List/filter bid configurations

        Args:
            market_configuration: The market configuration to filter on.
            external_id_prefix: The prefix of the external ID to filter on.
            space: The space to filter on.
            limit: Maximum number of bid configurations to return. Defaults to 25. Set to -1, float("inf") or None to return all items.
            filter: (Advanced) If the filtering available in the above is not sufficient, you can write your own filtering which will be ANDed with the filter above.

        Returns:
            List of requested bid configurations

        Examples:

            List bid configurations and limit to 5:

                >>> from cognite.powerops.client._generated.v1 import PowerOpsModelsV1Client
                >>> client = PowerOpsModelsV1Client()
                >>> bid_configurations = client.bid_configuration.list(limit=5)

        """
        filter_ = _create_bid_configuration_filter(
            self._view_id,
            market_configuration,
            external_id_prefix,
            space,
            filter,
        )
        return self._list(limit=limit, filter=filter_)
