from __future__ import annotations

import warnings
from pathlib import Path
from typing import Sequence

from cognite.client import ClientConfig, CogniteClient, data_modeling as dm
from cognite.client.data_classes import TimeSeriesList
from cognite.client.credentials import OAuthClientCredentials

from ._api.alert import AlertAPI
from ._api.bid_document import BidDocumentAPI
from ._api.bid_method import BidMethodAPI
from ._api.bid_row import BidRowAPI
from ._api.price_area import PriceAreaAPI
from ._api._core import SequenceNotStr
from .data_classes._core import DEFAULT_INSTANCE_SPACE
from . import data_classes


class AFRRBidAPI:
    """
    AFRRBidAPI

    Generated with:
        pygen = 0.99.10
        cognite-sdk = 7.20.1
        pydantic = 2.6.1

    Data Model:
        space: power-ops-afrr-bid
        externalId: AFRRBid
        version: 1
    """

    def __init__(self, config_or_client: CogniteClient | ClientConfig):
        if isinstance(config_or_client, CogniteClient):
            client = config_or_client
        elif isinstance(config_or_client, ClientConfig):
            client = CogniteClient(config_or_client)
        else:
            raise ValueError(f"Expected CogniteClient or ClientConfig, got {type(config_or_client)}")
        # The client name is used for aggregated logging of Pygen Usage
        client.config.client_name = "CognitePygen:0.99.10"

        view_by_read_class = {
            data_classes.Alert: dm.ViewId("power-ops-shared", "Alert", "1"),
            data_classes.BidDocument: dm.ViewId("power-ops-afrr-bid", "BidDocument", "1"),
            data_classes.BidMethod: dm.ViewId("power-ops-afrr-bid", "BidMethod", "1"),
            data_classes.BidRow: dm.ViewId("power-ops-afrr-bid", "BidRow", "1"),
            data_classes.PriceArea: dm.ViewId("power-ops-afrr-bid", "PriceArea", "1"),
        }
        self._view_by_read_class = view_by_read_class
        self._client = client

        self.alert = AlertAPI(client, view_by_read_class)
        self.bid_document = BidDocumentAPI(client, view_by_read_class)
        self.bid_method = BidMethodAPI(client, view_by_read_class)
        self.bid_row = BidRowAPI(client, view_by_read_class)
        self.price_area = PriceAreaAPI(client, view_by_read_class)

    def upsert(
        self,
        items: data_classes.DomainModelWrite | Sequence[data_classes.DomainModelWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> data_classes.ResourcesWriteResult:
        """Add or update (upsert) items.

        Args:
            items: One or more instances of the pygen generated data classes.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method will, by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        """
        if isinstance(items, data_classes.DomainModelWrite):
            instances = items.to_instances_write(self._view_by_read_class, write_none)
        else:
            instances = data_classes.ResourcesWrite()
            cache: set[tuple[str, str]] = set()
            for item in items:
                instances.extend(item._to_instances_write(cache, self._view_by_read_class, write_none))
        result = self._client.data_modeling.instances.apply(
            nodes=instances.nodes,
            edges=instances.edges,
            auto_create_start_nodes=True,
            auto_create_end_nodes=True,
            replace=replace,
        )
        time_series = []
        if instances.time_series:
            time_series = self._client.time_series.upsert(instances.time_series, mode="patch")

        return data_classes.ResourcesWriteResult(result.nodes, result.edges, TimeSeriesList(time_series))

    def apply(
        self,
        items: data_classes.DomainModelWrite | Sequence[data_classes.DomainModelWrite],
        replace: bool = False,
        write_none: bool = False,
    ) -> data_classes.ResourcesWriteResult:
        """Add or update (upsert) items.

        Args:
            items: One or more instances of the pygen generated data classes.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method will, by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        """
        warnings.warn(
            "The .apply method is deprecated and will be removed in v1.0. "
            "Please use the .upsert method on the instead."
            "The motivation is that .upsert is a more descriptive name for the operation.",
            UserWarning,
            stacklevel=2,
        )
        return self.upsert(items, replace, write_none)

    def delete(
        self, external_id: str | SequenceNotStr[str], space: str = DEFAULT_INSTANCE_SPACE
    ) -> dm.InstancesDeleteResult:
        """Delete one or more items.

        Args:
            external_id: External id of the item(s) to delete.
            space: The space where all the item(s) are located.

        Returns:
            The instance(s), i.e., nodes and edges which has been deleted. Empty list if nothing was deleted.

        Examples:

            Delete item by id:

                >>> from omni import OmniClient
                >>> client = OmniClient()
                >>> client.delete("my_node_external_id")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> AFRRBidAPI:
        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)
        config = ClientConfig.default(project, cdf_cluster, credentials)

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str, section: str | None = "cognite") -> AFRRBidAPI:
        import toml

        toml_content = toml.load(file_path)
        if section is not None:
            try:
                toml_content = toml_content[section]
            except KeyError as e:
                raise ValueError(f"Could not find section '{section}' in {file_path}") from e

        return cls.azure_project(**toml_content)

    def _repr_html_(self) -> str:
        return """<strong>AFRRBidAPI</strong> generated from data model ("power-ops-afrr-bid", "AFRRBid", "1")<br />
with the following APIs available<br />
&nbsp;&nbsp;&nbsp;&nbsp;.alert<br />
&nbsp;&nbsp;&nbsp;&nbsp;.bid_document<br />
&nbsp;&nbsp;&nbsp;&nbsp;.bid_method<br />
&nbsp;&nbsp;&nbsp;&nbsp;.bid_row<br />
&nbsp;&nbsp;&nbsp;&nbsp;.price_area<br />
<br />
and with the methods:<br />
&nbsp;&nbsp;&nbsp;&nbsp;.upsert - Create or update any instance.<br />
&nbsp;&nbsp;&nbsp;&nbsp;.delete - Delete instances.<br />
"""
