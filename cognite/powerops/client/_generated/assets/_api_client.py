from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any, Sequence

from cognite.client import ClientConfig, CogniteClient, data_modeling as dm
from cognite.client.data_classes import TimeSeriesList
from cognite.client.credentials import OAuthClientCredentials
from cognite.powerops.utils.deprecation import deprecated_class

from ._api.bid_method import BidMethodAPI
from ._api.generator import GeneratorAPI
from ._api.generator_efficiency_curve import GeneratorEfficiencyCurveAPI
from ._api.plant import PlantAPI
from ._api.price_area import PriceAreaAPI
from ._api.reservoir import ReservoirAPI
from ._api.turbine_efficiency_curve import TurbineEfficiencyCurveAPI
from ._api.watercourse import WatercourseAPI
from ._api._core import SequenceNotStr, GraphQLQueryResponse
from .data_classes._core import DEFAULT_INSTANCE_SPACE, GraphQLList
from . import data_classes

@deprecated_class
class PowerAssetAPI:
    """
    PowerAssetAPI

    Generated with:
        pygen = 0.99.22
        cognite-sdk = 7.37.4
        pydantic = 2.7.0

    Data Model:
        space: power-ops-assets
        externalId: PowerAsset
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
        client.config.client_name = "CognitePygen:0.99.22"

        view_by_read_class = {
            data_classes.BidMethod: dm.ViewId("power-ops-shared", "BidMethod", "1"),
            data_classes.Generator: dm.ViewId("power-ops-assets", "Generator", "1"),
            data_classes.GeneratorEfficiencyCurve: dm.ViewId("power-ops-assets", "GeneratorEfficiencyCurve", "1"),
            data_classes.Plant: dm.ViewId("power-ops-assets", "Plant", "1"),
            data_classes.PriceArea: dm.ViewId("power-ops-assets", "PriceArea", "1"),
            data_classes.Reservoir: dm.ViewId("power-ops-assets", "Reservoir", "1"),
            data_classes.TurbineEfficiencyCurve: dm.ViewId("power-ops-assets", "TurbineEfficiencyCurve", "1"),
            data_classes.Watercourse: dm.ViewId("power-ops-assets", "Watercourse", "1"),
        }
        self._view_by_read_class = view_by_read_class
        self._client = client

        self.bid_method = BidMethodAPI(client, view_by_read_class)
        self.generator = GeneratorAPI(client, view_by_read_class)
        self.generator_efficiency_curve = GeneratorEfficiencyCurveAPI(client, view_by_read_class)
        self.plant = PlantAPI(client, view_by_read_class)
        self.price_area = PriceAreaAPI(client, view_by_read_class)
        self.reservoir = ReservoirAPI(client, view_by_read_class)
        self.turbine_efficiency_curve = TurbineEfficiencyCurveAPI(client, view_by_read_class)
        self.watercourse = WatercourseAPI(client, view_by_read_class)

    def upsert(
        self,
        items: data_classes.DomainModelWrite | Sequence[data_classes.DomainModelWrite],
        replace: bool = False,
        write_none: bool = False,
        allow_version_increase: bool = False,
    ) -> data_classes.ResourcesWriteResult:
        """Add or update (upsert) items.

        Args:
            items: One or more instances of the pygen generated data classes.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)?
                Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.
            write_none (bool): This method will, by default, skip properties that are set to None. However, if you want to set properties to None,
                you can set this parameter to True. Note this only applies to properties that are nullable.
            allow_version_increase (bool): If set to true, the version of the instance will be increased if the instance already exists.
                If you get an error: 'A version conflict caused the ingest to fail', you can set this to true to allow
                the version to increase.
        Returns:
            Created instance(s), i.e., nodes, edges, and time series.

        """
        if isinstance(items, data_classes.DomainModelWrite):
            instances = items.to_instances_write(self._view_by_read_class, write_none, allow_version_increase)
        else:
            instances = data_classes.ResourcesWrite()
            cache: set[tuple[str, str]] = set()
            for item in items:
                instances.extend(
                    item._to_instances_write(
                        cache,
                        self._view_by_read_class,
                        write_none,
                        allow_version_increase,
                    )
                )
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

                >>> from cognite.powerops.client._generated.assets import PowerAssetAPI
                >>> client = PowerAssetAPI()
                >>> client.delete("my_node_external_id")
        """
        if isinstance(external_id, str):
            return self._client.data_modeling.instances.delete(nodes=(space, external_id))
        else:
            return self._client.data_modeling.instances.delete(
                nodes=[(space, id) for id in external_id],
            )

    def graphql_query(self, query: str, variables: dict[str, Any] | None = None) -> GraphQLList:
        """Execute a GraphQl query against the PowerAsset data model.

        Args:
            query (str): The GraphQL query to issue.
            variables (dict[str, Any] | None): An optional dict of variables to pass to the query.
        """
        data_model_id = dm.DataModelId("power-ops-assets", "PowerAsset", "1")
        result = self._client.data_modeling.graphql.query(data_model_id, query, variables)
        return GraphQLQueryResponse(data_model_id).parse(result)

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> PowerAssetAPI:
        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)
        config = ClientConfig.default(project, cdf_cluster, credentials)

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str, section: str | None = "cognite") -> PowerAssetAPI:
        import toml

        toml_content = toml.load(file_path)
        if section is not None:
            try:
                toml_content = toml_content[section]
            except KeyError as e:
                raise ValueError(f"Could not find section '{section}' in {file_path}") from e

        return cls.azure_project(**toml_content)

    def _repr_html_(self) -> str:
        return """<strong>PowerAssetAPI</strong> generated from data model ("power-ops-assets", "PowerAsset", "1")<br />
with the following APIs available<br />
&nbsp;&nbsp;&nbsp;&nbsp;.bid_method<br />
&nbsp;&nbsp;&nbsp;&nbsp;.generator<br />
&nbsp;&nbsp;&nbsp;&nbsp;.generator_efficiency_curve<br />
&nbsp;&nbsp;&nbsp;&nbsp;.plant<br />
&nbsp;&nbsp;&nbsp;&nbsp;.price_area<br />
&nbsp;&nbsp;&nbsp;&nbsp;.reservoir<br />
&nbsp;&nbsp;&nbsp;&nbsp;.turbine_efficiency_curve<br />
&nbsp;&nbsp;&nbsp;&nbsp;.watercourse<br />
<br />
and with the methods:<br />
&nbsp;&nbsp;&nbsp;&nbsp;.upsert - Create or update any instance.<br />
&nbsp;&nbsp;&nbsp;&nbsp;.delete - Delete instances.<br />
"""
