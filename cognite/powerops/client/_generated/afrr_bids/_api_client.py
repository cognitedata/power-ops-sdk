from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient, data_modeling as dm
from cognite.client.credentials import OAuthClientCredentials

from ._api.alert import AlertAPI
from ._api.bid_document import BidDocumentAPI
from ._api.bid_method import BidMethodAPI
from ._api.bid_row import BidRowAPI
from ._api.price_area import PriceAreaAPI
from . import data_classes


class AFRRBidsAPI:
    """
    AFRRBidsAPI

    Generated with:
        pygen = 0.31.0
        cognite-sdk = 7.4.0
        pydantic = 2.5.2

    Data Model:
        space: power-ops-afrr-bids
        externalId: AFRRBids
        version: 1
    """

    def __init__(self, config_or_client: CogniteClient | ClientConfig):
        if isinstance(config_or_client, CogniteClient):
            client = config_or_client
        elif isinstance(config_or_client, ClientConfig):
            client = CogniteClient(config_or_client)
        else:
            raise ValueError(f"Expected CogniteClient or ClientConfig, got {type(config_or_client)}")
        view_by_write_class = {
            data_classes.AlertApply: dm.ViewId("power-ops-afrr-bids", "Alert", "1"),
            data_classes.BidDocumentApply: dm.ViewId("power-ops-afrr-bids", "BidDocument", "1"),
            data_classes.BidMethodApply: dm.ViewId("power-ops-afrr-bids", "BidMethod", "1"),
            data_classes.BidRowApply: dm.ViewId("power-ops-afrr-bids", "BidRow", "1"),
            data_classes.PriceAreaApply: dm.ViewId("power-ops-afrr-bids", "PriceArea", "1"),
        }

        self.alert = AlertAPI(client, view_by_write_class)
        self.bid_document = BidDocumentAPI(client, view_by_write_class)
        self.bid_method = BidMethodAPI(client, view_by_write_class)
        self.bid_row = BidRowAPI(client, view_by_write_class)
        self.price_area = PriceAreaAPI(client, view_by_write_class)

    @classmethod
    def azure_project(
        cls, tenant_id: str, client_id: str, client_secret: str, cdf_cluster: str, project: str
    ) -> AFRRBidsAPI:
        credentials = OAuthClientCredentials.default_for_azure_ad(tenant_id, client_id, client_secret, cdf_cluster)
        config = ClientConfig.default(project, cdf_cluster, credentials)

        return cls(config)

    @classmethod
    def from_toml(cls, file_path: Path | str, section: str | None = "cognite") -> AFRRBidsAPI:
        import toml

        toml_content = toml.load(file_path)
        if section is not None:
            try:
                toml_content = toml_content[section]
            except KeyError as e:
                raise ValueError(f"Could not find section '{section}' in {file_path}") from e

        return cls.azure_project(**toml_content)
