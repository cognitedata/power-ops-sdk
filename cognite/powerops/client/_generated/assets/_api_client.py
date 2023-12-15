from __future__ import annotations

from pathlib import Path

from cognite.client import ClientConfig, CogniteClient, data_modeling as dm
from cognite.client.credentials import OAuthClientCredentials

from ._api.bid_method import BidMethodAPI
from ._api.generator import GeneratorAPI
from ._api.generator_efficiency_curve import GeneratorEfficiencyCurveAPI
from ._api.plant import PlantAPI
from ._api.price_area import PriceAreaAPI
from ._api.reservoir import ReservoirAPI
from ._api.turbine_efficiency_curve import TurbineEfficiencyCurveAPI
from ._api.watercourse import WatercourseAPI
from . import data_classes


class PowerAssetAPI:
    """
    PowerAssetAPI

    Generated with:
        pygen = 0.32.1
        cognite-sdk = 7.6.0
        pydantic = 2.5.2

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
        client.config.client_name = "CognitePygen:0.32.1"

        view_by_write_class = {
            data_classes.BidMethodApply: dm.ViewId("power-ops-shared", "BidMethod", "1"),
            data_classes.GeneratorApply: dm.ViewId("power-ops-assets", "Generator", "1"),
            data_classes.GeneratorEfficiencyCurveApply: dm.ViewId("power-ops-assets", "GeneratorEfficiencyCurve", "1"),
            data_classes.PlantApply: dm.ViewId("power-ops-assets", "Plant", "1"),
            data_classes.PriceAreaApply: dm.ViewId("power-ops-assets", "PriceArea", "1"),
            data_classes.ReservoirApply: dm.ViewId("power-ops-assets", "Reservoir", "1"),
            data_classes.TurbineEfficiencyCurveApply: dm.ViewId("power-ops-assets", "TurbineEfficiencyCurve", "1"),
            data_classes.WatercourseApply: dm.ViewId("power-ops-assets", "Watercourse", "1"),
        }

        self.bid_method = BidMethodAPI(client, view_by_write_class)
        self.generator = GeneratorAPI(client, view_by_write_class)
        self.generator_efficiency_curve = GeneratorEfficiencyCurveAPI(client, view_by_write_class)
        self.plant = PlantAPI(client, view_by_write_class)
        self.price_area = PriceAreaAPI(client, view_by_write_class)
        self.reservoir = ReservoirAPI(client, view_by_write_class)
        self.turbine_efficiency_curve = TurbineEfficiencyCurveAPI(client, view_by_write_class)
        self.watercourse = WatercourseAPI(client, view_by_write_class)
