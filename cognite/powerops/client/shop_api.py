from __future__ import annotations

import logging
import random
from typing import TYPE_CHECKING

from cognite.powerops.client.shop_result_files import ShopFilesAPI
from cognite.powerops.client.shop_results import ShopRunResultsAPI
from cognite.powerops.client.shop_run import ShopRunsAPI
from cognite.powerops.client.shop_run_compare import ShopRunCompareAPI

if TYPE_CHECKING:
    from cognite.powerops import PowerOpsClient

logger = logging.getLogger(__name__)


class ShopModel:
    def __init__(self) -> None:
        self.model_id = random.randint(1000, 9999)

    def render_yaml(self) -> str:
        return "sintef_shop_model_yaml_representation"

    def update(self):
        raise NotImplementedError


class ShopModelsAPI:
    def __init__(self, po_client: PowerOpsClient):
        self._po_client = po_client

    def list(self) -> list[ShopModel]:
        return [ShopModel()]

    def retrieve(self, model_id):
        m = ShopModel()
        m.model_id = model_id
        return m


class ShopAPI:
    def __init__(self, po_client: PowerOpsClient):
        self._po_client = po_client
        self.models = ShopModelsAPI(po_client)
        self.runs = ShopRunsAPI(po_client)
        self.results = ShopRunResultsAPI(po_client)
        self.compare = ShopRunCompareAPI(po_client)
        self.files = ShopFilesAPI(po_client)
