from __future__ import annotations

import logging
import random

from cognite.powerops.clients._api_client import CogShopAPIs

logger = logging.getLogger(__name__)


class ShopModel:
    def __init__(self) -> None:
        self.model_id = random.randint(1000, 9999)

    def render_yaml(self) -> str:
        return "sintef_shop_model_yaml_representation"

    def update(self):
        raise NotImplementedError


class ShopModelsAPI:
    def __init__(self, cogshop: CogShopAPIs):
        self._cogshop = cogshop

    def list(self) -> list[ShopModel]:
        return [ShopModel()]

    def retrieve(self, model_id):
        m = ShopModel()
        m.model_id = model_id
        return m
