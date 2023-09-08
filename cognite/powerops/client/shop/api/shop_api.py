from __future__ import annotations

import logging

from cognite.powerops.client._generated._api_client import CogShopAPIs
from cognite.powerops.client.shop.data_classes.shop_model import ShopModel

logger = logging.getLogger(__name__)


class ShopModelsAPI:
    """
        The SHOPModels API is used to interact with SHOP models in CDF.

    !!! warning "This is not implemented yet!"
        This API only returns dummy data.

    """

    def __init__(self, cogshop: CogShopAPIs):
        self._cogshop = cogshop

    def list(self) -> list[ShopModel]:
        """
        List all models .

        Returns:
            list[ShopModel]: List of SHOP models.
        """
        return [ShopModel()]

    def retrieve(self, model_id: int) -> ShopModel | None:
        """
        Retrieve a SHOP model by id.

        Args:
            model_id: The identifier of the model to retrieve.

        Returns:
            The retrieved model if it exists, otherwise None.
        """
        m = ShopModel()
        m.model_id = model_id
        return m
