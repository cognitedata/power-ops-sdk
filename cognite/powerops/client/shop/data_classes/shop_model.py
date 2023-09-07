from __future__ import annotations

import random


class ShopModel:
    """
    This class represents a SHOP model in CDF.

    """

    def __init__(self) -> None:
        self.model_id = random.randint(1000, 9999)

    def render_yaml(self) -> str:
        """
        Render the model as a YAML string.

        Returns:
            str: The YAML representation of the model.

        """
        return "sintef_shop_model_yaml_representation"

    def update(self):
        """
        Update the model in CDF.

        !!! warning "This is not implemented yet!"
            This will raise a NotImplementedError.

        """
        raise NotImplementedError
