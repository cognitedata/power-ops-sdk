from __future__ import annotations

from cognite.client.data_classes import Asset, Label

from cognite.powerops.bootstrap.data_classes.cdf_labels import AssetLabel


def price_area_asset(name: str) -> Asset:
    return Asset(
        external_id=f"price_area_{name}",
        name=name,
        parent_external_id="price_areas",
        labels=[Label(AssetLabel.PRICE_AREA.value)],
    )
