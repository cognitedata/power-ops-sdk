import re
from typing import Optional

from cognite.client.data_classes import Asset, Label

from cognite.powerops.utils.labels import AssetLabels


def price_area_asset(name: str) -> Asset:
    return Asset(
        external_id=f"price_area_{name}",
        name=name,
        parent_external_id="price_areas",
        labels=[Label(AssetLabels.PRICE_AREA)],
    )


def watercourse_asset(name: str, shop_penalty_limit: float) -> Asset:
    return Asset(
        external_id=f"watercourse_{name}",
        name=name,
        parent_external_id="watercourses",
        labels=[Label(AssetLabels.WATERCOURSE)],
        metadata={"shop:penalty_limit": str(shop_penalty_limit)},
    )


def plant_asset(name: str, display_name: Optional[str] = None, ordering_key: Optional[float] = None) -> Asset:
    return Asset(
        external_id=f"plant_{name}",
        name=name,
        parent_external_id="plants",
        metadata={
            "display_name": display_name or re.sub(r"\([0-9]+\)", "", name),
            "ordering": str(ordering_key) if ordering_key else "999",
        },
        labels=[Label(AssetLabels.PLANT)],
    )


def reservoir_asset(
    name: str,
    display_name: Optional[str] = None,
    ordering_key: Optional[float] = None,
) -> Asset:
    return Asset(
        external_id=f"reservoir_{name}",
        name=name,
        parent_external_id="reservoirs",
        metadata={
            "display_name": display_name or re.sub(r"\([0-9]+\)", "", name),
            "ordering": str(ordering_key) if ordering_key else "999",
        },
        labels=[Label(AssetLabels.RESERVOIR)],
    )


def generator_asset(name: str, penstock: str = "1", startcost: float = 0) -> Asset:
    return Asset(
        external_id=f"generator_{name}",
        name=name,
        parent_external_id="generators",
        labels=[Label(AssetLabels.GENERATOR)],
        metadata={"penstock": penstock, "startcost": startcost},
    )
