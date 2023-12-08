"""Set externalId factories"""


__all__ = []

from typing import Optional

from cognite.powerops.client._generated.day_ahead_bids.data_classes import (
    SHOPMultiScenarioApply,
    SHOPTableApply,
    WaterValueBasedApply,
)


def _prefix(value: Optional[str], prefix: str) -> Optional[str]:
    if value is not None and not value.startswith(prefix):
        return f"{prefix}{value}"
    return value


def prefix_shop(domain_cls: type, data: dict) -> Optional[str]:
    return _prefix(data.get("external_id"), "SHOP_")


def prefix_wvb(domain_cls: type, data: dict) -> Optional[str]:
    return _prefix(data.get("external_id"), "WVB_")


SHOPTableApply.external_id_factory = prefix_shop
SHOPMultiScenarioApply.external_id_factory = prefix_shop
WaterValueBasedApply.external_id_factory = prefix_wvb
