"""Set externalId factories"""


__all__ = []


from cognite.powerops.client._generated.day_ahead_frontend_contract.data_classes import (
    SHOPApply,
    SHOPTableApply,
    WaterValueBasedApply,
)


def _prefix(value: str, prefix: str) -> str:
    if value is not None and not value.startswith(prefix):
        return f"{prefix}{value}"
    return value


def prefix_shop(domain_cls: type, data: dict) -> str:
    return _prefix(data.get("external_id"), "SHOP_")


def prefix_wvb(domain_cls: type, data: dict) -> str:
    return _prefix(data.get("external_id"), "WVB_")


SHOPTableApply.external_id_factory = prefix_shop
SHOPApply.external_id_factory = prefix_shop
WaterValueBasedApply.external_id_factory = prefix_wvb
