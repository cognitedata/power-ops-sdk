from __future__ import annotations

from cognite.client.data_classes import Asset, Label, Relationship

from cognite.powerops.bootstrap.data_classes.cdf_labels import RelationshipLabel


def basic_relationship(
    source_external_id: str,
    source_type: str,
    target_external_id: str,
    target_type: str,
    label: RelationshipLabel,
) -> Relationship:
    return Relationship(
        external_id=f"{source_external_id}.{target_external_id}",
        source_external_id=source_external_id,
        source_type=source_type,
        target_external_id=target_external_id,
        target_type=target_type,
        labels=[Label(external_id=label.value)],
    )


def asset_to_asset(
    source: Asset | str,
    target: Asset | str,
    label: RelationshipLabel,
) -> Relationship:
    return basic_relationship(
        source_type="ASSET",
        target_type="ASSET",
        source_external_id=source if isinstance(source, str) else source.external_id,
        target_external_id=target if isinstance(target, str) else target.external_id,
        label=label,
    )


def asset_to_time_series(
    source_external_id: str,
    target_external_id: str,
    label: RelationshipLabel,
) -> Relationship:
    return basic_relationship(
        source_type="ASSET",
        target_type="TIMESERIES",
        source_external_id=source_external_id,
        target_external_id=target_external_id,
        label=label,
    )


def _asset_to_time_series(
    source_external_id: str,
    target_external_id: str,
    label: RelationshipLabel,
) -> Relationship:
    return asset_to_time_series(
        source_external_id=source_external_id,
        target_external_id=target_external_id,
        label=label,
    )


def plant_to_inlet_reservoir(plant: Asset | str, reservoir: Asset | str) -> Relationship:
    return asset_to_asset(
        source=plant,
        target=reservoir,
        label=RelationshipLabel.INLET_RESERVOIR,
    )


def plant_to_generator(plant: Asset | str, generator: Asset | str) -> Relationship:
    return asset_to_asset(
        source=plant,
        target=generator,
        label=RelationshipLabel.GENERATOR,
    )


def price_area_to_dayahead_price(price_area_asset: Asset, dayahead_price_time_series_external_id: str) -> Relationship:
    return _asset_to_time_series(
        source_external_id=price_area_asset.external_id,
        target_external_id=dayahead_price_time_series_external_id,
        label=RelationshipLabel.DAYAHEAD_PRICE_TIME_SERIES,
    )
