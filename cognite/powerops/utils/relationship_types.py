from __future__ import annotations

from cognite.client.data_classes import Asset, Label, Relationship, Sequence

from cognite.powerops.utils.labels import RelationshipLabels


def basic_relationship(
    source_external_id: str,
    source_type: str,
    target_external_id: str,
    target_type: str,
    label: str,
) -> Relationship:
    return Relationship(
        external_id=f"{source_external_id}.{target_external_id}",
        source_external_id=source_external_id,
        source_type=source_type,
        target_external_id=target_external_id,
        target_type=target_type,
        labels=[Label(external_id=label)],
    )


def asset_to_asset(
    source: Asset | str,
    target: Asset | str,
    label: str,
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
    label: str,
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
    label: str,
) -> Relationship:
    return asset_to_time_series(
        source_external_id=source_external_id,
        target_external_id=target_external_id,
        label=label,
    )


def _asset_to_sequence(
    source_external_id: str,
    target_external_id: str,
    label: str,
) -> Relationship:
    return Relationship(
        external_id=f"{source_external_id}.{target_external_id}",
        source_external_id=source_external_id,
        source_type="ASSET",
        target_external_id=target_external_id,
        target_type="SEQUENCE",
        labels=[Label(external_id=label)],
    )


def price_area_to_plant(price_area: Asset | str, plant: Asset | str) -> Relationship:
    return asset_to_asset(
        source=price_area,
        target=plant,
        label=RelationshipLabels.PLANT,
    )


def watercourse_to_plant(watercourse: Asset | str, plant: Asset | str) -> Relationship:
    return asset_to_asset(
        source=watercourse,
        target=plant,
        label=RelationshipLabels.PLANT,
    )


def price_area_to_watercourse(price_area: Asset | str, watercourse: Asset | str) -> Relationship:
    return asset_to_asset(
        source=price_area,
        target=watercourse,
        label=RelationshipLabels.WATERCOURSE,
    )


def plant_to_inlet_reservoir(plant: Asset | str, reservoir: Asset | str) -> Relationship:
    return asset_to_asset(
        source=plant,
        target=reservoir,
        label=RelationshipLabels.INLET_RESERVOIR,
    )


def plant_to_generator(plant: Asset | str, generator: Asset | str) -> Relationship:
    return asset_to_asset(
        source=plant,
        target=generator,
        label=RelationshipLabels.GENERATOR,
    )


def watercourse_to_production_obligation(watercourse: Asset, production_obligation_ts_ext_id: str) -> Relationship:
    return _asset_to_time_series(
        source_external_id=watercourse.external_id,
        target_external_id=production_obligation_ts_ext_id,
        label=RelationshipLabels.PRODUCTION_OBLIGATION_TIME_SERIES,
    )


def generator_to_generator_efficiency_curve(generator: Asset, generator_efficiency_curve: Sequence) -> Relationship:
    return _asset_to_sequence(
        source_external_id=generator.external_id,
        target_external_id=generator_efficiency_curve.external_id,
        label=RelationshipLabels.GENERATOR_EFFICIENCY_CURVE,
    )


def generator_to_turbine_efficiency_curve(generator: Asset, turbine_efficiency_curve: Sequence) -> Relationship:
    return _asset_to_sequence(
        source_external_id=generator.external_id,
        target_external_id=turbine_efficiency_curve.external_id,
        label=RelationshipLabels.TURBINE_EFFICIENCY_CURVE,
    )


def price_area_to_dayahead_price(price_area_asset: Asset, dayahead_price_time_series_external_id: str) -> Relationship:
    return _asset_to_time_series(
        source_external_id=price_area_asset.external_id,
        target_external_id=dayahead_price_time_series_external_id,
        label=RelationshipLabels.DAYAHEAD_PRICE_TIME_SERIES,
    )
