from typing import Any

from cognite.powerops.data_classes.time_series_mapping import TimeSeriesMapping, TimeSeriesMappingEntry
from cognite.powerops.data_classes.transformation import Transformation, TransformationType

ignored_attributes = [
    "gate.schedule_flag",
    "gate.schedule_m3s",
    "gate.schedule_percent",
    "generator.committed_flag",
    "generator.committed_in",
    "generator.maintenance_flag",
    "generator.production_schedule",
    "generator.production_schedule_flag",
    "market.buy_price",
    "market.sale_price",
    "plant.feeding_fee",
    "plant.maintenance_flag",
    "plant.production_schedule",
    "plant.production_schedule_flag",
    "reservoir.inflow",
    "reservoir.schedule",
    "reservoir.schedule_flag",
    "reservoir.min_vol_constr",
    "reservoir.max_vol_constr",
    "reservoir.tactical_cost_max",
    "reservoir.tactical_cost_min",
    "reservoir.tacical_limit_max",
    "reservoir.tactical_limit_min",
]


def is_constant_valued_dict(attribute_value: Any) -> bool:
    """Check if all dictionary values are the same"""
    return (
        isinstance(attribute_value, dict)
        and all(isinstance(value, (float, int)) for value in attribute_value.values())
        and len(set(attribute_value.values())) == 1
    )


def get_static_mapping(shop_model: dict) -> TimeSeriesMapping:
    # sourcery skip: for-append-to-extend, merge-nested-ifs
    static_mapping = TimeSeriesMapping()
    # e.g. reservoir: [reservoir_A, reservoir_B]
    for object_type, objects in shop_model.items():
        # e.g. reservoir_A: [maintenance_flag, max_vol, ...]
        for object_name, object_attributes in objects.items():
            # e.g. max_vol: 42 or maintenance_flag: {<date>: 1, <other_date>: 0}
            for attribute_name, attribute_value in object_attributes.items():
                if f"{object_type}.{attribute_name}" in ignored_attributes:
                    continue

                if is_constant_valued_dict(attribute_value):
                    static_value = {0: list(attribute_value.values())[0]}  # TODO: clean up
                    static_mapping.append(
                        TimeSeriesMappingEntry(
                            object_type=object_type,
                            object_name=str(object_name),
                            attribute_name=attribute_name,
                            transformations=[
                                Transformation(transformation=TransformationType.STATIC, kwargs=static_value)
                            ],
                        )
                    )

    return static_mapping
