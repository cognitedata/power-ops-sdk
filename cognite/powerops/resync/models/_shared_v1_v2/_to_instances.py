"""
The module contains functions for transforming the ReSync configuration into Cognite Data Model Types which are used
in multiple models. Note the use of the `make_ext_id` function which is used to generate unique external IDs for
all the conversions. This is done to ensure that the same external ID is used for the same object across multiple
transformations.
"""

from __future__ import annotations

import json
from hashlib import md5
from typing import Any

from cognite.powerops.client.data_classes import (
    DateTransformationApply,
    InputTimeSeriesMappingApply,
    ScenarioMappingApply,
    ShopTransformationApply,
    ValueTransformationApply,
)
from cognite.powerops.resync.config._shared import TimeSeriesMapping, TimeSeriesMappingEntry, Transformation
from cognite.powerops.resync.config.market._core import RelativeTime


def _to_date_transformations(time: RelativeTime | list[DateTransformationApply]) -> list[DateTransformationApply]:
    if isinstance(time, list) and time and isinstance(time[0], DateTransformationApply):
        return time
    output = []
    for operation in time.operations or []:
        method, arguments = operation
        args, kwargs = [], {}
        if isinstance(arguments, dict):
            kwargs = arguments
        elif isinstance(arguments, list):
            args = arguments
        elif isinstance(arguments, str):
            args = [arguments]
        else:
            raise ValueError(f"Unknown arguments type: {type(arguments)}")
        output.append(
            DateTransformationApply(
                external_id=make_ext_id(operation, DateTransformationApply),
                transformation=method,
                args=args,
                kwargs=kwargs,
            )
        )
    return output


def _to_shop_transformation(
    start: RelativeTime | list[DateTransformationApply], end: RelativeTime | list[DateTransformationApply]
) -> ShopTransformationApply:
    start_list = _to_date_transformations(start)
    end_list = _to_date_transformations(end)
    external_id = make_ext_id([d.model_dump_json() for d in start_list + end_list], ShopTransformationApply)
    return ShopTransformationApply(
        external_id=external_id,
        type_name=ShopTransformationApply.__name__.removesuffix("Apply"),
        start=start_list,
        end=end_list,
    )


def _to_input_timeseries_mapping(entry: TimeSeriesMappingEntry) -> InputTimeSeriesMappingApply:
    return InputTimeSeriesMappingApply(
        external_id=make_ext_id(entry.model_dump_json(), class_=InputTimeSeriesMappingApply),
        aggregation=entry.aggregation.name if entry.aggregation else None,
        cdf_time_series=entry.time_series_external_id,
        shop_object_type=entry.object_type,
        shop_object_name=entry.object_name,
        shop_attribute_name=entry.attribute_name,
        retrieve=entry.retrieve.name if entry.retrieve else None,
        transformations=[_to_value_transformation(t) for t in (entry.transformations or [])],
    )


def _to_value_transformation(transformation: Transformation) -> ValueTransformationApply:
    return ValueTransformationApply(
        external_id=make_ext_id(transformation.model_dump_json(), class_=ValueTransformationApply),
        method=transformation.transformation.name,
        arguments=transformation.kwargs,
    )


def _to_scenario_mapping(external_id: str, name: str, time_series_mapping: TimeSeriesMapping) -> ScenarioMappingApply:
    mappings = [_to_input_timeseries_mapping(entry) for entry in time_series_mapping]

    return ScenarioMappingApply(external_id=external_id, mapping_override=mappings, name=name)


def make_ext_id(arg: Any, class_: type) -> str:
    hash_value = md5()
    if isinstance(arg, (str, int, float, bool)):
        hash_value.update(str(arg).encode())
    elif isinstance(arg, (list, dict, tuple)):
        hash_value.update(json.dumps(arg).encode())
    return f"{class_.__name__.removesuffix('Apply')}__{hash_value.hexdigest()}"
