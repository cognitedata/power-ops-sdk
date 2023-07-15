"""
The module contains functions for transforming the ReSync configuration into Cognite Data Model Types which are used
in multiple models. Note the use of the `make_ext_id` function which is used to generate unique external IDs for
all the conversions. This is done to ensure that the same external ID is used for the same object across multiple
transformations.
"""

from __future__ import annotations

from cognite.powerops.clients.data_classes import (
    DateTransformationApply,
    InputTimeSeriesMappingApply,
    ShopTransformationApply,
    ValueTransformationApply,
)
from cognite.powerops.resync.config.market._core import RelativeTime
from cognite.powerops.resync.config.shared import TimeSeriesMappingEntry, Transformation
from cognite.powerops.resync.utils.common import make_ext_id


def _to_date_transformations(time: RelativeTime) -> list[DateTransformationApply]:
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
    start_list = _to_date_transformations(start) if isinstance(start, RelativeTime) else start
    end_list = _to_date_transformations(end) if isinstance(end, RelativeTime) else end
    external_id = make_ext_id([d.model_dump_json() for d in start_list + end_list], ShopTransformationApply)
    return ShopTransformationApply(
        external_id=external_id,
        start=start,
        end=end,
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
