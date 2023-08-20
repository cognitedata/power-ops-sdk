"""
Checks whether time series referenced in a model actually exist in CDF
and have data in the required time ranges.
"""

from __future__ import annotations

import logging
from collections import defaultdict

import arrow
from typing import Optional, Union, List, cast, Dict

from pydantic import BaseModel, ConfigDict

from cognite.powerops.clients.powerops_client import PowerOpsClient
from cognite.powerops.clients.data_classes import (
    DateTransformation,
    DateTransformationApply,
    InputTimeSeriesMapping,
    InputTimeSeriesMappingApply,
)
from cognite.powerops.utils.preprocessor_utils import retrieve_time_series_datapoints, arrow_to_ms
from cognite.powerops.resync.models.base import Model
from cognite.powerops.utils.require import require
from cognite.powerops.utils.time import relative_time_specification_to_arrow


logger = logging.getLogger(__name__)


class ValidationSpec(BaseModel):
    is_valid: Optional[bool] = None


class TimeSeriesValidation(ValidationSpec):
    mapping: Union[InputTimeSeriesMapping, InputTimeSeriesMappingApply]
    validation_range: ValidationRange
    data_models: list[str] = []

    model_config = ConfigDict(arbitrary_types_allowed=True)


class ValidationRange(BaseModel):
    start: arrow.Arrow
    end: arrow.Arrow

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def __str__(self):
        return f"{self.start} - {self.end}"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, ValidationRange):
            return self.start == other.start and self.end == other.end
        return super().__eq__(other)


def date_transformations_to_arrow(
    date_transformations: List[Union[DateTransformation, DateTransformationApply]]
) -> arrow.Arrow:
    return relative_time_specification_to_arrow(
        [
            (
                require(date_transformation.transformation, as_type=str),
                date_transformation.args[0] if date_transformation.args else date_transformation.kwargs,
            )
            for date_transformation in date_transformations
        ]
    )


def find_all_mappings(process) -> List[InputTimeSeriesMappingApply]:
    def attr_values(value, attr_subpath):
        if len(attr_subpath) == 0 or value is None:
            yield value
        else:
            subattr = attr_subpath[0]
            if callable(subattr):
                yield from subattr(value, attr_subpath[1:])
            else:
                yield from attr_values(getattr(value, subattr, None), attr_subpath[1:])

    def each(items, attr_subpath):
        for item in items:
            yield from attr_values(item, attr_subpath)

    possible_attrs = [
        ["incremental_mappings", each, "mapping_override", each],
    ]

    mappings: List[InputTimeSeriesMappingApply] = []
    for attr_path in possible_attrs:
        mappings.extend(filter(None, attr_values(process, attr_path)))
    return mappings


PreparedValidationsT = Dict[str, Dict[str, TimeSeriesValidation]]
ValidationRangesT = Dict[str, ValidationRange]


def prepare_validation(models: list[Model]) -> tuple[PreparedValidationsT, ValidationRangesT]:
    ts_validations = cast(PreparedValidationsT, defaultdict(lambda: defaultdict(dict)))
    validation_ranges: dict[str, ValidationRange] = {}

    for model in models:
        for process in model.processes:  # type: ignore[attr-defined]
            validation_range = ValidationRange(
                start=date_transformations_to_arrow(process.shop.start),
                end=date_transformations_to_arrow(process.shop.end),
            )
            validation_ranges[str(validation_range)] = validation_range

            # time_series = model.time_series()  # TODO returns []
            mappings = find_all_mappings(process)

            for mapping in mappings:
                ts_validation = ts_validations[f"{validation_range}"].get(mapping.external_id)
                if ts_validation is None:
                    ts_validation = TimeSeriesValidation(
                        mapping=mapping,
                        validation_range=validation_range,
                    )
                if model.model_name not in ts_validation.data_models:
                    ts_validation.data_models.append(model.model_name)
                ts_validations[f"{validation_range}"][mapping.external_id] = ts_validation
    return ts_validations, validation_ranges


def perform_validation(
    po_client: PowerOpsClient, ts_validations: PreparedValidationsT, validation_ranges: ValidationRangesT
) -> None:
    for range_str, validations_in_range in ts_validations.items():
        validation_range = validation_ranges[range_str]
        all_mappings = [validation.mapping for validation in validations_in_range.values()]
        ts_mappings = list(filter(lambda row: row.cdf_time_series, all_mappings))

        logger.info(f"Retrieving datapoints for {len(ts_mappings)} mappings from range {range_str}")
        datapoints = retrieve_time_series_datapoints(
            po_client.cdf,
            ts_mappings,
            start=arrow_to_ms(validation_range.start),
            end=arrow_to_ms(validation_range.end),
        )
        warnings_n = 0
        for ts_mapping in ts_mappings:
            data = datapoints[require(ts_mapping.cdf_time_series)]
            if not len(data):
                ts_validation: TimeSeriesValidation = ts_validations[range_str][ts_mapping.external_id]
                logger.warning(
                    f"No datapoints found for range {range_str} in timeseries '{ts_mapping.cdf_time_series}',"
                    f" used in models: {', '.join(ts_validation.data_models)}."
                )
                warnings_n += 1
                continue
            if ts_mapping.retrieve == "RANGE" and len(data) < 2:
                ts_validation = ts_validations[range_str][ts_mapping.external_id]
                logger.warning(
                    f"Only one datapoint found for range {range_str} in timeseries '{ts_mapping.cdf_time_series}',"
                    f" used in models: {', '.join(ts_validation.data_models)}."
                )
                warnings_n += 1
                continue
        if warnings_n:
            logger.warning(f"Total warnings for range {range_str}: {warnings_n}.")
