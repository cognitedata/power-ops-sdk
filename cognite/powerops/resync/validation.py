"""
Checks whether time series referenced in a model actually exist in CDF
and have data in the required time ranges.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Optional, Union, cast

import arrow
import numpy as np
import pandas as pd
from cognite.client.data_classes import TimeSeries
from pydantic import BaseModel, ConfigDict

from cognite.powerops import PowerOpsClient
from cognite.powerops.client._generated.data_classes import (
    InputTimeSeriesMapping,
    InputTimeSeriesMappingApply,
)
from cognite.powerops.resync.models.base import Model
from cognite.powerops.utils.preprocessor_utils import arrow_to_ms, retrieve_time_series_datapoints
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


def find_all_mappings(process) -> list[InputTimeSeriesMappingApply]:
    def attr_values(value, attr_subpath):
        if len(attr_subpath) == 0 or value is None:
            yield value
        else:
            subattr = attr_subpath[0]
            if callable(subattr):
                yield from subattr(value, attr_subpath[1:])
            else:
                if isinstance(value, dict):
                    val = value.get(subattr)
                else:
                    val = getattr(value, subattr, None)
                yield from attr_values(val, attr_subpath[1:])

    def each(items, attr_subpath):
        for item in items:
            yield from attr_values(item, attr_subpath)

    def to_dict_records(item, attr_subpath):
        return attr_values(item.replace(np.nan, None).to_dict("records"), attr_subpath)

    def to_input_time_series_mapping_apply(item, attr_subpath):
        if item.get("time_series_external_id") is None:
            return None, attr_subpath
        shop_obj_type, shop_obj_name, shop_attr_name = item["shop_model_path"].split(".")
        return (
            InputTimeSeriesMappingApply(
                external_id=item["time_series_external_id"],
                aggregation=item["aggregation"],
                retrieve=item["retrieve"],
                cdf_time_series=item["time_series_external_id"],
                transformations=[],  # TODO fill out from item["transformations"] + item["transformations1"] + ...
                shop_object_type=shop_obj_type,
                shop_object_name=shop_obj_name,
                shop_attribute_name=shop_attr_name,
            ),
            attr_subpath,
        )

    possible_attrs = [
        ["incremental_mapping", each, "content", to_dict_records, each, to_input_time_series_mapping_apply],
    ]

    mappings: list[InputTimeSeriesMappingApply] = []
    for attr_path in possible_attrs:
        mappings.extend(filter(None, attr_values(process, attr_path)))
    return mappings


PreparedValidationsT = dict[str, dict[str, TimeSeriesValidation]]
ValidationRangesT = dict[str, ValidationRange]


def prepare_validation(
    models: list[Model], additional_timeseries: list[TimeSeries]
) -> tuple[PreparedValidationsT, ValidationRangesT]:
    ts_validations = cast(PreparedValidationsT, defaultdict(lambda: defaultdict(dict)))
    validation_ranges: dict[str, ValidationRange] = {}

    for model in models:
        for process in model.processes:  # type: ignore[attr-defined]
            starttime = relative_time_specification_to_arrow(process.shop.starttime)
            endtime = relative_time_specification_to_arrow(process.shop.endtime)
            validation_range = ValidationRange(start=starttime, end=endtime)
            validation_ranges[str(validation_range)] = validation_range

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

    # Add validations for "additional_timeseries".
    # Assume these are needed in all validation ranges.
    for additional_ts in additional_timeseries:
        for validation_range_str, ts_validations_in_range in ts_validations.items():
            ts_validations_in_range[require(additional_ts.external_id)] = TimeSeriesValidation(
                validation_range=validation_ranges[validation_range_str],
                mapping=InputTimeSeriesMappingApply(  # dummy mapping, for validation only
                    external_id=require(additional_ts.external_id),
                    cdf_time_series=require(additional_ts.external_id),
                    retrieve="RANGE",
                    shop_attribute_name="N/A",  # could get this from the model, possibly enhance .timeseries()
                ),
            )

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
            data: pd.Series = datapoints.get(require(ts_mapping.cdf_time_series), pd.Series())
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
            logger.debug(f"Validated timeseries {ts_mapping.cdf_time_series} in range {range_str}.")
        if warnings_n:
            logger.warning(f"Total warnings for range {range_str}: {warnings_n}.")
