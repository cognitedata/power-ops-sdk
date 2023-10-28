"""
Checks whether time series referenced in a model actually exist in CDF
and have data in the required time ranges.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from collections.abc import Sequence
from enum import Enum
from typing import Literal, Optional, Union, cast

import arrow
import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, field_serializer, model_validator

from cognite.powerops import PowerOpsClient
from cognite.powerops.client._generated.data_classes import (
    InputTimeSeriesMapping,
    InputTimeSeriesMappingApply,
)
from cognite.powerops.resync.models.base import Model
from cognite.powerops.utils.cdf.calls import retrieve_time_series_datapoints
from cognite.powerops.utils.lookup import attr_lookup, dict_values, each
from cognite.powerops.utils.require import require
from cognite.powerops.utils.time import arrow_to_ms, relative_time_specification_to_arrow

logger = logging.getLogger(__name__)


class ValidationSpec(BaseModel):
    pass


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

    @field_serializer("start", "end")
    def serialize_arrow(self, value: arrow.Arrow, _info):
        return value.isoformat()


class TimeSeriesValidationFailures(str, Enum):
    NO_DATAPOINTS = "NO_DATAPOINTS"
    SINGLE_DATAPOINT = "SINGLE_DATAPOINT"


class ValidationResult(BaseModel):
    valid: bool = False


class ValidationResults(BaseModel):
    results: Sequence[ValidationResult]

    def as_markdown(self, include_valid: bool) -> str:
        raise NotImplementedError()

    def as_json(self, include_valid: bool) -> str:
        results = self.results
        if not include_valid:
            results = [result for result in results if not result.valid]
        return json.dumps([result.model_dump(mode="json") for result in results], ensure_ascii=False)


class TimeSeriesValidationResults(ValidationResults):
    results: Sequence[TimeSeriesValidationResult]

    def as_markdown(self, include_valid: bool) -> str:
        lines = [
            "| Time Range                                            | Valid | models | reason | ExternalID |",
            "|-------------------------------------------------------|-------|--------|--------|------------|",
        ]
        for result in self.results:
            if result.valid and not include_valid:
                continue
            time_range = str(result.validation.validation_range)
            valid = str(result.valid)
            models = ",".join(result.validation.data_models)
            reason = result.reason
            external_id = result.validation.mapping.cdf_time_series
            lines.append(f"| {time_range} | {valid:5} | {models} | {reason} | {external_id} |")
        return "\n".join(lines)


class TimeSeriesValidationResult(ValidationResult):
    failure: Optional[TimeSeriesValidationFailures] = None
    validation: TimeSeriesValidation

    @model_validator(mode="before")
    @classmethod
    def set_valid(cls, data: dict) -> dict:
        if "valid" not in data and "failure" not in data:
            raise ValueError("One of valid=True or failure is required.")
        if data.get("valid") and "failure" in data:
            raise ValueError("One of valid=True or failure is required.")
        if "valid" not in data:
            data["valid"] = not bool(data.get("failure"))
        return data

    @property
    def reason(self) -> str:
        return "" if self.valid else require(self.failure).value


def find_mappings(obj, lookup: Literal["base", "process"]) -> list[InputTimeSeriesMappingApply]:
    def df_to_dict_records(item, attr_path):
        yield from attr_lookup(item.replace(np.nan, None).to_dict("records"), attr_path)

    def to_input_time_series_mapping_apply(item, _attr_path):
        if item.get("time_series_external_id") is not None:
            shop_obj_type, shop_obj_name, shop_attr_name = item["shop_model_path"].split(".")
            yield InputTimeSeriesMappingApply(
                external_id=item["time_series_external_id"],
                aggregation=item["aggregation"],
                retrieve=item["retrieve"],
                cdf_time_series=item["time_series_external_id"],
                transformations=[],  # TODO fill out from item["transformations"] + item["transformations1"] + ...
                shop_object_type=shop_obj_type,
                shop_object_name=shop_obj_name,
                shop_attribute_name=shop_attr_name,
            )

    def from_mapping_to_input_time_series_mapping_apply(item, _attr_path):
        if item.timeseries_external_id is not None:
            shop_obj_type, shop_obj_name, shop_attr_name = item.path.split(".")
            yield InputTimeSeriesMappingApply(
                external_id=item.timeseries_external_id,
                aggregation=item.aggregation,
                retrieve=item.retrieve,
                cdf_time_series=item.timeseries_external_id,
                transformations=[],  # TODO fill out rom item.transformations
                shop_object_type=shop_obj_type,
                shop_object_name=shop_obj_name,
                shop_attribute_name=shop_attr_name,
            )

    lookup_paths = {
        "process": [
            ["incremental_mapping", each, "content", df_to_dict_records, each, to_input_time_series_mapping_apply],
        ],
        "base": [
            ["mappings", dict_values, each, from_mapping_to_input_time_series_mapping_apply],
            #  ^ includes scenario mappings
        ],
    }

    mappings: list[InputTimeSeriesMappingApply] = []
    for lookup_path in lookup_paths[lookup]:
        mappings.extend(filter(None, list(attr_lookup(obj, lookup_path))))
    return mappings


PreparedValidationsT = dict[str, dict[str, TimeSeriesValidation]]
ValidationRangesT = dict[str, ValidationRange]


def prepare_validation(models: list[Model]) -> tuple[PreparedValidationsT, ValidationRangesT]:
    # data containers:
    ts_validations = cast(PreparedValidationsT, defaultdict(lambda: defaultdict(dict)))
    validation_ranges: dict[str, ValidationRange] = {}

    # Find processes (i.e. configuration for actual SHOP runs, with start and end times)
    # Without this we don't know what time ranges to query from CDF.
    for model in models:
        if not hasattr(model, "processes"):
            continue
        for process in model.processes:
            start_time = relative_time_specification_to_arrow(process.shop.starttime)
            end_time = relative_time_specification_to_arrow(process.shop.endtime)
            validation_range = ValidationRange(start=start_time, end=end_time)
            validation_ranges[str(validation_range)] = validation_range

            # Find mappings that are configured within processes.
            # For these mapping we only validate time ranges of the process.
            process_mappings = find_mappings(process, "process")
            for mapping in process_mappings:
                ts_validation = ts_validations[f"{validation_range}"].get(mapping.external_id)
                if ts_validation is None:
                    ts_validation = TimeSeriesValidation(
                        mapping=mapping,
                        validation_range=validation_range,
                    )
                if model.model_name not in ts_validation.data_models:
                    ts_validation.data_models.append(model.model_name)
                ts_validations[f"{validation_range}"][mapping.external_id] = ts_validation

    # Find base mappings.
    # For these mappings we don't know the time ranges, so we validate all time ranges.
    # Overwrite any process-mappings from above.
    for model in models:
        base_mappings = find_mappings(model, "base")
        for mapping in base_mappings:
            for validation_range_str, validation_range in validation_ranges.items():
                ts_validation = TimeSeriesValidation(
                    mapping=mapping,
                    validation_range=validation_range,
                )
                if model.model_name not in ts_validation.data_models:
                    ts_validation.data_models.append(model.model_name)
                ts_validations[validation_range_str][mapping.external_id] = ts_validation

    # Get any additional timeseries that are part of the models but are not part of the mappings.
    # These are ambiguous... TODO Overwrite previous mappings or not? These are hard to connect back to SHOP path.
    additional_timeseries = [ts for model in models for ts in model.timeseries()]
    for additional_ts in additional_timeseries:
        for validation_range_str in validation_ranges:
            ts_validations[validation_range_str][require(additional_ts.external_id)] = TimeSeriesValidation(
                validation_range=validation_ranges[validation_range_str],
                mapping=InputTimeSeriesMappingApply(  # dummy mapping, for validation only
                    external_id=require(additional_ts.external_id),  # type: ignore[call-arg]
                    cdf_time_series=require(additional_ts.external_id),  # type: ignore[call-arg]
                    retrieve="RANGE",
                    shop_attribute_name="N/A",  # could get this from the model, possibly enhance .timeseries()
                ),
            )

    return ts_validations, validation_ranges


def perform_validation(
    po_client: PowerOpsClient, ts_validations: PreparedValidationsT, validation_ranges: ValidationRangesT
) -> ValidationResults:
    results: list[TimeSeriesValidationResult] = []
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
            ts_validation: TimeSeriesValidation = ts_validations[range_str][ts_mapping.external_id]
            if not len(data):
                logger.info(
                    f"VALIDATION FAIL: No datapoints found for range {range_str} in timeseries "
                    f"'{ts_mapping.cdf_time_series}', used in models: {', '.join(ts_validation.data_models)}.",
                )
                results.append(
                    TimeSeriesValidationResult(
                        validation=ts_validation, failure=TimeSeriesValidationFailures.NO_DATAPOINTS
                    )
                )
                warnings_n += 1
            elif ts_mapping.retrieve == "RANGE" and len(data) < 2:
                logger.info(
                    f"VALIDATION FAIL: Only one datapoint found for range {range_str} in timeseries "
                    f"'{ts_mapping.cdf_time_series}', used in models: {', '.join(ts_validation.data_models)}.",
                )
                results.append(
                    TimeSeriesValidationResult(
                        validation=ts_validation, failure=TimeSeriesValidationFailures.SINGLE_DATAPOINT
                    )
                )
                warnings_n += 1
            else:
                results.append(TimeSeriesValidationResult(validation=ts_validation, valid=True))
        if warnings_n:
            logger.info(f"Total warnings for range {range_str}: {warnings_n}.")
    return TimeSeriesValidationResults(results=results)
