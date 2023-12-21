"""
Checks whether time series referenced in a model actually exist in CDF
and have data in the required time ranges.
"""

from __future__ import annotations

#
#
#
#
#
# class ValidationSpec(BaseModel):
#     pass

#
# class TimeSeriesValidation(ValidationSpec):
#
#
#
# class ValidationRange(BaseModel):
#
#
#     def __str__(self):
#
#     def __eq__(self, other: object) -> bool:
#         if isinstance(other, ValidationRange):
#
#     @field_serializer("start", "end")
#     def serialize_arrow(self, value: arrow.Arrow, _info):
#
#
# class TimeSeriesValidationFailures(str, Enum):
#
#
# class ValidationResult(BaseModel):
#
#
# class ValidationResults(BaseModel):
#
#     def as_markdown(self, include_valid: bool) -> str:
#
#     def as_json(self, include_valid: bool) -> str:
#         if not include_valid:
#
#
# class TimeSeriesValidationResults(ValidationResults):
#
#     def as_markdown(self, include_valid: bool) -> str:
#             "| Time Range                                            | Valid | models | reason | ExternalID |",
#             "|-------------------------------------------------------|-------|--------|--------|------------|",
#         for result in self.results:
#             if result.valid and not include_valid:
#
#
# class TimeSeriesValidationResult(ValidationResult):
#
#     @model_validator(mode="before")
#     @classmethod
#     def set_valid(cls, data: dict) -> dict:
#         if "valid" not in data and "failure" not in data:
#         if data.get("valid") and "failure" in data:
#         if "valid" not in data:
#
#     @property
#     def reason(self) -> str:
#
#
# def find_mappings(obj, lookup: Literal["base", "process"]) -> list[InputTimeSeriesMappingApply]:
#     def df_to_dict_records(item, attr_path):
#
#     def to_input_time_series_mapping_apply(item, _attr_path):
#         if item.get("time_series_external_id") is not None:
#             yield InputTimeSeriesMappingApply(
#
#     def from_mapping_to_input_time_series_mapping_apply(item, _attr_path):
#         if item.timeseries_external_id is not None:
#             yield InputTimeSeriesMappingApply(
#
#         "process": [
#         ],
#         "base": [
#             #  ^ includes scenario mappings
#         ],
#
#     for lookup_path in lookup_paths[lookup]:
#
#
#
#
# def prepare_validation(models: list[Model]) -> tuple[PreparedValidationsT, ValidationRangesT]:
#     # data containers:
#
#     # Find processes (i.e. configuration for actual SHOP runs, with start and end times)
#     # Without this we don't know what time ranges to query from CDF.
#     for model in models:
#         if not hasattr(model, "processes"):
#         for process in model.processes:
#
#             # Find mappings that are configured within processes.
#             # For these mapping we only validate time ranges of the process.
#             for mapping in process_mappings:
#                 if ts_validation is None:
#                 if model.model_name not in ts_validation.data_models:
#
#     # Find base mappings.
#     # For these mappings we don't know the time ranges, so we validate all time ranges.
#     # Overwrite any process-mappings from above.
#     for model in models:
#         for mapping in base_mappings:
#             for validation_range_str, validation_range in validation_ranges.items():
#                 if model.model_name not in ts_validation.data_models:
#
#     # Get any additional timeseries that are part of the models but are not part of the mappings.
#     # These are ambiguous... TODO Overwrite previous mappings or not? These are hard to connect back to SHOP path.
#     for additional_ts in additional_timeseries:
#         for validation_range_str in validation_ranges:
#             ts_validations[validation_range_str][require(additional_ts.external_id)] = TimeSeriesValidation(
#                 mapping=InputTimeSeriesMappingApply(  # dummy mapping, for validation only
#                 ),
#
#
#
# def perform_validation(
#     po_client: PowerOpsClient, ts_validations: PreparedValidationsT, validation_ranges: ValidationRangesT
# ) -> ValidationResults:
#     for range_str, validations_in_range in ts_validations.items():
#
#         for ts_mapping in ts_mappings:
#             if not len(data):
#                 logger.info(
#                 results.append(
#                     TimeSeriesValidationResult(
#                 logger.info(
#                 results.append(
#                     TimeSeriesValidationResult(
#         if warnings_n:
