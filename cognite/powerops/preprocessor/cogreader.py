from __future__ import annotations

import tempfile
from functools import cached_property
from typing import List, Optional

import arrow
from cognite.client import CogniteClient

from cognite.powerops.preprocessor import knockoff_logging as logging
from cognite.powerops.preprocessor.data_classes import CogShopCase, CogShopConfig
from cognite.powerops.preprocessor.data_classes.time_series_mapping import TimeSeriesMapping
from cognite.powerops.preprocessor.exceptions import CogReaderError
from cognite.powerops.preprocessor.utils import (
    ShopMetadata,
    arrow_to_ms,
    download_file,
    log_and_reraise,
    retrieve_yaml_file,
)

from .get_fdm_data import Case, get_case

logger = logging.getLogger(__name__)


class CogReader:
    """Read data from CDF and preprocess for SHOP"""

    def __init__(
        self,
        client: CogniteClient,
        fdm_space_external_id: str,
        fdm_case_external_id: str,
        fdm_model_version: Optional[int] = None,
    ) -> None:
        self.client: CogniteClient = client

        self.fdm_space_external_id = fdm_space_external_id
        self.fdm_case_external_id = fdm_case_external_id
        self.fdm_model_version = fdm_model_version

        self._tmp_dir = tempfile.TemporaryDirectory()
        logger.info(f"{self.__class__.__name__} initialized.")

    @property
    def file_external_id_prefix(self) -> str:
        return f"SHOP_{self.cog_shop_config.watercourse}_"

    @property
    def fdm_case(self) -> Case:
        try:
            return self._fdm_case
        except AttributeError:
            raise RuntimeError("Expected to to run `retrieve_case()` method first.") from None

    def retrieve_case(self) -> None:
        self._fdm_case = get_case(
            self.client,
            self.fdm_space_external_id,
            self.fdm_case_external_id,
            model_version=self.fdm_model_version,
        )

    @cached_property
    def cog_shop_config(self) -> CogShopConfig:
        config = CogShopConfig(
            watercourse=self.fdm_case.scenario.model_template.watercourse,
            starttime=self.fdm_case.start_time,
            endtime=self.fdm_case.end_time,
        )
        logger.debug(f"{config} loaded from event metadata.")
        return config

    @cached_property
    def cog_shop_case(self) -> CogShopCase:
        """
        Download and parse model yaml file from cdf and merge with commands.
        Instantiate a CogShopCase from the resulting dict.
        """
        case_dict = retrieve_yaml_file(
            self.client,
            file_external_id=self.fdm_case.scenario.model_template.model.file_external_id,
        )
        case_dict["commands"] = self.fdm_case.scenario.commands.commands

        cog_shop_case = CogShopCase.from_dict(case_dict)
        logger.debug("SHOP case loaded.")
        return cog_shop_case

    @cached_property
    def base_mapping(self) -> List[TimeSeriesMapping]:
        mappings = self.fdm_case.scenario.model_template.base_mappings.items
        return [TimeSeriesMapping.from_mapping_model(m) for m in mappings]

    @cached_property
    def incremental_mapping(self) -> Optional[List[TimeSeriesMapping]]:
        return []
        # TODO uncomment, SHOP needs this:
        # relationships = self.client.relationships.list(
        #     source_external_ids=[self.shop_event.external_id],  # BROKEN
        #     data_set_ids=self.shop_event.data_set_id,           # BROKEN
        #     target_types=["sequence"],
        #     labels=LabelFilter(contains_all=[RelationshipsConfig.INCREMENTAL_MAPPING_LABEL]),
        # )
        # if not relationships:
        #     logger.info("No relationships to 'incremental mapping'.")
        #     return None

        # if len(relationships) > 1:
        #     raise CogReaderError(
        #         f"Expected only one incremental mapping. Found {len(relationships)} relationships:"
        #         f" {[rel.external_id for rel in relationships]}"
        #     )
        # sequence_external_id = relationships[0].target_external_id
        # # shop_type = "incremental_mapping"  # TODO: assert?
        # sequence_rows = retrieve_sequence_rows_as_dicts(
        #     client=self.client, external_id=sequence_external_id
        # )
        # return [TimeSeriesMapping.from_dict(row) for row in sequence_rows]

    def get_extra_files_metadata(self) -> List[dict[str, str]]:
        files = self.client.files.list(
            external_id_prefix=self.file_external_id_prefix,
            metadata=ShopMetadata(type="extra_data"),
        )
        if files:
            return [self.file_metadata_to_dict(fmd) for fmd in files]
        else:
            return []

    @staticmethod
    def file_metadata_to_dict(file_metadata):
        md = {"id": file_metadata.id}
        if (xid := file_metadata.external_id) is not None:
            md["external_id"] = xid
        if (n := file_metadata.name) is not None:
            md["name"] = n
        if (dsid := file_metadata.data_set_id) is not None:
            md["data_set_id"] = dsid
        return md

    @cached_property
    def extra_file_paths(self) -> Optional[List[str]]:
        logger.debug("Looking for 'extra' file.")
        files = self.client.files.list(
            external_id_prefix=self.file_external_id_prefix,
            metadata=ShopMetadata(type="extra_data"),
        )
        if not files:
            logger.debug(f"No 'extra' file found with and prefix '{self.file_external_id_prefix}'.")
            return None
        logger.debug(f"Found {len(files)} 'extra' files: {[file.external_id for file in files]}.")
        return [download_file(client=self.client, file=file, download_directory=self._tmp_dir.name) for file in files]

    def get_mapping_files_metadata(self):
        files = self.client.files.list(
            external_id_prefix=self.file_external_id_prefix,
            metadata=ShopMetadata(type="water_value_cut_file_reservoir_mapping"),
        )
        if files:
            return [self.file_metadata_to_dict(fmd) for fmd in files]
        else:
            return []

    @cached_property
    def mapping_file_paths(self) -> Optional[List[str]]:
        logger.debug("Looking for 'reservoir mapping' file.")
        files = self.client.files.list(
            external_id_prefix=self.file_external_id_prefix,
            metadata=ShopMetadata(type="water_value_cut_file_reservoir_mapping"),
        )
        if not files:
            logger.debug(f"No 'reservoir mapping' file found with and prefix '{self.file_external_id_prefix}'.")
            return None
        logger.debug(f"Found {len(files)} 'reservoir mapping' files: {[file.external_id for file in files]}.")
        return [download_file(client=self.client, file=file, download_directory=self._tmp_dir.name) for file in files]

    def get_cut_file_metadata(self):
        files = self.client.files.list(
            external_id_prefix=self.file_external_id_prefix,
            metadata=ShopMetadata(type="water_value_cut_file", watercourse=self.cog_shop_config.watercourse),
            limit=None,
        )
        if not files:
            logger.debug(f"No 'cut' file found with prefix '{self.file_external_id_prefix}'.")
            return None

        # Select file that is closest in time before starttime
        best_diff = float("inf")
        closest_file = None
        for this_file in files:
            try:
                # Assume datetime string after last "_"
                updated_at = this_file.metadata.get("update_datetime", this_file.external_id.split("_")[-1])
                updated_at_ms = arrow_to_ms(arrow.get(updated_at))  # parse ISO 8601 compliant string
                this_diff = self.cog_shop_config.starttime_ms - updated_at_ms

                if this_diff >= 0 and this_diff < best_diff:
                    logger.debug(f"Cutfile {this_file.external_id} is closer to starttime.")
                    best_diff = this_diff
                    closest_file = this_file

            except arrow.parser.ParserError as e:
                logger.warning(f"Failed to parse '{this_file.external_id}': {e}")
        if not closest_file:
            if len(files) > 0:
                logger.warning(
                    f"Could not find a cut file with a valid datetime - using {files[0].external_id}"
                    f" (which may be outdated)"
                )
                return self.file_metadata_to_dict(files[0])
            raise CogReaderError("Could not find any cut file!")

        return self.file_metadata_to_dict(closest_file)

    @cached_property
    def cut_file_path(self) -> Optional[str]:
        # TODO: logic for selecting cut file might need to be revised
        #   if getting additonal metadata about which date/time period the file is valid for.
        logger.debug("Looking for 'cut' file.")
        files = self.client.files.list(
            external_id_prefix=self.file_external_id_prefix,
            metadata=ShopMetadata(type="water_value_cut_file", watercourse=self.cog_shop_config.watercourse),
            limit=None,
        )
        if not files:
            logger.debug(f"No 'cut' file found with prefix '{self.file_external_id_prefix}'.")
            return None

        # Select file that is closest in time before starttime
        best_diff = float("inf")
        closest_file = None
        for this_file in files:
            try:
                # Assume datetime string after last "_"
                updated_at = this_file.metadata.get("update_datetime", this_file.external_id.split("_")[-1])
                updated_at_ms = arrow_to_ms(arrow.get(updated_at))  # parse ISO 8601 compliant string
                this_diff = self.cog_shop_config.starttime_ms - updated_at_ms

                if this_diff >= 0 and this_diff < best_diff:
                    logger.debug(f"Cutfile {this_file.external_id} is closer to starttime.")
                    best_diff = this_diff
                    closest_file = this_file

            except arrow.parser.ParserError as e:
                logger.warning(f"Failed to parse '{this_file.external_id}': {e}")
        if not closest_file:
            if len(files) > 0:
                logger.warning(
                    f"Could not find a cut file with a valid datetime - using {files[0].external_id}"
                    f" (which may be outdated)"
                )
                return download_file(client=self.client, file=files[0], download_directory=self._tmp_dir.name)
            raise CogReaderError("Could not find any cut file!")

        return download_file(client=self.client, file=closest_file, download_directory=self._tmp_dir.name)

    @cached_property
    def license_file_path(self) -> Optional[str]:
        """Read SHOP license file from CDF, store locally"""
        logger.debug("Looking for license file.")
        files = self.client.files.list(metadata=ShopMetadata(type="license"))
        if not files:
            logger.warning("Did not find any license file in CDF.")
            return None
        if len(files) > 1:
            logger.warning(f"Found more than one license file: {files}. Will use {files[0]}.")
        file_name = files[0].name
        if file_name != "SHOP_license.dat":
            raise CogReaderError(f"License file has to be named 'SHOP_license.dat'! Given file is named '{file_name}'.")
        download_file(client=self.client, file=files[0], download_directory=self._tmp_dir.name)
        return self._tmp_dir.name  # path to folder with license file instead of path to file

    @log_and_reraise(CogReaderError)
    def _replace_model_time_series(self) -> None:
        start = self.cog_shop_config.starttime_ms
        end = self.cog_shop_config.endtime_ms
        logger.debug("Applying 'base mapping'")
        self.cog_shop_case.replace_time_series(client=self.client, start=start, end=end, mappings=self.base_mapping)
        if self.incremental_mapping:
            logger.debug("Applying 'incremental mapping'")
            self.cog_shop_case.replace_time_series(
                client=self.client,
                start=start,
                end=end,
                mappings=self.incremental_mapping,
                dynamic_minute_offset=self.cog_shop_config.dynamic_minute_offset,
            )

    def run(self) -> CogReader:
        self.retrieve_case()

        # If specified in CogShopConfig (overrides previously set time specification)
        if self.cog_shop_config.timeresolution:
            self.cog_shop_case.set_time(
                starttime=self.cog_shop_config.starttime,
                endtime=self.cog_shop_config.endtime,
                timeresolution=self.cog_shop_config.timeresolution,
            )

        # Use defaults if time specification not already set
        if self.cog_shop_case.time is None:
            self.cog_shop_case.set_default_time(
                starttime=self.cog_shop_config.starttime, endtime=self.cog_shop_config.endtime
            )

        # TODO: consider if needed or commands should be required
        if self.cog_shop_case.commands is None:
            self.cog_shop_case._set_some_commands()

        self._replace_model_time_series()
        return self
