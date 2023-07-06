from __future__ import annotations

import tempfile
from functools import cached_property
from typing import List, Literal, Optional, TypedDict, Union

import arrow
from cognite.client import CogniteClient
from cognite.client.data_classes import FileMetadata
from pydantic import BaseModel, model_validator

from cognite.powerops.preprocessor import knockoff_logging as logging
from cognite.powerops.preprocessor.data_classes import CogShopCase, CogShopConfig
from cognite.powerops.preprocessor.data_classes.time_series_mapping import TimeSeriesMapping
from cognite.powerops.preprocessor.exceptions import CogReaderError
from cognite.powerops.preprocessor.utils import (
    ShopMetadata,
    arrow_to_ms,
    download_file,
    log_and_reraise,
    now,
    retrieve_yaml_file,
)

from .get_fdm_data import Case, get_case

logger = logging.getLogger(__name__)


class CogShopFileDict(TypedDict):
    external_id: str
    file_type: str


class SortBy(BaseModel):
    metadata_key: Optional[str] = None
    file_attribute: Optional[Literal["last_updated_time", "created_time", "uploaded_time"]] = None

    @model_validator(mode="after")
    def has_valid_sorting_method(cls, value: SortBy):
        has_metadata_key = value.metadata_key is not None
        has_file_attribute = value.file_attribute is not None
        if has_metadata_key and has_file_attribute:
            raise ValueError("Can only sort on ether metadata_key or file_attribute, got both.")
        if not has_metadata_key and not has_file_attribute:
            raise ValueError("Missing a sorting key, please set either metadata_key or file_attribute.")
        return value


class CogShopFile(BaseModel):
    label: Optional[str] = None
    pick: Optional[Literal["latest_before", "exact", "latest"]] = None
    sort_by: Optional[SortBy] = None
    external_id_prefix: Optional[str] = None
    external_id: Optional[str] = None
    file_type: Literal["ascii", "yaml"]

    class Config:
        validate_all = True

    @model_validator(mode="after")
    def external_id_or_prefix_set(cls, value: CogShopFile):
        if not value.external_id and not value.external_id_prefix:
            raise ValueError("Either external_id or external_id_prefix must be set")
        if not value.pick and not value.external_id:
            raise ValueError("File selection method by 'pick' must be set if not external id has been specified.")
        return value

    def _find_file_latest_before(self, files: list[FileMetadata], starttime_ms: float) -> Optional[FileMetadata]:
        # Select file that is closest in time before starttime
        valid_files = []

        best_diff = float("inf")
        closest_file = None

        for this_file in files:
            try:
                if self.sort_by:
                    if key := self.sort_by.metadata_key:
                        if updated_at := this_file.metadata.get(key):
                            updated_at_ms = arrow_to_ms(arrow.get(updated_at))
                        else:
                            logger.warning(
                                f"Provided metadata field: {key} not in file metadata for {this_file.external_id} - "
                                f"skipping file"
                            )
                            continue
                    elif attr := self.sort_by.file_attribute:
                        updated_at_ms = getattr(this_file, attr)
                else:
                    logger.debug("No file selection method chosen. Will use last_updated_time attribute for file.")
                    updated_at_ms = getattr(this_file, "last_updated_time")

                # Assume datetime string after last "_"
                this_diff = starttime_ms - updated_at_ms
                if this_diff >= 0:
                    valid_files.append(this_file)
                    if this_diff < best_diff:
                        logger.debug(f"Cutfile {this_file.external_id} is closer to starttime.")
                        best_diff = this_diff
                        closest_file = this_file

            except (arrow.parser.ParserError, TypeError) as e:
                logger.warning(f"Failed to parse date for '{this_file.external_id}': {e}")

        if not closest_file and valid_files:
            logger.warning(f"Could not find a cut file with a valid datetime - using {valid_files[0].external_id}")
            return valid_files[0]
        elif not valid_files:
            raise CogReaderError(
                f"No files valid found in CDF before start time {starttime_ms} for file {self.external_id_prefix}. "
                f"Please check existence of files before start time or check file selection method"
            )

        return closest_file

    def get_file_dict(self, client: CogniteClient, starttime_ms: float) -> Optional[CogShopFileDict]:
        if self.external_id:
            return {"external_id": self.external_id, "file_type": self.file_type}
        elif self.external_id_prefix and self.pick == "latest_before":
            files = client.files.list(external_id_prefix=self.external_id_prefix, limit=None)
            if closest_file := self._find_file_latest_before(files, starttime_ms):
                return {"external_id": closest_file.external_id, "file_type": self.file_type}
        elif self.external_id_prefix and self.pick == "latest":
            files = client.files.list(external_id_prefix=self.external_id_prefix, limit=None)
            if closest_file := self._find_file_latest_before(files, now()):
                return {"external_id": closest_file.external_id, "file_type": self.file_type}
        raise CogReaderError("Could not extract a valid file dict from config")


class CogShopFilesConfig(BaseModel):
    file_load_sequence: list[CogShopFile]

    @classmethod
    def from_cdf(cls, client: CogniteClient, file_ex_id: str) -> "CogShopFilesConfig":
        config = retrieve_yaml_file(client, file_ex_id)
        return cls(**config)

    def to_cog_shop_file_list(self, client: CogniteClient, starttime_ms: float) -> list[Optional[CogShopFileDict]]:
        return [file.get_file_dict(client, starttime_ms) for file in self.file_load_sequence]


class CogReader:
    """Read data from CDF and preprocess for SHOP"""

    def __init__(
        self,
        client: CogniteClient,
        fdm_space_external_id: str,
        fdm_case_external_id: str,
        fdm_model_external_id: Optional[str] = None,
        fdm_model_version: Optional[int] = None,
    ) -> None:
        self.client: CogniteClient = client

        self.fdm_space_external_id = fdm_space_external_id
        self.fdm_case_external_id = fdm_case_external_id
        self.fdm_model_external_id = fdm_model_external_id
        self.fdm_model_version = fdm_model_version

        self._tmp_dir = tempfile.TemporaryDirectory()
        self.cog_shop_files_config: CogShopFilesConfig
        logger.info(f"{self.__class__.__name__} initialized.")

    @property
    def file_external_id_prefix(self) -> str:
        return f"SHOP_{self.cog_shop_config.watercourse}_"

    def _set_cog_shop_files_config(self) -> None:
        if not (
            file := self.client.files.retrieve(
                external_id=f"SHOP_{self.cog_shop_config.watercourse}_cog_shop_files_config"
            )
        ):
            raise CogReaderError(
                f"No cog_shop_files_config for watercourse {self.cog_shop_config.watercourse} in CDF. "
                f"Ensure that file exists"
            )
        logger.debug("Attempting to download cog shop files config from CDF.")
        self.cog_shop_files_config = CogShopFilesConfig.from_cdf(self.client, file.external_id)
        logger.info("Cog Shop file list config successfully initialised")

    @property
    def fdm_case(self) -> Case:
        try:
            return self._fdm_case
        except AttributeError:
            raise RuntimeError("Expected to run `retrieve_case()` method first.") from None

    def retrieve_case(self) -> None:
        self._fdm_case = get_case(
            self.client,
            self.fdm_space_external_id,
            self.fdm_case_external_id,
            model_extenal_id=self.fdm_model_external_id,
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
    def incremental_mapping(self) -> List[TimeSeriesMapping]:
        if (mo := self.fdm_case.scenario.mappings_override) is not None:
            return [TimeSeriesMapping.from_mapping_model(m) for m in mo.items]
        return []

    def get_cog_shop_file_list(self) -> list[Optional[CogShopFileDict]]:
        return self.cog_shop_files_config.to_cog_shop_file_list(self.client, self.cog_shop_config.starttime_ms)

    @staticmethod
    def file_metadata_to_dict(file_metadata) -> dict[str, Union[str, int]]:
        md = {"id": file_metadata.id}
        if (xid := file_metadata.external_id) is not None:
            md["external_id"] = xid
        if (n := file_metadata.name) is not None:
            md["name"] = n
        if (dsid := file_metadata.data_set_id) is not None:
            md["data_set_id"] = dsid
        return md

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

        self._set_cog_shop_files_config()

        return self
