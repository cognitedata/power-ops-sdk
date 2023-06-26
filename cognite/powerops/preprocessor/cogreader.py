from __future__ import annotations

import tempfile
from functools import cached_property
from pathlib import Path
from typing import List, Optional, Union, Literal
from pydantic import BaseModel, validator

from cognite.client import CogniteClient

from cognite.powerops.preprocessor import knockoff_logging as logging
from cognite.powerops.preprocessor.data_classes import CogShopCase, CogShopConfig
from cognite.powerops.preprocessor.data_classes.time_series_mapping import TimeSeriesMapping
from cognite.powerops.preprocessor.exceptions import CogReaderError
from cognite.powerops.preprocessor.utils import ShopMetadata, download_file, log_and_reraise, retrieve_yaml_file

from .get_fdm_data import Case, get_case
from .utils import find_closest_file, group_files_by_metadata, load_yaml

logger = logging.getLogger(__name__)



class CogShopFile(BaseModel):
    label: Optional[str]
    pick: Optional[Literal["closest", "latest", "all"]]
    sort_by: Optional[str]
    external_id_prefix: Optional[str]
    external_id: str = ""
    file_type: Literal["ascii", "yaml"]

    class Config:
        validate_all = True

    @validator("external_id")
    def external_id_or_prefix_set(cls, value, values: dict):
        if not values.get("external_id_prefix"):
            print(value, values.get(""))
            if value == "":
                raise ValueError("Either external_id or external_id_prefix must be set")


class CogShopFileLoader(BaseModel):
    file_load_sequence: list[CogShopFile]

    @classmethod
    def from_yaml(cls, path: Path) -> "CogShopFileLoader":
        configs = load_yaml(path, encoding="utf-8")
        return cls(**configs)



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
        self.cog_shop_file_loader: None | CogShopFileLoader = None
        logger.info(f"{self.__class__.__name__} initialized.")

    @property
    def file_external_id_prefix(self) -> str:
        return f"SHOP_{self.cog_shop_config.watercourse}_"

    @property
    def cog_shop_files_config_path(self) -> Path:
        return Path(f"{self._tmp_dir}/cog_shop_files_config.yaml")

    @property
    def cog_shop_file_loader(self) -> CogShopFileLoader:
        self.client.files.download_to_path(self.cog_shop_files_config_path,
                                           external_id=f"SHOP_{self.cog_shop_config.watercourse}_cog_shop_files_config")
        return CogShopFileLoader.from_yaml(self.cog_shop_files_config_path)

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

    def get_extra_files_metadata(self) -> List[dict[str, Union[str, int]]]:
        files = self.client.files.list(
            external_id_prefix=self.file_external_id_prefix,
            metadata=ShopMetadata(type="extra_data"),
        )
        if files:
            return [self.file_metadata_to_dict(fmd) for fmd in files]
        else:
            return []

    def get_cog_shop_files(self) -> list[dict]:
        file_loader_sequence = []

        for file in self.cog_shop_file_loader.file_load_sequence:
            if file.external_id and file.pick == "latest":
                file_loader_sequence.append({"external_id": file.external_id,
                                               "file_type": file.file_type})
            elif file.external_id_prefix and file.pick == "closest":
                files = self.client.files.list(external_id_prefix=file.external_id_prefix, limit=None)
                closest_file = find_closest_file(files, self.cog_shop_config.starttime_ms)
                file_loader_sequence.append({"external_id": closest_file.external_id,
                                               "file_type": file.file_type})

        if not file_loader_sequence:
            raise ValueError("No file loader sequence obtained from CDF")
        return file_loader_sequence

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

    def get_mapping_files_metadata(self):
        files = self.client.files.list(
            external_id_prefix=self.file_external_id_prefix,
            metadata=ShopMetadata(type="water_value_cut_file_reservoir_mapping"),
        )
        if files:
            return [self.file_metadata_to_dict(fmd) for fmd in files]
        else:
            return []

    def get_cut_files_metadata(self) -> list[dict]:
        files = self.client.files.list(
            external_id_prefix=self.file_external_id_prefix,
            metadata={
                "shop:type": "water_value_cut_file",
                "shop:watercourse": self.cog_shop_config.watercourse,
            },
            limit=None,
        )
        if not files:
            return []
        return [
            self.file_metadata_to_dict(f)
            for group in group_files_by_metadata(files).values()
            if (f := find_closest_file(group, self.cog_shop_config.starttime_ms) is not None)
        ]

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

        if not self.cogshop_file_loader:
            self.set_cogshop_file_loader()

        return self

    @cog_shop_file_loader.setter
    def cog_shop_file_loader(self, value):
        self._cog_shop_file_loader = value
