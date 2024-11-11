from __future__ import annotations

import abc
import logging
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Generic, TypeVar, Union

import yaml
from cognite.client.data_classes import FileMetadata

from cognite.powerops.utils.helpers import get_dict_dot_keys, is_time_series_dict

logger = logging.getLogger(__name__)


FileContentTypeT = TypeVar("FileContentTypeT", bound=Union[str, dict])


class SHOPResultFile(abc.ABC, Generic[FileContentTypeT]):
    """Base class for handling a results file from SHOP."""

    def __init__(
        self,
        content: FileContentTypeT,
        file_metadata: FileMetadata = None,
        encoding="utf-8",
    ) -> None:
        self._file_metadata = file_metadata
        self._encoding = encoding
        super().__init__()
        self.data: FileContentTypeT = content

    @property
    def encoding(self) -> str:
        """
        Encoding of the file.

        Returns:
            Encoding of the file.
        """
        return self._encoding

    @property
    def external_id(self):
        """
        External ID of the file.

        Returns:
            External ID of the file.
        """
        return self._file_metadata.external_id

    @property
    def name(self):
        """
        Name of the file.

        Returns:
            Name of the file.
        """

        return self._file_metadata.name

    def save_to_disk(self, dir_path: str = "") -> Path:
        """Save file to local filesystem."""
        file_path = (Path(dir_path) or Path.cwd()) / self._file_metadata.external_id
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        with file_path.open("w", encoding=self.encoding) as out_file:
            out_file.write(self.file_content)
        return file_path

    @property
    def file_content(self) -> str:
        """Data encoded for writing to local filesystem."""
        raise NotImplementedError()


class SHOPLogFile(SHOPResultFile[str]):
    """
    Plain text result file (for SHOP messages and CPlex logs).
    """

    @property
    def file_content(self) -> str:
        """
        Data encoded for writing to local filesystem.
        """
        return self.data


class SHOPYamlFile(SHOPResultFile[dict]):
    """
    Yaml-formatted results file (for post_run.yaml file created by SHOP).
    """

    @property
    def file_content(self) -> str:
        """
        Data encoded for writing to local filesystem.
        """
        return yaml.safe_dump(self.data, sort_keys=False)

    def _retrieve_time_series_dict(self, key: str) -> dict[datetime, float]:
        # key is a dot separated string of nested keys
        try:
            data = get_dict_dot_keys(self.data, key)
            if not is_time_series_dict(data):
                raise ValueError
            return data
        except KeyError:
            logger.error(f'Key "{key}" not found in {self.name}')
        except ValueError:
            logger.error(f'Data at with key "{key}" cannot be plotted as a time series')
        return {}

    def _prepare_plot_time_series(self, keys: Union[str, Sequence[str]]) -> dict:
        if isinstance(keys, str):
            keys = [keys]
        return {key: self._retrieve_time_series_dict(key) for key in keys}

    def _case_insensitive_filter_out(self, str_list: list[str], to_match: Union[str, int]) -> bool:
        """Some keys are parsed as numbers by the yaml parser"""
        return all(str(to_match).lower() != str_in_list.lower() for str_in_list in str_list)

    def find_time_series(
        self,
        matches_object_types: Sequence[str] | str = "",
        matches_object_names: Sequence[str] | str = "",
        matches_attribute_names: Sequence[str] | str = "",
    ) -> list[str]:
        """
        Find time series in the results file.

        Args:
            matches_object_types: Object types to match.
            matches_object_names: Object names to match.
            matches_attribute_names: Attribute names to match.

        Returns:
            List of dot separated strings of nested keys to the time series data.
        """
        if matches_object_types and isinstance(matches_object_types, str):
            matches_object_types = [matches_object_types]

        if matches_object_names and isinstance(matches_object_names, str):
            matches_object_names = [matches_object_names]

        if matches_attribute_names and isinstance(matches_attribute_names, str):
            matches_attribute_names = [matches_attribute_names]

        keys = []
        model = self.data["model"]
        for key1 in model:
            if matches_object_types and self._case_insensitive_filter_out(matches_object_types, key1):
                continue
            object_type: dict[str, dict] = model[key1]
            for key2, object_name in object_type.items():
                if matches_object_names and self._case_insensitive_filter_out(matches_object_names, key2):
                    continue
                for key3 in object_name:
                    if matches_attribute_names and self._case_insensitive_filter_out(matches_attribute_names, key3):
                        continue
                    attribute = object_name[key3]
                    if isinstance(attribute, dict) and all(isinstance(x, datetime) for x in attribute.keys()):
                        keys.append(f"model.{key1}.{key2}.{key3}")
        return keys
