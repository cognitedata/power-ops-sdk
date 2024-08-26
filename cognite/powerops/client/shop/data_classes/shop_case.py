from __future__ import annotations

import logging
from collections.abc import Sequence as SequenceType
from pathlib import Path

import yaml

from cognite.powerops.client.shop.data_classes.shop_file_reference import SHOPFileReference, SHOPFileType

logger = logging.getLogger(__name__)


class SHOPCase:
    r"""
    Wrapper around YAML file for SHOP, describing a case.

    Main features:
     * Values can be retrieved or set / changed like with a dict.
     * Data will be lazily loaded to a dict, only where the data is accessed; otherwise, it is kept as a string.
     * Can hold references to additional files ("extra files").
        * These are just externalID values of files in CDF, their content is not accessed here.
     * Loading from string or file (with `SHOPCase(filepath=path_to_yaml)`).
     * Saving to yaml (with `case.save_yaml(path_to_file)`).

    Examples:
      * load a case from string
          >>> case = SHOPCase('''
          ... foo:
          ...   bar1: 11
          ...   bar2: 22
          ... ''')
          >>> case.data
          {'foo': {'bar1': 11, 'bar2': 22}}

      * edit a value and show updated data
          >>> case.data["foo"]["bar2"] = 202
          >>> case.yaml
          'foo:\n  bar1: 11\n  bar2: 202\n'

      * load from and save to a file:
          case = SHOPCase(file_path="path/to/my_case.yaml")
          case[...] = ...  # edit data
          case.save_yaml("path/to/same_or_different.yaml")
    """

    def __init__(
        self,
        data: str = "",
        *,
        file_path: str = "",
        shop_files: SequenceType[SHOPFileReference] = (),
        watercourse: str = "",
        shop_version: str = "",
    ) -> None:
        if data and file_path:
            raise ValueError("Cannot specify both data and file_path")

        if data:
            self._case_string = data.lstrip("\n")
        else:
            self._case_string = SHOPCase.load_case_file(file_path)

        self._data = {}
        self.excess_yaml_parts: list[str] = []

        self._shop_files: list[SHOPFileReference] = list(shop_files)
        self.watercourse = watercourse
        self.shop_version = shop_version

    def load_case_data(self) -> None:
        self.excess_yaml_parts = []
        try:
            if yaml_docs := list(yaml.safe_load_all(self._case_string)):
                self._data = yaml_docs[0]
                self._handle_excess_yaml_parts(yaml_docs[1:])
        except yaml.YAMLError as e:
            raise ValueError("Could not parse case data") from e

    @staticmethod
    def load_case_file(file_path: str, encoding: str = "utf-8") -> str:
        with Path(file_path).open(encoding=encoding) as file:
            return file.read()

    @property
    def data(self) -> dict:
        if not self._data:
            self.load_case_data()

        return self._data

    @property
    def shop_files(self) -> list[SHOPFileReference]:
        return self._shop_files.copy()

    def add_shop_file(self, external_id: str, shop_file_type: SHOPFileType = SHOPFileType.ASCII):
        logger.info(f"adding shop file from CDF: {external_id!r}, {shop_file_type=!r}")
        shop_file_reference = SHOPFileReference(external_id=external_id, file_type=shop_file_type)
        self.shop_files.append(shop_file_reference)

    @property
    def yaml(self) -> str:
        if not self._data:
            return self._case_string

        case_data = yaml.dump(self.data, sort_keys=False)
        if not case_data.endswith("\n"):
            case_data += "\n"
        return "---\n".join([case_data, *self.excess_yaml_parts])

    def save_yaml(self, path: str, encoding: str = "utf-8") -> None:
        logger.info(f"Saving case file to: {path}")
        with Path(path).open("w", encoding=encoding) as output_file:
            output_file.write(self.yaml)

    def _handle_excess_yaml_parts(self, excess_yaml_parts: list[str]) -> None:
        """
        If `Case.__init__` gets a yaml string which has multiple documents (separated by "---"),
        only the first document is parsed and set to `self.data`. Any subsequent documents are stored
        as "excess_yaml_parts". They are not part of `self.data`, but are kept and appended to the output.
        """
        if excess_yaml_parts:
            logger.warning(
                f"Case file contains {len(excess_yaml_parts) + 1} YAML documents. Only the first document is parsed,"
                f' additional documents will be passed to SHOP verbatim as "extra files".'
            )
        for yaml_doc in excess_yaml_parts:
            self.excess_yaml_parts.append(yaml.dump(yaml_doc, sort_keys=False))
