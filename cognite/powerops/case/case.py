from __future__ import annotations

import logging
import tempfile
from typing import List, Optional, TypedDict

import yaml

logger = logging.getLogger(__name__)


class FileRefT(TypedDict):
    file: str
    encoding: str


class Case:
    r"""
    Wrapper around YAML file for SHOP, describing a case.

    Main features:
     * Values can be retrieved or set / changed like with a dict.
     * Can hold references to additional files ("extra files").
        * These are just paths to files, their content is not accessed here.
     * Loading from string or file (with `Case.load_yaml(path_to_yaml)`).
     * Saving to yaml (with `case.save_yaml(path_to_file)`).

    Examples:
      * load a case from string
          >>> case = Case('''
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
          case = Case.load_yaml("path/to/my_case.yaml")
          case[...] = ...  # edit data
          case.save_yaml("path/to/same_or_different.yaml")
    """

    def __init__(self, data: str = "") -> None:
        self._cut_file: Optional[FileRefT] = None
        self._extra_files: list[FileRefT] = []
        self._mapping_files: list[FileRefT] = []

        yaml_docs = list(yaml.safe_load_all(data))
        if yaml_docs:
            self.data = yaml_docs[0]
            self._handle_additional_yaml_documents(yaml_docs[1:])

    def _handle_additional_yaml_documents(self, extra_yaml_docs: List[str]) -> None:
        """
        This probably happens never, but just in case...
        If `Case.__init__` gets a yaml string which has multiple documents (separated by "---"),
        only the first document is parsed and set to `self.data`. Any subsequent documents are stored
        as "extra files". They are not part of `self.data`, but are not lost either.
        """
        for yaml_doc in extra_yaml_docs:
            tmp_file = tempfile.NamedTemporaryFile(
                mode="w",
                delete=False,
                prefix="powerops-sdk-tmp-",
                suffix=".yaml",
            )
            tmp_file.write(yaml.dump(yaml_doc))
            tmp_file.close()
            self.add_extra_file(tmp_file.name)

    @classmethod
    def load_yaml(cls, yaml_path: str, encoding: str = "utf-8") -> Case:
        logger.info(f"loading case file: {yaml_path}")
        with open(yaml_path, "r", encoding=encoding) as yaml_file:
            return cls(yaml_file.read())

    def add_extra_file(self, file_path: str, encoding: str = "utf-8") -> None:
        logger.info(f"adding extra file: '{file_path}'")
        self._extra_files.append({"file": file_path, "encoding": encoding})

    def add_mapping_file(self, file_path: str, encoding: str = "utf-8") -> None:
        logger.info(f"adding mapping file: '{file_path}'")
        self._mapping_files.append({"file": file_path, "encoding": encoding})

    def add_cut_file(self, file_path: str, encoding: str = "utf-8") -> None:
        logger.info(f"adding cut file: '{file_path}'")
        self._cut_file = {"file": file_path, "encoding": encoding}

    @property
    def extra_files(self) -> list[FileRefT]:
        return self._extra_files

    @property
    def mapping_files(self) -> list[FileRefT]:
        return self._mapping_files

    @property
    def cut_file(self) -> FileRefT:
        return self._cut_file

    @property
    def yaml(self) -> str:
        return yaml.dump(self.data, sort_keys=False)

    def save_yaml(self, path: str, encoding: str = "utf-8") -> None:
        logger.info(f"Saving case file to: {path}")
        with open(path, "w", encoding=encoding) as output_file:
            output_file.write(yaml.dump(self.data, sort_keys=False))
