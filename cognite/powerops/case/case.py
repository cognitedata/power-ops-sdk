from __future__ import annotations

import logging
import tempfile
from typing import Any

import yaml

logger = logging.getLogger(__name__)


class Case:
    r"""
    Wrapper around YAML file for SHOP, describing a case.

    Main features:
     * Values can be retrieved or set / changed like with a dict.
        * Dot syntax is also supported.
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
          >>> case["foo"]["bar2"] = 202
          >>> case.yaml
          'foo:\n  bar1: 11\n  bar2: 202\n'

      * dot-notation, just for convenience
          >>> case["foo.bar1"] = 101
          >>> case["foo.bar1"]
          101

      * load from and save to a file:
          case = Case.load_yaml("path/to/my_case.yaml")
          case[...] = ...  # edit data
          case.save_yaml("path/to/same_or_different.yaml")
    """

    def __init__(self, data: str = "") -> None:
        self._data = {}
        self._extra_files = []

        yaml_docs = list(yaml.safe_load_all(data))
        if yaml_docs:
            self._data = yaml_docs[0]
            for yaml_doc in yaml_docs[1:]:
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
    def load_yaml(cls, yaml_path: str) -> Case:
        logger.info(f"loading case file: {yaml_path}")
        with open(yaml_path, "r") as yaml_file:
            return cls(yaml_file.read())

    def add_extra_file(self, file_path: str) -> None:
        logger.info(f"adding extra file: {file_path}")
        self._extra_files.append(file_path)

    @property
    def extra_files(self):
        return self._extra_files.copy()

    @property
    def data(self) -> dict:
        return self._data.copy()

    def __getitem__(self, path: str) -> Any:
        """Get item from `self._data` dict, supporting "." as separator"""
        if path in self._data:  # edge case: key actually contains a dot
            return self._data[path]
        part_data = self.data
        for part in path.split("."):
            if isinstance(part_data, list):
                part = int(part)
            part_data = part_data[part]
        return part_data

    def __setitem__(self, path: str, value: Any) -> None:
        """Set item from `self._data` dict, supporting "." as separator"""
        if path in self._data:  # edge case: key actually contains a dot
            self._data[path] = value
        else:
            part_data = self.data
            parts = path.split(".")
            while parts:
                part = parts.pop(0)
                if isinstance(part_data, list):
                    part = int(part)
                if parts:
                    part_data = part_data[part]
                else:
                    # last loop:
                    part_data[part] = value

    @property
    def yaml(self) -> str:
        return yaml.dump(self._data, sort_keys=False)

    def save_yaml(self, path: str) -> None:
        logger.info(f"Saving case file to: {path}")
        with open(path, "w") as output_file:
            output_file.write(yaml.dump(self._data, sort_keys=False))
