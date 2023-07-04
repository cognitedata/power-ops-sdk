from pathlib import Path

import pytest
import yaml

from cognite.powerops.client.data_classes import Case


@pytest.fixture
def case():
    return Case(
        """
        foo:
          bar:
           - baz1
           - baz2
          zzz: 42
        """
    )


def test_case_loading():
    case = Case("""foo: bar""")
    assert case.data == {"foo": "bar"}


def test_multipart_case_loading():
    case = Case(
        """
foo: bar
---
baz: zzz
"""
    )
    assert case.data == {"foo": "bar"}

    assert len(case.extra_files) == 1
    with open(case.extra_files[0]["file"]) as fh:
        extra = fh.read()
    assert extra == "baz: zzz\n"


def test_yaml(case):
    expected = """foo:
  bar:
  - baz1
  - baz2
  zzz: 42
"""
    assert expected == case.yaml


def test_save_yaml(case, tmp_path: Path):
    tmp_file = tmp_path / "test_save.yaml"
    case.save_yaml(tmp_file)
    value = tmp_file.read_text()
    assert yaml.safe_load(value) == case.data


def test_load_yaml(tmp_path: Path):
    with (tmp_path / "test_load.yaml").open("w") as fh:
        fh.write("foo:\n  bar")
        fh.flush()
        case = Case.from_yaml_file(fh.name)
    assert case.data == {"foo": "bar"}
