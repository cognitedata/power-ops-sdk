from pathlib import Path

import pytest
import yaml

from cognite.powerops.client.shop.data_classes.shop_case import SHOPCase


@pytest.fixture
def case():
    return SHOPCase(
        """
foo:
  bar:
  - baz1
  - baz2
  zzz: 42
""".lstrip("\n")
    )


def test_case_loading():
    case = SHOPCase("""foo: bar""")
    assert case.data == {"foo": "bar"}


def test_multipart_case_loading():
    case = SHOPCase(
        """
foo: bar
---
baz: zzz
"""
    )
    assert case.data == {"foo": "bar"}

    assert len(case.excess_yaml_parts) == 1
    extra = case.excess_yaml_parts[0]
    assert extra == "baz: zzz\n"


def test_yaml(case):
    expected = """
foo:
  bar:
  - baz1
  - baz2
  zzz: 42
""".lstrip("\n")
    assert expected == case.yaml


def test_yaml_keep_excess_parts(case):
    case = SHOPCase(
        """
foo: bar
---
baz: zzz
"""
    )
    full_case = case.yaml
    assert full_case == "foo: bar\n---\nbaz: zzz\n"


def test_save_yaml(case, tmp_path: Path):
    tmp_file = tmp_path / "test_save.yaml"
    case.save_yaml(tmp_file)
    value = tmp_file.read_text()
    assert yaml.safe_load(value) == case.data


def test_load_yaml(tmp_path: Path):
    with (tmp_path / "test_load.yaml").open("w") as fh:
        fh.write("foo:\n  bar")
        fh.flush()
        case = SHOPCase(file_path=fh.name)
        assert case.data == {"foo": "bar"}
