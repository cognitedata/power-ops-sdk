import os
import shutil
import tempfile

import pytest
import yaml

from cognite.powerops.case import Case


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


@pytest.fixture
def tmp_dir():
    tmp_path = tempfile.mkdtemp(prefix="powerops-sdk-tmp-")
    yield tmp_path
    shutil.rmtree(tmp_path)


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


def test_save_yaml(case, tmp_dir):
    tmp_file = os.path.join(tmp_dir, "test_save.yaml")
    case.save_yaml(tmp_file)
    with open(tmp_file) as fh:
        value = fh.read()
    assert yaml.safe_load(value) == case.data


def test_load_yaml(tmp_dir):
    with tempfile.NamedTemporaryFile("w", dir=tmp_dir) as fh:
        fh.write("foo:\n  bar")
        fh.flush()
        case = Case.from_yaml_file(fh.name)
    assert case.data == {"foo": "bar"}
