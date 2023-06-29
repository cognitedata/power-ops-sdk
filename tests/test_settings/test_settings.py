import importlib
import os
from unittest.mock import patch

import pytest

import cognite.powerops.settings


@pytest.fixture
def with_env_vars():
    vars = {
        "SETTINGS__COGNITE__PROJECT": "BAR",
        "SETTINGS__COGNITE__SPACE": "mockspace",
    }
    os.environ.update(vars)
    yield
    for var in vars:
        del os.environ[var]


@pytest.fixture
def no_files():
    with patch("cognite.powerops.settings.loader._load_file") as load_file_p:
        load_file_p.side_effect = FileNotFoundError()
        yield


@pytest.fixture
def with_files():
    with patch("cognite.powerops.settings.loader._load_file") as load_file_p:
        load_file_p.side_effect = [
            # settings.toml:
            {
                "cognite": {"project": "0", "space": "22"},
                "powerops": {"write_dataset": "333"},
            },
            # .secrets.toml:
            {
                "cognite": {"project": "1"},
            },
        ]
        yield


@pytest.fixture
def settings():
    """Re-import the settings module for each test."""
    yield importlib.reload(cognite.powerops.settings).settings


def test_settings_empty(no_files, settings):
    value = settings.dict()

    expected = {
        "cognite": {
            "cdf_cluster": None,
            "client_id": None,
            "client_secret": None,
            "data_model": None,
            "project": None,
            "schema_version": None,
            "space": None,
            "tenant_id": None,
        },
        "powerops": {
            "cogshop_version": None,
            "read_dataset": None,
            "write_dataset": None,
        },
    }
    assert value == expected


# In the remaining tests only check for specific values, to make adding new settings easier.


def test_settings_from_env(with_env_vars, no_files, settings):
    assert settings.cognite.project == "BAR"
    assert settings.cognite.space == "mockspace"
    assert settings.cognite.cdf_cluster is None


def test_settings_from_files(with_files, settings):
    assert settings.cognite.project == "1"
    assert settings.cognite.space == "22"
    assert settings.powerops.write_dataset == "333"
    assert settings.cognite.cdf_cluster is None


def test_settings_merge(with_env_vars, with_files, settings):
    assert settings.cognite.project == "BAR"  # env overrides files
    assert settings.cognite.space == "mockspace"  # env overrides files
    assert settings.powerops.write_dataset == "333"
    assert settings.cognite.cdf_cluster is None
