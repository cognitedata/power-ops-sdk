import importlib
import os
from unittest.mock import patch

import pytest

import cognite.powerops.utils.settings


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
def with_cognite_env_vars():
    vars = {
        # (case doesn't matter)
        "SETTINGS__COGNITE__PROJECT": "envproj",
        "SETTINGS__COGNITE__cdf_cluster": "env_clstr",
        "SETTINGS__COGNITE__tenant_id": "env_tnnt",
        "SETTINGS__cognite__client_id": "env_clnt",
        "SETTINGS__cognite__CLIENT_SECRET": "shhhhh!",
        "SETTINGS__POWEROPS__READ_DATASET": "read_from_this_dataset",
        "SETTINGS__POWEROPS__WRITE_DATASET": "write_to_this_dataset",
        "SETTINGS__POWEROPS__cogshop_version": "987",
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
