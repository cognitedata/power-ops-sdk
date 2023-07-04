import os
from pathlib import Path

import pytest
import tomli_w

from cognite.powerops.utils.cdf import Settings


def env_vars_to_vars(env_vars: dict[str, str]) -> dict[str, str]:
    output = {}
    for k, v in env_vars.items():
        parts = k.removeprefix(Settings.Config.env_prefix).split(Settings.Config.env_nested_delimiter)
        level = output
        for key in parts[:-1]:
            level[key.lower()] = level.get(key.lower(), {})
            level = level[key.lower()]
        level[parts[-1].lower()] = v

    return output


@pytest.fixture
def setting_environmental_vars():
    setting_vars = {
        "SETTINGS__COGNITE__PROJECT": "BAR",
        "SETTINGS__COGNITE__CLIENT_ID": "mockclient",
        "SETTINGS__COGNITE__LOGIN_FLOW": "interactive",
        "SETTINGS__COGNITE__CDF_CLUSTER": "mockfield",
        "SETTINGS__COGNITE__TENANT_ID": "mocktenant",
        "SETTINGS__POWEROPS__READ_DATASET": "read_from_this_dataset",
        "SETTINGS__POWEROPS__WRITE_DATASET": "write_to_this_dataset",
        "SETTINGS__POWEROPS__cogshop_version": "987",
    }
    os.environ.update(setting_vars)
    yield setting_vars
    for var in setting_vars:
        del os.environ[var]


@pytest.fixture
def settings_files(tmp_path: Path):
    settings_file = tmp_path / "settings.toml"
    secrets_file = tmp_path / ".secrets.toml"
    file_contents = {
        "cognite": {
            "project": "0",
            "client_id": "22",
            "client_secret": "super-secret",
            "tenant_id": "44",
            "cdf_cluster": "55",
            "login_flow": "client_credentials",
        },
        "powerops": {"write_dataset": "333", "read_dataset": "444", "cogshop_version": "555"},
    }
    settings_file.write_text(
        tomli_w.dumps(
            {
                "cognite": {k: v for k, v in file_contents["cognite"].items() if k != "client_secret"},
                "powerops": file_contents["powerops"],
            }
        )
    )
    secrets_file.write_text(
        tomli_w.dumps(
            {
                "cognite": {"client_secret": file_contents["cognite"]["client_secret"]},
            }
        )
    )
    os.environ["SETTINGS_FILES"] = ";".join([str(settings_file), str(secrets_file)])
    yield file_contents


def test_settings_from_env(setting_environmental_vars):
    # Arrange
    expected = env_vars_to_vars(setting_environmental_vars)

    # Act
    settings = Settings()

    # Assert
    assert expected == settings.dict(exclude_unset=True)


def test_settings_from_files(settings_files: dict[str, dict[str, str]]):
    # Act
    actual = Settings()

    # Assert
    assert settings_files == actual.dict(exclude_unset=True)


def test_settings_overwrite(setting_environmental_vars):
    # Arrange
    from_env = Settings()

    # Act
    with_overwrite = Settings(**{"cognite": {"project": "mySuperProject"}})

    # Assert
    assert from_env.cognite.project != with_overwrite.cognite.project
