import json
import os
from pathlib import Path

import pytest
import tomli_w
from cognite.client import CogniteClient
from cognite.client.data_classes import DataSet, FileMetadata, ExtractionPipelineRun
from cognite.client.testing import monkeypatch_cognite_client
from cognite.powerops.utils.cdf import Settings
from cognite.powerops.utils.cdf.extraction_pipelines import ExtractionPipelineCreate, RunStatus, MSG_CHAR_LIMIT


def env_vars_to_vars(env_vars: dict[str, str]) -> dict[str, str]:
    output = {}
    for k, v in env_vars.items():
        parts = k.removeprefix(Settings.model_config["env_prefix"]).split(Settings.model_config["env_nested_delimiter"])
        level = output
        for key in parts[:-1]:
            level[key.lower()] = level.get(key.lower(), {})
            level = level[key.lower()]
        level[parts[-1].lower()] = v

    return output


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
        "powerops": {"write_dataset": "333", "read_dataset": "444", "monitor_dataset": "123", "cogshop_version": "555"},
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

    # Actual
    settings = Settings()
    settings.cognite.client_secret = "super-secret"

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


@pytest.fixture()
def cognite_client() -> CogniteClient:
    with monkeypatch_cognite_client() as client:
        client.data_sets.retrieve.return_value = DataSet(id=1, external_id="unit test")
        client.files.upload_bytes.return_value = FileMetadata(id=123)
        yield client


@pytest.fixture()
def extraction_pipeline() -> ExtractionPipelineCreate:
    return ExtractionPipelineCreate(
        external_id="resync/plan",
        data_set_external_id="powerops",
        dump_truncated_to_file=True,
        truncate_keys=["plan"],
        log_file_prefix="resync_loss",
        description="The resync/plan function checks that the configuration files are matching "
        "the expected resources in CDF. If there are any differences, the run will report as failed",
    )


def test_create_pipeline_run_truncate_message(
    extraction_pipeline: ExtractionPipelineCreate, cognite_client: CogniteClient
):
    # Act
    with extraction_pipeline.create_pipeline_run(cognite_client) as run:
        run.update_data(
            status=RunStatus.FAILURE,
            plan="too large" * MSG_CHAR_LIMIT,
        )
        message = run.get_message(dump_truncated_to_file=False)

    # Assert
    file_content = cognite_client.files.upload_bytes.call_args.kwargs["content"]
    assert "plan" in file_content
    assert json.loads(message)
    assert len(message) <= MSG_CHAR_LIMIT
    assert "..." in message


def test_create_pipeline_run_truncate_multiple_keys(
    extraction_pipeline: ExtractionPipelineCreate,
    cognite_client: CogniteClient,
):
    # Act
    with extraction_pipeline.create_pipeline_run(cognite_client) as run:
        run.update_data(
            status=RunStatus.FAILURE,
            plan="too large" * MSG_CHAR_LIMIT,
            error="too large" * MSG_CHAR_LIMIT,
        )
        message = run.get_message(dump_truncated_to_file=False)

    # Assert
    file_content = cognite_client.files.upload_bytes.call_args.kwargs["content"]
    assert "plan" in file_content
    assert "error" in file_content
    assert json.loads(message)
    assert len(message) < MSG_CHAR_LIMIT
    assert "..." in message


def test_create_pipeline_run_raise_exception(
    extraction_pipeline: ExtractionPipelineCreate, cognite_client: CogniteClient
):
    # Act
    try:
        with extraction_pipeline.create_pipeline_run(cognite_client) as run:
            raise ValueError("Massive Error" * MSG_CHAR_LIMIT)
    except ValueError:
        ...

    # Assert
    file_content = cognite_client.files.upload_bytes.call_args.kwargs["content"]
    assert "exception" in file_content
    run: ExtractionPipelineRun
    run, *_ = cognite_client.extraction_pipelines.runs.create.call_args.args
    assert run.status == "failure"
    assert "..." in run.message
    assert len(run.message) <= MSG_CHAR_LIMIT
