import json

import pytest
from cognite.client import CogniteClient
from cognite.client.data_classes import DataSet, ExtractionPipelineRun, FileMetadata
from cognite.client.testing import monkeypatch_cognite_client

from cognite.powerops.utils.cdf.extraction_pipelines import MSG_CHAR_LIMIT, ExtractionPipelineCreate, RunStatus


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
        truncate_keys_first=["plan"],
        log_file_prefix="resync_loss",
        description="The resync/plan function checks that the configuration files are matching "
        "the expected resources in CDF. If there are any differences, the run will report as failed",
        reverse_truncate_keys=["exception"],
    )


def test_create_pipeline_run_truncate_message(
    extraction_pipeline: ExtractionPipelineCreate, cognite_client: CogniteClient
):
    # Act
    with extraction_pipeline.create_pipeline_run(cognite_client) as run:
        run.update_data(status=RunStatus.FAILURE, plan="too large" * MSG_CHAR_LIMIT)
        message = run.get_message(dump_truncated_to_file=False)

    # Assert
    file_content = cognite_client.files.upload_bytes.call_args.kwargs["content"]
    assert "plan" in file_content
    assert json.loads(message)
    assert len(message) <= MSG_CHAR_LIMIT
    assert "..." in message


def test_create_pipeline_run_truncate_multiple_keys(
    extraction_pipeline: ExtractionPipelineCreate, cognite_client: CogniteClient
):
    # Act
    with extraction_pipeline.create_pipeline_run(cognite_client) as run:
        run.update_data(status=RunStatus.FAILURE, plan="too large" * MSG_CHAR_LIMIT, error="too large" * MSG_CHAR_LIMIT)
        message = run.get_message(dump_truncated_to_file=False)

    # Assert
    file_content = cognite_client.files.upload_bytes.call_args.kwargs["content"]
    assert "plan" in file_content
    assert "error" in file_content
    assert json.loads(message)
    assert len(message) <= MSG_CHAR_LIMIT
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
    assert '"exception":"...' in run.message
    assert len(run.message) <= MSG_CHAR_LIMIT


def test_pipeline_run_upload_file() -> None:
    # Arrange
    long_error_message = "Long error message" * 1000
    short_error_message = "Short error message"
    with monkeypatch_cognite_client() as client:
        client.files.upload_bytes.return_value = FileMetadata(id=1, external_id="test_file")
        pipeline = ExtractionPipelineCreate(
            external_id="test_pipeline",
            data_set_external_id="test_dataset",
            dump_truncated_to_file=True,
            message_keys_skip=["error"],
            truncate_keys_first=["error"],
            log_file_prefix="test",
        )

        # Act
        with pipeline.create_pipeline_run(client) as run:
            run.update_data(RunStatus.FAILURE, error=long_error_message, error_short=short_error_message)

        file_content = client.files.upload_bytes.call_args.kwargs["content"]
        message = client.extraction_pipelines.runs.create.call_args[0][0].message

    # Assert
    assert long_error_message in file_content
    assert short_error_message in file_content
    loaded_message = json.loads(message)
    assert "error" not in loaded_message


def test_pipeline_run_upload_file_nested_structure() -> None:
    # Arrange
    nested_structure = {"nested": "Long error message" * 1000}
    a_list = ["listItem", "longMessage" * 100]
    with monkeypatch_cognite_client() as client:
        client.files.upload_bytes.return_value = FileMetadata(id=1, external_id="test_file")
        pipeline = ExtractionPipelineCreate(
            external_id="test_pipeline",
            data_set_external_id="test_dataset",
            dump_truncated_to_file=True,
            truncate_keys_first=["output", "empty", "long"],
            log_file_prefix="test",
        )

        # Act
        with pipeline.create_pipeline_run(client) as run:
            run.update_data(RunStatus.FAILURE, output=nested_structure, empty=None, long=a_list)

        message = client.extraction_pipelines.runs.create.call_args[0][0].message

    # Assert
    assert len(message) < MSG_CHAR_LIMIT
    loaded_message = json.loads(message)

    assert "output" in loaded_message


def test_pipeline_minimum_input_files() -> None:
    with monkeypatch_cognite_client() as client:
        client.files.upload_bytes.return_value = FileMetadata(id=1, external_id="test_file")
        pipeline = ExtractionPipelineCreate(
            external_id="test_pipeline", data_set_external_id="test_dataset", truncate_keys_first=["logs"]
        )
        massive_logs = "massive_logs" * 1000
        # Act
        with pipeline.create_pipeline_run(client) as run:
            run.update_data(some_data="here is some data")

            run.update_data(RunStatus.SUCCESS, some_more="here is some more data", logs=massive_logs)

        file_content = client.files.upload_bytes.call_args.kwargs["content"]
        message = json.loads(client.extraction_pipelines.runs.create.call_args[0][0].message)

    # Assert
    assert "some_data" in message
    assert "some_more" in message
    assert "logs" in file_content
    assert message["some_more"] == "here is some more data"
