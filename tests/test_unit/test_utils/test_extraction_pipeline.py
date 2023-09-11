import json

from cognite.client.data_classes import FileMetadata
from cognite.client.testing import monkeypatch_cognite_client

from cognite.powerops.utils.cdf.extraction_pipelines import ExtractionPipelineCreate, RunStatus


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
    assert "error" not in json.loads(message)
