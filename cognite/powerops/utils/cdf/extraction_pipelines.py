from __future__ import annotations
import json

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional, ClassVar, Callable
import traceback


from cognite.client._constants import MAX_VALID_INTERNAL_ID
from cognite.client import CogniteClient
from cognite.client.data_classes import ExtractionPipeline, ExtractionPipelineRun
from cognite.client.exceptions import CogniteAPIError

MSG_CHAR_LIMIT = 1000


class RunStatus(str, Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    SEEN = "seen"


@dataclass
class _Config:
    dump_truncated_to_file: bool = True
    truncate_keys: list[str] = None
    log_file_prefix: str | None = None


class PipelineRun:
    """
    A context manager to create an extraction pipeline run when exiting the context.
    Run status defaults to SEEN and FAILURE if an exception is raised. To set a custom
    status and message, use the `update_data` method.

    """

    init_status: ClassVar[RunStatus] = RunStatus.SEEN
    log_file_external_id: ClassVar[str] = "log_file_external_id"
    log_file_id: ClassVar[str] = "log_file_id"
    exception: ClassVar[str] = "exception"

    reserved_keys: ClassVar[tuple[str]] = [log_file_external_id, log_file_id, exception]

    def __init__(
        self,
        client: CogniteClient,
        pipeline_external_id: str,
        config: _Config,
        data_set_id: int,
        error_logger: Callable[[str], None],
    ) -> None:
        self.client = client
        self.pipeline_external_id = pipeline_external_id
        self.config = config
        self.data_set_id = data_set_id
        self.error_logger = error_logger

        self.data = {}
        self.status = self.init_status

    def __enter__(self) -> "PipelineRun":
        return self

    def update_data(
        self,
        status: RunStatus = init_status,
        **data: Any,
    ) -> "PipelineRun":
        self.status = status
        if isinstance(data, dict):
            for key in self.reserved_keys:
                if key in data:
                    self.error_logger(f"Key {key} is reserved and cannot be used in data. This will be skipped")
                    del data[key]
            self.data.update(data)
        else:
            raise TypeError(f"Data must be a dict, got {type(data)}")
        return self

    def __exit__(self, exc_type=None, exc_value=None, traceback_=None) -> bool:
        suppress_exception = False

        if any((exc_type, exc_value, traceback_)):
            self.status = RunStatus.FAILURE
            self.data[self.exception] = traceback.format_exc()

        message = self.get_message(self.config.dump_truncated_to_file)
        run = ExtractionPipelineRun(
            status=self.status.value,
            extpipe_external_id=self.pipeline_external_id,
            message=message,
        )
        self.client.extraction_pipelines.runs.create(run)

        return suppress_exception

    def get_message(self, dump_truncated_to_file: bool = None) -> str:
        file_external_id = (
            f"{self.config.log_file_prefix}/{self.pipeline_external_id}/"
            f"{datetime.now(timezone.utc).isoformat().replace(':', '')}"
        )
        data, file_content = self._create_run_data_and_file_content(file_external_id, dump_truncated_to_file)
        if dump_truncated_to_file and file_content:
            try:
                file_id = self.client.files.upload_bytes(
                    content=file_content,
                    name=f"{file_external_id.lower()}.log".replace("/", "_"),
                    external_id=file_external_id,
                    mime_type="text/plain",
                    data_set_id=self.data_set_id,
                ).id
            except CogniteAPIError as e:
                self.error_logger(f"Failed to upload log file {file_external_id}: {e}")
                file_id = ""

            data[self.log_file_id] = file_id

        return self._as_json(data)

    def _create_run_data_and_file_content(
        self, file_external_id: str, dump_truncated_to_file: bool
    ) -> tuple[dict, str]:
        file_content = []
        data = self.data.copy()
        if dump_truncated_to_file:
            data[self.log_file_external_id] = file_external_id
            # Need a dummy id as we do not know the id before the file is uploaded
            data[self.log_file_id] = MAX_VALID_INTERNAL_ID

        dumped = self._as_json(data)
        if (above_limit := len(dumped) - MSG_CHAR_LIMIT) > 0:
            # In case, truncating the specified keys is not enough, we also start to truncate everything else.
            truncate_keys = self.config.truncate_keys + list(
                set(data) - {self.log_file_id, self.log_file_external_id} - set(self.config.truncate_keys)
            )
            for key in truncate_keys:
                if key not in data:
                    continue
                file_content.append(
                    f"{'='*70}\n{key}\n{'='*70}\n{self.data[key]}\n{'='*70}",
                )
                entry_length = len(data[key])
                if entry_length >= above_limit + 3:
                    data[key] = data[key][: entry_length - (above_limit + 3)] + "..."
                    break
                elif entry_length < 3:
                    continue
                else:
                    reduction = min((above_limit + 3) - entry_length, entry_length)
                    data[key] = "..."
                    above_limit = above_limit - reduction

        return data, "\n\n".join(file_content)

    @staticmethod
    def _as_json(data: dict) -> str:
        # Removing space to use as little space as possible
        return json.dumps(data, separators=(",", ":"))


class ExtractionPipelineCreate:
    """
    This is a simplified write version of an Extraction pipeline.

    This ensures that the Extraction Pipeline exists when calling the `create_pipeline_run` method.

    """

    def __init__(
        self,
        external_id: str,
        data_set_external_id: str,
        description: str | None = None,
        dump_truncated_to_file: bool = True,
        truncate_keys: list[str] = None,
        log_file_prefix: str | None = None,
    ) -> None:
        self.external_id = external_id
        self.dataset_external_id = data_set_external_id
        self.description = description
        self._data_set_id: Optional[int] = None

        self.config = _Config(
            dump_truncated_to_file=dump_truncated_to_file,
            truncate_keys=truncate_keys,
            log_file_prefix=log_file_prefix,
        )

    def get_or_create(self, client: CogniteClient):
        extraction_pipeline = client.extraction_pipelines.retrieve(external_id=self.external_id)
        if extraction_pipeline is None:
            self._data_set_id = client.data_sets.retrieve(external_id=self.dataset_external_id).id
            client.extraction_pipelines.create(
                ExtractionPipeline(
                    external_id=self.external_id,
                    name=self.external_id,
                    data_set_id=self._data_set_id,
                    description=self.description,
                )
            )
        return self

    def create_pipeline_run(self, client: CogniteClient, error_logger: Callable[[str], None] = None) -> PipelineRun:
        """

        Args:
            client: An instance of the CogniteClient.
            error_logger: The function to use for logging errors. Defaults to print.

        Returns:
            Return a pipeline run that can be used as a context manager.
        """
        self._data_set_id = self._data_set_id or client.data_sets.retrieve(external_id=self.dataset_external_id).id

        return PipelineRun(client, self.external_id, self.config, self._data_set_id, error_logger or print)
