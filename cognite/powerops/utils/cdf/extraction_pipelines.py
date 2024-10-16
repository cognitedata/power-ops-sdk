from __future__ import annotations

import json
import logging
import traceback
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, ClassVar, Optional

from cognite.client import CogniteClient
from cognite.client._constants import MAX_VALID_INTERNAL_ID
from cognite.client.data_classes import ExtractionPipeline, ExtractionPipelineRun
from cognite.client.exceptions import CogniteAPIError

from cognite.powerops.utils.retry import retry

logger = logging.getLogger(__name__)

MSG_CHAR_LIMIT = 1000


class RunStatus(str, Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    SEEN = "seen"


@dataclass
class _Config:
    dump_truncated_to_file: bool = True
    message_keys_skip: list[str] = field(default_factory=list)
    truncate_keys: list[str] = field(default_factory=list)
    reverse_truncate_keys: list[str] = field(default_factory=list)
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

    reserved_keys: ClassVar[tuple[str, ...]] = (log_file_external_id, log_file_id, exception)

    def __init__(
        self,
        client: CogniteClient,
        pipeline_external_id: str,
        config: _Config,
        data_set_id: int,
        error_logger: Callable[[str], None],
        is_try_run: bool,
    ) -> None:
        self.client = client
        self.pipeline_external_id = pipeline_external_id
        self.config = config
        self.data_set_id = data_set_id
        self.error_logger = error_logger
        self.is_dry_run = is_try_run

        self.data: dict[str, Any] = {}
        self.status = self.init_status

    def __enter__(self) -> PipelineRun:
        return self

    def update_data(self, status: RunStatus | None = None, **data: dict[str, Any]) -> PipelineRun:
        if status is not None:
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

    def __exit__(self, exc_type=None, exc_value=None, traceback_=None) -> bool:  # type: ignore[no-untyped-def]
        suppress_exception = False

        if any((exc_type, exc_value, traceback_)):
            self.status = RunStatus.FAILURE
            self.data[self.exception] = traceback.format_exc()

        message = self.get_message(self.config.dump_truncated_to_file)
        run = ExtractionPipelineRun(
            status=self.status.value, extpipe_external_id=self.pipeline_external_id, message=message
        )
        if not self.is_dry_run:
            retry(tries=5, delay=1, backoff=3, logger=logger)(self.client.extraction_pipelines.runs.create)(run)

        return suppress_exception

    def get_message(self, dump_truncated_to_file: bool) -> str:
        file_external_id = (
            f"{self.config.log_file_prefix}/{self.pipeline_external_id}/"
            f"{datetime.now(timezone.utc).isoformat().replace(':', '')}"
        )
        data, file_content = self._create_run_data_and_file_content(file_external_id, dump_truncated_to_file)
        if dump_truncated_to_file and file_content and not self.is_dry_run:
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
                file_id = None

            data[self.log_file_id] = file_id

        return self._as_json(data, self.config.message_keys_skip)

    def _create_run_data_and_file_content(
        self, file_external_id: str, dump_truncated_to_file: bool
    ) -> tuple[dict, str]:
        file_content = []
        data = self.data.copy()
        if dump_truncated_to_file:
            data[self.log_file_external_id] = file_external_id
            # Need a dummy id as we do not know the id before the file is uploaded
            data[self.log_file_id] = MAX_VALID_INTERNAL_ID
        # Ensure we get a dict[str, str]
        for key in list(data):
            value = data[key]
            data[key] = self._as_json(value) if isinstance(value, (dict | list)) else str(value)

        truncating = len(self._as_json(data)) - MSG_CHAR_LIMIT > 0
        truncate_keys = self.config.truncate_keys + list(
            set(data) - {self.log_file_id, self.log_file_external_id} - set(self.config.truncate_keys)
        )
        while truncate_keys and (above_limit := len(self._as_json(data)) - MSG_CHAR_LIMIT) > 0:
            # In case, truncating the specified keys is not enough, we also start to truncate everything else.
            key = truncate_keys.pop(0)
            if key not in data:
                continue

            entry_length = len(data[key])
            if entry_length < 3:
                continue

            if entry_length >= above_limit + 3:
                trim_len = entry_length - (above_limit + 3)
                if key in self.config.reverse_truncate_keys:
                    data[key] = "..." + data[key][-trim_len:]
                else:
                    data[key] = data[key][:trim_len] + "..."
            else:
                data[key] = "..."

        if truncating:
            # We dump all data to the file, even the keys which are not truncated.
            for key, value in self.data.items():
                if key in {self.log_file_id, self.log_file_external_id}:
                    continue
                file_content.append(f"{'='*70}\n{key}\n{'='*70}\n{value}\n")

        return data, "\n\n".join(file_content)

    @staticmethod
    def _as_json(data: dict | list, exclude_keys: Iterable[str] | None = None) -> str:
        exclude_keys = set(exclude_keys or [])
        cleaned: dict[str, Any] | list[Any]
        # Removing space to use as little space as possible
        if isinstance(data, dict):
            cleaned = {k: v for k, v in data.items() if k not in exclude_keys}
        elif isinstance(data, list):
            cleaned = data
        else:
            raise TypeError(f"Data must be a dict or list, got {type(data)}")
        return json.dumps(cleaned, separators=(",", ":"), default=str)


class ExtractionPipelineCreate:
    """
    This is a simplified write version of an Extraction pipeline.

    This ensures that the Extraction Pipeline exists when calling the `create_pipeline_run` method.

    Args:
        external_id: The external id of the extraction pipeline.
        data_set_external_id: The external id of the dataset to use.
        description: The description of the extraction pipeline.
        dump_truncated_to_file: Whether to dump truncated data to a file. This is used when the data is too large to
            be stored in the message field of the extraction pipeline run.
        message_keys_skip: The keys that should not be part of the pipeline run message, and instead only
            be dumped to a file.
        truncate_keys_first: The keys to truncate first. This is useful when you expect the data to be too large too
            to be stored in a message field of the extraction pipeline run, and you want to select which keys
            to truncate first.
        reverse_truncate_keys: Keys with important content at the end, so trim the start of the content.
    """

    def __init__(
        self,
        external_id: str,
        data_set_external_id: str,
        description: str | None = None,
        dump_truncated_to_file: bool = True,
        message_keys_skip: Optional[list[str]] = None,
        truncate_keys_first: Optional[list[str]] = None,
        reverse_truncate_keys: Optional[list[str]] = None,
        log_file_prefix: str | None = None,
    ) -> None:
        self.external_id = external_id
        self.dataset_external_id = data_set_external_id
        self.description = description
        self._data_set_id: Optional[int] = None

        self.config = _Config(
            dump_truncated_to_file=dump_truncated_to_file,
            message_keys_skip=message_keys_skip or [],
            truncate_keys=truncate_keys_first or [],
            reverse_truncate_keys=reverse_truncate_keys or [],
            log_file_prefix=log_file_prefix,
        )

    def get_or_create(self, client: CogniteClient) -> ExtractionPipelineCreate:
        extraction_pipeline = client.extraction_pipelines.retrieve(external_id=self.external_id)
        if extraction_pipeline is None:
            self._data_set_id = self._get_data_set_it(client)
            client.extraction_pipelines.create(
                ExtractionPipeline(
                    external_id=self.external_id,
                    name=self.external_id,
                    data_set_id=self._data_set_id,
                    description=self.description,
                )
            )
        return self

    def create_pipeline_run(
        self, client: CogniteClient, is_dry_run: bool = False, error_logger: Optional[Callable[[str], None]] = None
    ) -> PipelineRun:
        """

        Args:
            client: An instance of the CogniteClient.
            is_dry_run: Whether to run the pipeline as a dry run. This will not create a pipeline run.
            error_logger: The function to use for logging errors. Defaults to print.

        Returns:
            Return a pipeline run that can be used as a context manager.
        """
        self._data_set_id = self._data_set_id or self._get_data_set_it(client)

        return PipelineRun(client, self.external_id, self.config, self._data_set_id, error_logger or print, is_dry_run)

    def _get_data_set_it(self, client: CogniteClient) -> int:
        data_set = client.data_sets.retrieve(external_id=self.dataset_external_id)
        if data_set is None:
            raise ValueError(f"Dataset {self.dataset_external_id} does not exist")
        if data_set.id is None:
            raise ValueError(f"Dataset {self.dataset_external_id} does not have an id")
        return data_set.id
