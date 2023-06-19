import traceback
from enum import Enum, auto
from functools import wraps
from typing import Callable, Optional
from uuid import uuid4

from cognite.client import CogniteClient
from cognite.client.data_classes import Event
from cognite.client.exceptions import CogniteAPIError, CogniteMultiException
from retry import retry

from cognite.powerops.preprocessor import knockoff_logging as logging
from cognite.powerops.preprocessor.config import RelationshipsConfig, StatusEventConfig
from cognite.powerops.preprocessor.utils import create_relationship, now

logger = logging.getLogger(__name__)


class EventStatus(Enum):
    STARTED = auto()
    FINISHED = auto()
    FAILED = auto()


def _create_status_event(
    client: CogniteClient,
    status: EventStatus,
    parent_event: Event,
    extra_metadata: Optional[dict] = None,
) -> Event:
    suffix = str(uuid4())
    current_time = now()
    external_id = f"{StatusEventConfig.EVENT_EXTERNAL_ID_PREFIX}_{status.name}_{suffix}"
    event_type = f"{StatusEventConfig.EVENT_TYPE_PREFIX}_{status.name}"
    metadata = {"event_external_id": parent_event.external_id}
    if extra_metadata:
        metadata = {**extra_metadata, **metadata}
    event = Event(
        start_time=current_time,
        end_time=current_time,
        external_id=external_id,
        type=event_type,
        subtype=None,
        source=StatusEventConfig.EVENT_SOURCE,
        data_set_id=parent_event.data_set_id,
        metadata=metadata,
    )
    client.events.create(event=event)
    logger.info(f"Created status event '{event.external_id}'")
    return event


@retry(
    (CogniteAPIError, CogniteMultiException),
    tries=6,
    delay=0.5,
    backoff=1.2,  # increasing delays from 500ms to about 1s (500ms * 1.2^(6-1))
)
def _post_status_event(
    client: CogniteClient,
    status: EventStatus,
    parent_event_external_id: str,
    extra_metadata: Optional[dict] = None,
) -> None:
    """Posts a status event to CDF that is connected to the parent event through a relationship."""
    parent_event = client.events.retrieve(external_id=parent_event_external_id)
    event = _create_status_event(client=client, status=status, parent_event=parent_event, extra_metadata=extra_metadata)
    label_external_id = f"{RelationshipsConfig.LABEL_PREFIX}status_event_{status.name.lower()}"
    relationship = create_relationship(
        client=client,
        source=parent_event,
        target=event,
        label_external_id=label_external_id,
        data_set_id=parent_event.data_set_id,
    )
    logger.info(
        f"Status event '{event.external_id}' connected to '{parent_event.external_id}' by relationship"
        f" '{relationship.external_id}'"
    )


def wrap_status_events(func: Callable) -> Callable:
    """Wrapper that handles writing status events to CDF"""

    @wraps(func)
    def wrapper(client: CogniteClient, event_external_id: str):
        try:
            _post_status_event(
                client=client,
                status=EventStatus.STARTED,
                parent_event_external_id=event_external_id,
            )
            res = func(client, event_external_id)
            _post_status_event(
                client=client,
                status=EventStatus.FINISHED,
                parent_event_external_id=event_external_id,
            )
            return res
        except Exception:
            logger.error("Something went wrong:")
            stack_trace = traceback.format_exc()
            logger.error(stack_trace)
            extra_metadata = {
                "errorStackTrace": stack_trace,
            }  # "errorStackTrace" used by front end
            _post_status_event(
                client=client,
                status=EventStatus.FAILED,
                parent_event_external_id=event_external_id,
                extra_metadata=extra_metadata,
            )
            raise

    return wrapper
