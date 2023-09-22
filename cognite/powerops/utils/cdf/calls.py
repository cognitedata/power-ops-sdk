from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Optional, Union

from cognite.client import CogniteClient
from cognite.client.data_classes import Event, LabelFilter, RelationshipList

logger = logging.getLogger(__name__)


def retrieve_event(client: CogniteClient, external_id: str) -> Event:
    event = client.events.retrieve(external_id=external_id)
    if event is None:
        raise ValueError(f"Event not found: {external_id}")
    return event


def retrieve_relationships_from_source_ext_id(
    client: CogniteClient,
    source_ext_id: str,
    label_ext_id: Optional[Union[str, list[str]]],
    target_types: Sequence[str] | None = None,
) -> RelationshipList:
    """
    Retrieve relationships between source and target using the source ext id.
    Using the `containsAny` filter, we can retrieve all relationships with  given label.
    """
    if isinstance(label_ext_id, str):
        label_ext_id = [label_ext_id]
    if label_ext_id is not None:
        _labels = LabelFilter(contains_any=label_ext_id)
    else:
        _labels = None
    return client.relationships.list(
        source_external_ids=[source_ext_id], labels=_labels, limit=-1, target_types=target_types
    )
