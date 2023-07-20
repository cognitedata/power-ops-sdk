from __future__ import annotations

import logging
from typing import Optional, Union

from cognite.client import CogniteClient
from cognite.client._api.assets import AssetsAPI
from cognite.client._api.events import EventsAPI
from cognite.client._api.files import FilesAPI
from cognite.client._api.labels import LabelsAPI
from cognite.client._api.relationships import RelationshipsAPI
from cognite.client._api.sequences import SequencesAPI
from cognite.client.data_classes import Asset, Event, LabelDefinition, Relationship, RelationshipList, Sequence
from cognite.client.exceptions import CogniteDuplicatedError

logger = logging.getLogger(__name__)

CogniteAPI = Union[
    AssetsAPI,
    EventsAPI,
    FilesAPI,
    SequencesAPI,
    LabelsAPI,
    RelationshipsAPI,
]


def upsert_cognite_resources(
    api: CogniteAPI,
    resource_type: str,
    resources: list[Asset | Event | Sequence | LabelDefinition | Relationship],
) -> None:
    try:
        api.create(resources)
    except CogniteDuplicatedError as e:
        duplicated_ids = [ids["externalId"] for ids in e.duplicated]

        # Since the non-existing resource are not created when create fails, we need to create them in a separate call.
        if failed_resources := [r for r in e.failed if r.external_id not in duplicated_ids]:
            api.create(failed_resources)

        logger.debug(f"The following {resource_type} already exists in CDF: {duplicated_ids}")
        resources_to_update = [r for r in resources if r.external_id in duplicated_ids]
        if hasattr(api, "update"):
            api.update(resources_to_update)
        elif resource_type == "label_definitions":
            # For labels, the update method is not implemented. Hence, we need to delete and recreate.
            logger.debug("Deleting existing label definitions")
            api.delete(external_id=duplicated_ids)
            logger.debug("Creating new label definitions")
            api.create(resources_to_update)


def retrieve_event(client: CogniteClient, external_id: str) -> Event:
    event = client.events.retrieve(external_id=external_id)
    if event is None:
        raise ValueError(f"Event not found: {external_id}")
    return event


def retrieve_relationships_from_source_ext_id(
    client: CogniteClient,
    source_ext_id: str,
    label_ext_id: Optional[Union[str, list[str]]],
    target_types: Sequence[str] = None,
) -> RelationshipList:
    """
    Retrieve relationships between source and target using the source ext id.
    Using the `containsAny` filter, we can retrieve all relationships with  given label.
    """
    if isinstance(label_ext_id, str):
        label_ext_id = [label_ext_id]
    _labels = None
    if label_ext_id is not None:
        _labels = {"containsAny": [{"externalId": ext_id} for ext_id in label_ext_id]}
    return client.relationships.list(
        source_external_ids=[source_ext_id],
        labels=_labels,
        limit=-1,
        target_types=target_types,
    )
