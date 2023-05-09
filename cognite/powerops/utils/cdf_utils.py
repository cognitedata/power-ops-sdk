from __future__ import annotations

import logging
from typing import Union

from cognite.client._api.assets import AssetsAPI
from cognite.client._api.events import EventsAPI
from cognite.client._api.files import FilesAPI
from cognite.client._api.labels import LabelsAPI
from cognite.client._api.relationships import RelationshipsAPI
from cognite.client._api.sequences import SequencesAPI
from cognite.client.data_classes import Asset, Event, FileMetadata, LabelDefinition, Relationship, Sequence, TimeSeries
from cognite.client.exceptions import CogniteDuplicatedError

logger = logging.getLogger(__name__)


def simple_relationship(
    source: Union[Asset, TimeSeries, FileMetadata, Sequence, Event],
    target: Union[Asset, TimeSeries, FileMetadata, Sequence, Event],
    label_external_id: str,
) -> Relationship:
    """Simplifies Cognite Python SDK creation of Relationships."""
    external_id = f"{source.external_id}.{target.external_id}"
    source_type = "file" if isinstance(source, FileMetadata) else source.__class__.__name__
    target_type = "file" if isinstance(target, FileMetadata) else target.__class__.__name__

    return Relationship(
        external_id=external_id,
        source_type=source_type,
        target_type=target_type,
        source_external_id=source.external_id,
        target_external_id=target.external_id,
        labels=[label_external_id],
    )


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
