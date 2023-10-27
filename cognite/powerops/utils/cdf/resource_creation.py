from __future__ import annotations

from typing import Union

from cognite.client.data_classes import Asset, Event, FileMetadata, Relationship, Sequence, TimeSeries

CDF_Resource = Union[Asset, TimeSeries, FileMetadata, Sequence, Event]


def simple_relationship(
    source: CDF_Resource,
    target: CDF_Resource,
    label_external_id: str,
) -> Relationship:
    """Simplifies Cognite Python SDK creation of Relationships."""
    external_id = f"{source.external_id}.{target.external_id}"
    source_type = "file" if isinstance(source, FileMetadata) else type(source).__name__
    target_type = "file" if isinstance(target, FileMetadata) else type(target).__name__

    return Relationship(
        external_id=external_id,
        source_type=source_type,
        target_type=target_type,
        source_external_id=source.external_id,
        target_external_id=target.external_id,
        labels=[label_external_id],
        data_set_id=source.data_set_id,
    )
