from __future__ import annotations

from typing import Union, cast

from cognite.client.data_classes import Asset, Event, FileMetadata, Relationship, Sequence, TimeSeries

# from cognite.powerops.clients.cogshop.data_classes import ModelTemplate, ModelTemplateApply, Mapping, MappingApply, \
#     Transformation, TransformationApply, FileRef, FileRefApply
# from cognite.powerops.clients.cogshop.data_classes._core import DomainModel, DomainModelApply


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
