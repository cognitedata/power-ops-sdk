from __future__ import annotations

import hashlib
import json
from typing import BinaryIO, Optional, TextIO, Union

from cognite.client.data_classes import (
    Asset,
    AssetList,
    Event,
    EventList,
    FileMetadata,
    LabelDefinition,
    LabelDefinitionList,
    Relationship,
    RelationshipList,
    Sequence,
    SequenceList,
    TimeSeries,
    TimeSeriesList,
)
from cognite.client.data_classes.data_modeling import (
    EdgeApply,
    EdgeApplyResult,
    EdgeApplyResultList,
    InstanceApply,
    InstancesApplyResult,
    NodeApply,
    NodeApplyResult,
    NodeApplyResultList,
)


class MockAssetsCreate:
    def __init__(self):
        self.assets = []

    def __call__(self, asset: Union[Asset, list[Asset]]) -> Union[Asset, AssetList]:
        if isinstance(asset, list):
            self.assets.extend(asset)
        else:
            self.assets.append(asset)
        return asset

    def serialize(self) -> list[dict]:
        assets_serialized = [asset.dump(camel_case=False) for asset in self.assets]
        # remove the field "data_set_id" from all the serialized asset dicts
        for asset in assets_serialized:
            asset.pop("data_set_id")
            if "metadata" in asset:
                metadata = asset["metadata"]
                if "rkom:plants" in metadata:
                    # replace single quotes with double and double with single to adhere to json str format
                    rkom_plants_metadata_json = json.loads(metadata["rkom:plants"].replace("'", '"'))
                    metadata["rkom:plants"] = str(sorted(rkom_plants_metadata_json))
        return assets_serialized


class MockSequencesCreate:
    def __init__(self):
        self.sequences = []

    def __call__(self, sequence: Union[Sequence, list[Sequence]]) -> Union[Sequence, SequenceList]:
        if isinstance(sequence, list):
            self.sequences.extend(sequence)
        else:
            self.sequences.append(sequence)
        return sequence

    def serialize(self) -> list[dict]:
        sequences_serialized = [sequence.dump(camel_case=False) for sequence in self.sequences]
        for sequence in sequences_serialized:
            sequence.pop("data_set_id")
        return sequences_serialized


class MockRelationshipsCreate:
    def __init__(self):
        self.relationships = []

    def __call__(self, relationship: Union[Relationship, list[Relationship]]) -> Union[Relationship, RelationshipList]:
        if isinstance(relationship, list):
            self.relationships.extend(relationship)
        else:
            self.relationships.append(relationship)
        return relationship

    def serialize(self) -> list[dict]:
        relationships_serialized = [relationship.dump(camel_case=False) for relationship in self.relationships]
        for relationship in relationships_serialized:
            relationship.pop("data_set_id")
            # do not check source and target as they are checked in assets
            relationship.pop("source") if "source" in relationship else None
            relationship.pop("target") if "target" in relationship else None
            # convert labels to string
            if "labels" in relationship:
                relationship["labels"] = str(relationship["labels"])
        return relationships_serialized


class MockTimeSeriesCreate:
    def __init__(self):
        self.time_series = []

    def __call__(self, time_series: Union[TimeSeries, list[TimeSeries]]) -> Union[TimeSeries, TimeSeriesList]:
        if isinstance(time_series, list):
            self.time_series.extend(time_series)
        else:
            self.time_series.append(time_series)
        return time_series

    def serialize(self) -> list[dict]:
        time_series_serialized = [time_series.dump(camel_case=False) for time_series in self.time_series]
        for time_series in time_series_serialized:
            time_series.pop("data_set_id")
        return time_series_serialized


class MockTimeSeriesRetrieveMultiple:
    def __init__(self, time_series: list[TimeSeries]):
        self.time_series = time_series

    def __call__(self, external_ids: list[str], **_):
        return TimeSeriesList(
            [time_series for time_series in self.time_series if time_series.external_id in external_ids]
        )


class MockLabelsCreate:
    def __init__(self):
        self.labels = []

    def __call__(
        self, label: Union[LabelDefinition, list[LabelDefinition]]
    ) -> Union[LabelDefinition, LabelDefinitionList]:
        if isinstance(label, list):
            self.labels.extend(label)
        else:
            self.labels.append(label)
        return label

    def serialize(self) -> list[dict]:
        labels_serialized = [label.dump(camel_case=False) for label in self.labels]
        for label in labels_serialized:
            label.pop("data_set_id")
        return labels_serialized


class MockEventsCreate:
    def __init__(self):
        self.events = []

    def __call__(self, event: Union[Event, list[Event]]) -> Union[Event, EventList]:
        if isinstance(event, list):
            self.events.extend(event)
        else:
            self.events.append(event)
        return event

    def serialize(self) -> list[dict]:
        events_serialized = [event.dump(camel_case=False) for event in self.events]
        for event in events_serialized:
            event.pop("data_set_id")
            # remove start time,end time, and external id as they depend on the time of creation
            event.pop("start_time")
            event.pop("end_time")
            event.pop("external_id")
        return events_serialized


class MockInstancesApply:
    def __init__(self):
        self.nodes = []
        self.edges = []

    def __call__(
        self,
        nodes: NodeApply | Sequence[NodeApply] | None = None,
        edges: EdgeApply | Sequence[EdgeApply] | None = None,
        replace: bool = False,
        **_,
    ) -> InstancesApplyResult:
        if nodes:
            if isinstance(nodes, list):
                self.nodes.extend(nodes)
            else:
                self.nodes.append(nodes)
        if edges:
            if isinstance(edges, list):
                self.edges.extend(edges)
            else:
                self.edges.append(edges)

        return InstancesApplyResult(
            nodes=NodeApplyResultList(
                [
                    NodeApplyResult(
                        space=node.space,
                        external_id=node.external_id,
                        version="1",
                        was_modified=True,
                        last_updated_time=1,
                        created_time=1,
                    )
                    for node in self.nodes
                ]
            ),
            edges=EdgeApplyResultList(
                [
                    EdgeApplyResult(
                        space=edge.space,
                        external_id=edge.external_id,
                        version="1",
                        was_modified=True,
                        last_updated_time=1,
                        created_time=1,
                    )
                    for edge in self.edges
                ]
            ),
        )

    def serialize(self) -> list[dict]:
        def key(instance: InstanceApply) -> tuple[str, str]:
            return instance.space, instance.external_id

        return [
            instance.dump(camel_case=False) for instance in sorted(self.nodes, key=key) + sorted(self.edges, key=key)
        ]


class MockFilesUploadBytes:
    def __init__(self):
        self.file_metadata = []
        self.content_sha256_hash = []

    def __call__(
        self,
        content: str | bytes | TextIO | BinaryIO,
        name: str,
        external_id: Optional[str] = None,
        source: Optional[str] = None,
        mime_type: Optional[str] = None,
        metadata: Optional[dict[str, str]] = None,
        directory: Optional[str] = None,
        data_set_id: Optional[int] = None,
        overwrite: bool = False,
        **_,
    ) -> FileMetadata:
        file_metadata = FileMetadata(
            external_id=external_id or name,
            name=name,
            metadata=metadata,
            source=source,
            mime_type=mime_type or "text/plain",
        )
        self.file_metadata.append(file_metadata)
        if isinstance(content, str):
            content = content.encode("utf-8")
        # Bytes are different between Windows and Linux
        sha256_hash = hashlib.sha256(content.replace(b"\r\n", b"\n")).hexdigest()
        self.content_sha256_hash.append(sha256_hash)
        return file_metadata

    def serialize(self) -> list[dict]:
        return [
            {**file_metadata.dump(camel_case=False), "sha256_hash": hash_}
            for file_metadata, hash_ in sorted(
                zip(self.file_metadata, self.content_sha256_hash), key=lambda f: f[0].external_id
            )
        ]
