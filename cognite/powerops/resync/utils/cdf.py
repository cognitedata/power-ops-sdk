from __future__ import annotations

from typing import TypeVar, Union, Protocol, Sequence, Any, Literal, cast

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, Sequence as CogniteSequence, FileMetadata, Relationship, FileMetadataList
from cognite.client.data_classes.data_modeling import NodeApply, EdgeApply

from cognite.powerops.resync.models.cdf_resources import CDFFile, CDFSequence

T_CogniteResource = TypeVar(
    "T_CogniteResource", bound=Union[Asset, CogniteSequence, FileMetadata, Relationship, NodeApply, EdgeApply]
)


class CogniteAPI(Protocol[T_CogniteResource]):  # type: ignore[misc]
    def create(self, items: T_CogniteResource | Sequence[T_CogniteResource]) -> Any:
        ...

    def delete(self, external_ids: str | Sequence[str]) -> Any:
        ...

    def upsert(
        self, item: T_CogniteResource | Sequence[T_CogniteResource], mode: Literal["patch", "replace"] = "patch"
    ) -> Any:
        ...


class FileAdapter(CogniteAPI[FileMetadata]):
    def __init__(self, client: CogniteClient, files_by_id: dict[str, CDFFile]):
        self.client = client
        self.files_by_id = files_by_id

    def create(self, items: FileMetadata | Sequence[FileMetadata]) -> Any:
        items = [items] if isinstance(items, FileMetadata) else items
        for item in items:
            if item.external_id is None or item.external_id not in self.files_by_id:
                raise ValueError("Cannot create new file {item.external_id} missing file content")
            content = self.files_by_id[item.external_id].content
            if content is None:
                raise ValueError(f"Cannot create new file {item.external_id} missing file content")
            self.client.files.upload_bytes(content, **item.dump())

    def delete(self, external_ids: str | Sequence[str]) -> Any:
        self.client.files.delete(external_id=external_ids)

    def upsert(self, item: FileMetadata | Sequence[FileMetadata], mode: Literal["patch", "replace"] = "patch") -> Any:
        items = [item] if isinstance(item, FileMetadata) else item
        updated = self.client.files.update(items)
        update_by_id = {u.external_id: u for u in ([updated] if isinstance(updated, FileMetadata) else updated)}

        # There are not all fields that can be updates for files, for example, name.
        # Therefore, we need to delete and recreate files that have changed for example name.
        to_delete_and_recreate = FileMetadataList([])
        for item in items:
            if item.external_id not in update_by_id:
                raise ValueError(f"Could not update file {item.external_id}")
            if item != update_by_id[item.external_id]:
                to_delete_and_recreate.append(item)
        if to_delete_and_recreate:
            self.delete(to_delete_and_recreate.as_external_ids())
            self.create(to_delete_and_recreate)


class SequenceAdapter(CogniteAPI[CogniteSequence]):
    def __init__(self, client: CogniteClient, sequence_by_id: dict[str, CDFSequence]):
        self.client = client
        self.sequence_by_id = sequence_by_id

    def create(self, items: CogniteSequence | Sequence[CogniteSequence]) -> Any:
        items = [items] if isinstance(items, CogniteSequence) else items
        if missing := set(self.sequence_by_id) - {i.external_id for i in items}:
            raise ValueError(f"Missing sequence content for {missing}")
        self.client.sequences.create(items)
        for item in items:
            if item.external_id is None:
                raise ValueError("Missing external id for sequence")
            df = self.sequence_by_id[item.external_id].content
            if df is None:
                raise ValueError(f"Missing sequence content for {item.external_id}")
            self.client.sequences.data.insert_dataframe(df, external_id=item.external_id)

    def delete(self, external_ids: str | Sequence[str]) -> Any:
        self.client.sequences.delete(external_id=external_ids)

    def upsert(
        self, item: CogniteSequence | Sequence[CogniteSequence], mode: Literal["patch", "replace"] = "patch"
    ) -> Any:
        self.client.sequences.update(item)


T_Instance = TypeVar("T_Instance", bound=Union[NodeApply, EdgeApply])


class InstanceAdapter(CogniteAPI[T_Instance]):
    def __init__(self, client: CogniteClient, instance_type: Literal["node", "edge"]):
        self.instance_type = instance_type
        self.client = client

    def create(self, items: T_Instance | Sequence[T_Instance]) -> Any:
        if self.instance_type == "node":
            self.client.data_modeling.instances.apply(nodes=items)  # type: ignore[arg-type]
        else:
            self.client.data_modeling.instances.apply(edges=items)  # type: ignore[arg-type]

    def delete(self, external_ids: str | Sequence[str]) -> Any:
        if self.instance_type == "node":
            self.client.data_modeling.instances.delete(nodes=external_ids)  # type: ignore[arg-type]
        else:
            self.client.data_modeling.instances.delete(edges=external_ids)  # type: ignore[arg-type]

    def upsert(self, item: T_Instance | Sequence[T_Instance], mode: Literal["patch", "replace"] = "patch") -> Any:
        self.create(item)


def get_cognite_api(
    client: CogniteClient, name: str, new_sequences_by_id: dict[str, CDFSequence], new_files_by_id: dict[str, CDFFile]
) -> CogniteAPI:
    if name == "assets":
        return cast(CogniteAPI[Asset], client.assets)
    elif name == "time_series":
        raise NotImplementedError("Resync does not create timeseries")
    elif name == "sequences":
        return SequenceAdapter(client, new_sequences_by_id)
    elif name == "files":
        return FileAdapter(client, new_files_by_id)
    elif name == "relationships":
        return client.relationships
    elif name == "nodes":
        return InstanceAdapter[NodeApply](client, "node")
    elif name == "edges":
        return InstanceAdapter[EdgeApply](client, "edge")
    raise ValueError(f"Unknown resource type {name}")
