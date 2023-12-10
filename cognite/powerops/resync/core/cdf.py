from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal, Protocol, TypeVar, Union, cast

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, FileMetadata, FileMetadataList, LabelDefinition, Relationship
from cognite.client.data_classes import Sequence as CogniteSequence
from cognite.client.data_classes.data_modeling import ContainerId, DataModelId, EdgeApply, NodeApply, ViewId
from cognite.client.exceptions import CogniteAPIError

from cognite.powerops.resync.models.base import CDFFile, CDFSequence
from cognite.powerops.utils.navigation import chunks

T_CogniteResource = TypeVar(
    "T_CogniteResource",
    bound=Union[
        Asset,
        CogniteSequence,
        FileMetadata,
        Relationship,
        NodeApply,
        EdgeApply,
        ContainerId,
        ViewId,
        DataModelId,
        LabelDefinition,
    ],
)


class CogniteAPI(Protocol[T_CogniteResource]):  # type: ignore[misc]
    def create(self, items: T_CogniteResource | Sequence[T_CogniteResource]) -> Any:
        ...

    def delete(self, external_id: str | Sequence[str]) -> Any:
        ...

    def upsert(
        self, item: T_CogniteResource | Sequence[T_CogniteResource], mode: Literal["patch", "replace"] = "patch"
    ) -> Any:
        ...


class FileAdapter(CogniteAPI[FileMetadata]):
    def __init__(self, client: CogniteClient, files_by_id: dict[str, CDFFile] | None = None):
        self.client = client
        self.files_by_id = files_by_id

    def create(self, items: FileMetadata | Sequence[FileMetadata]) -> Any:
        if self.files_by_id is None:
            raise ValueError("Missing file content need to be provided")
        items = [items] if isinstance(items, FileMetadata) else items
        for item in items:
            if item.external_id is None or item.external_id not in self.files_by_id:
                raise ValueError("Cannot create new file {item.external_id} missing file content")
            content = self.files_by_id[item.external_id].content
            if content is None:
                raise ValueError(f"Cannot create new file {item.external_id} missing file content")
            self.client.files.upload_bytes(content, **item.dump(), overwrite=True)

    def delete(self, external_id: str | Sequence[str]) -> Any:
        self.client.files.delete(external_id=external_id)

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
    def __init__(self, client: CogniteClient, sequence_by_id: dict[str, CDFSequence] | None = None):
        self.client = client
        self.sequence_by_id = sequence_by_id

    def create(self, items: CogniteSequence | Sequence[CogniteSequence]) -> Any:
        if self.sequence_by_id is None:
            raise ValueError("Missing sequence content need to be provided")

        items = [items] if isinstance(items, CogniteSequence) else items
        if missing := {i.external_id for i in items} - set(self.sequence_by_id):
            raise ValueError(f"Missing sequence content for {missing}")
        self.client.sequences.upsert(items, mode="replace")
        for item in items:
            if item.external_id is None:
                raise ValueError("Missing external id for sequence")
            df = self.sequence_by_id[item.external_id].content
            if df is None:
                raise ValueError(f"Missing sequence content for {item.external_id}")
            self.client.sequences.data.insert_dataframe(df, external_id=item.external_id)

    def delete(self, external_id: str | Sequence[str]) -> Any:
        self.client.sequences.delete(external_id=external_id)

    def upsert(
        self, item: CogniteSequence | Sequence[CogniteSequence], mode: Literal["patch", "replace"] = "patch"
    ) -> Any:
        self.client.sequences.update(item)


T_Instance = TypeVar("T_Instance", bound=Union[NodeApply, EdgeApply])


class InstanceAdapter(CogniteAPI[T_Instance]):
    instance_limit = 1000

    def __init__(self, client: CogniteClient, instance_type: Literal["node", "edge"]):
        self.instance_type = instance_type
        self.client = client

    def create(self, items: T_Instance | Sequence[T_Instance]) -> Any:
        item_sequence: Sequence[T_Instance]
        if not isinstance(items, Sequence):
            item_sequence = [items]
        else:
            item_sequence = items

        failed = []
        for chunk in chunks(item_sequence, 1):  # type: ignore[type-var]
            if self.instance_type == "node":
                try:
                    self.client.data_modeling.instances.apply(nodes=chunk, auto_create_direct_relations=True)  # type: ignore[arg-type]
                except CogniteAPIError:
                    print(f"Failed on {chunk[0].dump()}")
                    failed.append(chunk[0].dump())
            else:
                try:
                    self.client.data_modeling.instances.apply(
                        edges=chunk, auto_create_start_nodes=True, auto_create_end_nodes=True  # type: ignore[arg-type]
                    )
                except CogniteAPIError:
                    print(f"Failed on {chunk[0].dump()}")
                    failed.append(chunk[0].dump())
        if failed:
            raise ValueError(f"Failed on {len(failed)}/{len(item_sequence)}")

    def delete(self, external_id: str | Sequence[str]) -> Any:
        if self.instance_type == "node":
            self.client.data_modeling.instances.delete(nodes=external_id)  # type: ignore[arg-type]
        else:
            self.client.data_modeling.instances.delete(edges=external_id)  # type: ignore[arg-type]

    def upsert(self, item: T_Instance | Sequence[T_Instance], mode: Literal["patch", "replace"] = "patch") -> Any:
        self.create(item)


class DataModelingAdapter(CogniteAPI[T_CogniteResource]):
    def __init__(self, api: Any) -> None:
        self.api = api

    def create(self, items: T_CogniteResource | Sequence[T_CogniteResource]) -> Any:
        self.api.apply(items)

    def delete(self, external_id: str | Sequence[str]) -> Any:
        self.api.delete(external_id)

    def upsert(
        self, item: T_CogniteResource | Sequence[T_CogniteResource], mode: Literal["patch", "replace"] = "patch"
    ) -> Any:
        self.api.apply(item)


class LabelsAdapter(CogniteAPI[LabelDefinition]):
    def __init__(self, client: CogniteClient):
        self.client = client

    def create(self, items: LabelDefinition | Sequence[LabelDefinition]) -> Any:
        self.client.labels.create(items)

    def delete(self, external_id: str | Sequence[str]) -> Any:
        # Labels are not deleted.
        ...

    def upsert(
        self, item: LabelDefinition | Sequence[LabelDefinition], mode: Literal["patch", "replace"] = "patch"
    ) -> Any:
        # Labels are not changes
        ...


def get_cognite_api(
    client: CogniteClient,
    name: str,
    new_sequences_by_id: dict[str, CDFSequence] | None = None,
    new_files_by_id: dict[str, CDFFile] | None = None,
) -> CogniteAPI:
    if name == "assets" or name == "parent_assets":
        return cast(CogniteAPI[Asset], client.assets)
    elif name == "labels":
        return LabelsAdapter(client)
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
    elif name == "data_models":
        return DataModelingAdapter[DataModelId](client.data_modeling.data_models)
    elif name == "views":
        return DataModelingAdapter[ViewId](client.data_modeling.views)
    elif name == "containers":
        return DataModelingAdapter[ContainerId](client.data_modeling.containers)
    raise ValueError(f"Unknown resource type {name}")
