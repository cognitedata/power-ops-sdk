from __future__ import annotations

import datetime
import re
from typing import Any
from unittest.mock import MagicMock

import cognite.powerops.client._generated.data_classes as data_classes
from cognite.powerops.client._generated.data_classes._core import (
    DEFAULT_INSTANCE_SPACE,
    DataRecord,
    DataRecordGraphQL,
    DomainModelWrite,
    GraphQLList,
)

_FIXED_TS = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)


def _make_data_record() -> DataRecord:
    return DataRecord(version=1, last_updated_time=_FIXED_TS, created_time=_FIXED_TS)


def _make_data_record_graphql() -> DataRecordGraphQL:
    return DataRecordGraphQL(last_updated_time=_FIXED_TS, created_time=_FIXED_TS)


def _write_to_read(write_obj: DomainModelWrite, store: dict, retrieve_connections: str = "full"):
    """Convert a DomainModelWrite object to its corresponding Read object."""
    write_class = type(write_obj)
    read_class_name = write_class.__name__.removesuffix("Write")
    read_class = getattr(data_classes, read_class_name, None)
    if read_class is None:
        return None

    edge_fields = {name for name, _ in getattr(write_class, "_outwards_edges", ())}
    direct_relation_fields = set(getattr(write_class, "_direct_relations", ()))

    kwargs: dict[str, Any] = {
        "space": getattr(write_obj, "space", DEFAULT_INSTANCE_SPACE),
        "external_id": write_obj.external_id,
        "data_record": _make_data_record(),
    }

    for field_name in write_class.model_fields:
        if field_name in ("space", "external_id", "data_record", "node_type"):
            continue

        value = getattr(write_obj, field_name, None)

        if field_name in edge_fields:
            if retrieve_connections == "skip":
                kwargs[field_name] = None
            elif value is None:
                kwargs[field_name] = None
            else:
                items = value if isinstance(value, list) else [value]
                if retrieve_connections == "identifier":
                    ids = [
                        v.external_id if isinstance(v, DomainModelWrite) else str(v)
                        for v in items
                        if v is not None
                    ]
                    kwargs[field_name] = ids if ids else None
                else:  # full
                    resolved = []
                    for v in items:
                        if v is None:
                            continue
                        if isinstance(v, DomainModelWrite):
                            read = _write_to_read(v, store, retrieve_connections)
                            resolved.append(read if read is not None else v.external_id)
                        elif isinstance(v, str):
                            stored = store.get(v)
                            resolved.append(
                                _write_to_read(stored, store, retrieve_connections) if stored else v
                            )
                        else:
                            resolved.append(v)
                    kwargs[field_name] = resolved if resolved else None

        elif field_name in direct_relation_fields:
            if value is None:
                kwargs[field_name] = None
            elif retrieve_connections == "full":
                if isinstance(value, DomainModelWrite):
                    read = _write_to_read(value, store, retrieve_connections)
                    kwargs[field_name] = read if read is not None else value.external_id
                elif isinstance(value, str):
                    stored = store.get(value)
                    kwargs[field_name] = (
                        _write_to_read(stored, store, retrieve_connections) if stored else value
                    )
                else:
                    kwargs[field_name] = value
            else:  # skip or identifier: return external_id string
                if isinstance(value, DomainModelWrite):
                    kwargs[field_name] = value.external_id
                else:
                    kwargs[field_name] = value

        else:
            kwargs[field_name] = value

    try:
        return read_class(**kwargs)
    except Exception:
        return None


def _write_to_graphql(write_obj: DomainModelWrite, store: dict):
    """Convert a DomainModelWrite object to its corresponding GraphQL object."""
    write_class = type(write_obj)
    graphql_class_name = write_class.__name__.removesuffix("Write") + "GraphQL"
    graphql_class = getattr(data_classes, graphql_class_name, None)
    if graphql_class is None:
        return None

    edge_fields = {name for name, _ in getattr(write_class, "_outwards_edges", ())}
    direct_relation_fields = set(getattr(write_class, "_direct_relations", ()))

    kwargs: dict[str, Any] = {
        "space": getattr(write_obj, "space", DEFAULT_INSTANCE_SPACE),
        "external_id": write_obj.external_id,
        "data_record": _make_data_record_graphql(),
    }

    for field_name in write_class.model_fields:
        if field_name in ("space", "external_id", "data_record", "node_type"):
            continue

        value = getattr(write_obj, field_name, None)

        if field_name in edge_fields:
            if value is None:
                kwargs[field_name] = None
            else:
                items = value if isinstance(value, list) else [value]
                resolved = []
                for v in items:
                    if v is None:
                        continue
                    if isinstance(v, DomainModelWrite):
                        gql = _write_to_graphql(v, store)
                        if gql is not None:
                            resolved.append(gql)
                    elif isinstance(v, str):
                        stored = store.get(v)
                        if stored:
                            gql = _write_to_graphql(stored, store)
                            if gql is not None:
                                resolved.append(gql)
                kwargs[field_name] = resolved if resolved else None
        elif field_name in direct_relation_fields:
            if value is None:
                kwargs[field_name] = None
            elif isinstance(value, DomainModelWrite):
                kwargs[field_name] = _write_to_graphql(value, store)
            elif isinstance(value, str):
                stored = store.get(value)
                kwargs[field_name] = _write_to_graphql(stored, store) if stored else None
            else:
                kwargs[field_name] = value
        else:
            kwargs[field_name] = value

    try:
        return graphql_class(**kwargs)
    except Exception:
        return None


class _MockRetrieveAPI:
    def __init__(self, store: dict):
        self._store = store

    def retrieve(self, external_id=None, retrieve_connections: str = "full", **kwargs):
        if isinstance(external_id, list):
            results = []
            for eid in external_id:
                obj = self._store.get(eid)
                if obj is not None:
                    read = _write_to_read(obj, self._store, retrieve_connections)
                    if read is not None:
                        results.append(read)
            return results
        obj = self._store.get(external_id)
        if obj is None:
            return None
        return _write_to_read(obj, self._store, retrieve_connections)


class _MockSubModelClient:
    def __init__(self, store: dict):
        self._store = store
        self._api = _MockRetrieveAPI(store)

    def __getattr__(self, name: str):
        return self._api

    def graphql_query(self, query: str, variables: dict | None = None) -> GraphQLList:
        match = re.search(r'externalId:\s*["\']([^"\']+)["\']', query)
        if not match:
            return GraphQLList([])
        external_id = match.group(1)
        obj = self._store.get(external_id)
        if obj is None:
            return GraphQLList([])
        graphql_obj = _write_to_graphql(obj, self._store)
        if graphql_obj is None:
            return GraphQLList([])
        return GraphQLList([graphql_obj])


class MockPowerOpsModelsClient:
    """In-memory mock for PowerOpsModelsClient.

    Stores DomainModelWrite objects by external_id and converts them
    to Read/GraphQL objects on retrieve, without any CDF credentials.
    """

    def __init__(self):
        self._store: dict[str, DomainModelWrite] = {}

    def upsert(self, items, replace: bool = False, allow_version_increase: bool = False):
        if isinstance(items, DomainModelWrite):
            items = [items]
        for item in items:
            self._store_recursive(item)
        return MagicMock()

    def _store_recursive(self, item: DomainModelWrite) -> None:
        self._store[item.external_id] = item
        for field_name in type(item).model_fields:
            value = getattr(item, field_name, None)
            if isinstance(value, DomainModelWrite):
                self._store_recursive(value)
            elif isinstance(value, list):
                for v in value:
                    if isinstance(v, DomainModelWrite):
                        self._store_recursive(v)

    def delete(self, external_id=None, space: str = DEFAULT_INSTANCE_SPACE):
        if isinstance(external_id, str):
            self._store.pop(external_id, None)
        elif isinstance(external_id, list):
            for eid in external_id:
                if isinstance(eid, str):
                    self._store.pop(eid, None)
        elif isinstance(external_id, DomainModelWrite):
            self._cascade_delete(external_id)
        return MagicMock()

    def _cascade_delete(self, item: DomainModelWrite) -> None:
        self._store.pop(item.external_id, None)
        for field_name in type(item).model_fields:
            value = getattr(item, field_name, None)
            if isinstance(value, DomainModelWrite):
                self._cascade_delete(value)
            elif isinstance(value, list):
                for v in value:
                    if isinstance(v, DomainModelWrite):
                        self._cascade_delete(v)

    def __getattr__(self, name: str):
        return _MockSubModelClient(self._store)
