from __future__ import annotations

from itertools import zip_longest
from typing import Any, Literal

from cognite.client.data_classes import AssetList, LabelDefinitionList
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList

from cognite.powerops.client._generated.assets.data_classes import DomainModelApply as DomainModelApplyAssets
from cognite.powerops.client._generated.cogshop1.data_classes._core import DomainModelApply as DomainModelApplyCogShop1
from cognite.powerops.client._generated.data_classes._core import DomainModelApply
from cognite.powerops.resync.models.base import AssetModel, CDFFile, CDFSequence, Model, ResourceType
from cognite.powerops.utils.serialization import remove_read_only_fields

from .data_classes import Change, FieldDifference, ModelDifference


def model_difference(
    current_model: Model, new_model: Model, static_resources: dict[str, AssetList | LabelDefinitionList] | None = None
) -> ModelDifference:
    if type(current_model) != type(new_model):
        raise ValueError(f"Cannot compare model of type {type(current_model)} with {type(new_model)}")
    # The dump and load calls are to remove all read only fields
    current_reloaded = current_model.load_from_cdf_resources(current_model.dump_as_cdf_resource(), link="external_id")
    new_reloaded = new_model.load_from_cdf_resources(new_model.dump_as_cdf_resource(), link="external_id")
    static_resources = {
        k: type(v)._load([remove_read_only_fields(resource.dump(camel_case=True)) for resource in v])
        for k, v in (static_resources or {}).items()
    }

    diffs = []
    for field_name in current_model.model_fields:
        current_value = getattr(current_reloaded, field_name)
        new_value = getattr(new_reloaded, field_name)
        if type(current_value) != type(new_value):
            raise ValueError(f"Cannot compare field {field_name} of type {type(current_value)} with {type(new_value)}")
        diff = _find_diffs(current_value, new_value, "Domain", field_name)
        diffs.append(diff)

    for function in current_model.cdf_resources:
        current_items = function(current_reloaded)
        new_items = function(new_reloaded)

        diff = _find_diffs(current_items, new_items, "CDF", function.__name__)

        diffs.append(diff)

    if isinstance(new_model, AssetModel) and static_resources:
        for field_name, static_resource in static_resources.items():
            new_resources = getattr(new_reloaded, field_name)()
            diff = _find_diffs(static_resource, new_resources, "CDF", field_name)
            if field_name == "labels":
                # Labels are not removed, only added
                diff.unchanged.extend(diff.removed)
                diff.removed = []
            diffs.append(diff)

    return ModelDifference(model_name=current_model.model_name, changes={d.field_name: d for d in diffs})


def remove_only(
    model: Model, static_resources: dict[str, LabelDefinitionList | AssetList] | None = None
) -> ModelDifference:
    # The dump and load calls are to remove all read only fields
    current_reloaded = model.load_from_cdf_resources(model.dump_as_cdf_resource())
    diffs = []
    for field_name in current_reloaded.model_fields:
        diff = _as_removals(getattr(current_reloaded, field_name), "Domain", field_name)
        diffs.append(diff)

    for function in current_reloaded.cdf_resources:
        diff = _as_removals(function(current_reloaded), "CDF", function.__name__)
        diffs.append(diff)

    for field_name, resources in (static_resources or {}).items():
        diff = _as_removals(resources, "CDF", field_name)
        diffs.append(diff)

    return ModelDifference(model_name=model.model_name, changes={d.field_name: d for d in diffs})


def _find_diffs(
    current_value: Any, new_value: Any, group: Literal["CDF", "Domain"], field_name: str
) -> FieldDifference:
    if not current_value and not new_value:
        return FieldDifference(group=group, field_name=field_name)

    current_value_by_id = _to_value_by_id(current_value)
    new_value_by_id = _to_value_by_id(new_value)

    added_ids = set(new_value_by_id.keys()) - set(current_value_by_id.keys())
    added = [new_value_by_id[external_id] for external_id in sorted(added_ids)]

    removed_ids = set(current_value_by_id.keys()) - set(new_value_by_id.keys())
    removed = [current_value_by_id[external_id] for external_id in sorted(removed_ids)]

    existing_ids = set(current_value_by_id.keys()) & set(new_value_by_id.keys())
    changed = []
    unchanged = []
    for existing in sorted(existing_ids):
        current = current_value_by_id[existing]
        new = new_value_by_id[existing]
        if _is_equals(current, new):
            unchanged.append(current)
        else:
            changed.append(Change(last=current, new=new))

    return FieldDifference(
        group=group, field_name=field_name, added=added, removed=removed, changed=changed, unchanged=unchanged
    )


def _is_equals(current: Any, new: Any) -> bool:
    if current == new:
        return True
    sentinel = (object(), object())
    for (k1, v1), (k2, v2) in zip_longest(vars(current).items(), vars(new).items(), fillvalue=sentinel):
        if k1 != k2:
            return False
        if v1 != v2 and (v1 and v2):
            return False
    return True


def _as_removals(value: Any, group: Literal["CDF", "Domain"], field_name: str) -> FieldDifference:
    if not value:
        return FieldDifference(group=group, field_name=field_name)

    value_by_id = _to_value_by_id(value)
    removed = [value_by_id[external_id] for external_id in sorted(value_by_id.keys())]
    return FieldDifference(group=group, field_name=field_name, removed=removed)


def _to_value_by_id(value: Any) -> dict[str, Any]:
    if (
        isinstance(value, (list, CogniteResourceList))
        and value
        and isinstance(value[0], (ResourceType, CogniteResource))
    ):
        return {item.external_id: item for item in value}
    elif isinstance(value, list) and value and isinstance(value[0], (CDFSequence, CDFFile)):
        return {item.external_id: item.cdf_resource for item in value}
    elif (
        isinstance(value, dict)
        and value
        and isinstance(next(iter(value.values())), (DomainModelApply, DomainModelApplyCogShop1))
    ):
        return {item.external_id: item for item in value.values()}
    elif isinstance(value, (dict, list, CogniteResourceList)) and not value:
        return {}
    elif (
        isinstance(value, list)
        and value
        and isinstance(value[0], (DomainModelApply, DomainModelApplyCogShop1, DomainModelApplyAssets))
    ):
        return {item.external_id: item for item in value}
    raise NotImplementedError(f"{type(value)} is not supported")
