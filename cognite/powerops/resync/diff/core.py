from typing import Any, Literal

from cognite.client.data_classes._base import CogniteResource, CogniteResourceList

from cognite.powerops.client._generated.cogshop1.data_classes._core import DomainModelApply as DomainModelApplyCogShop1
from cognite.powerops.client._generated.data_classes._core import DomainModelApply
from cognite.powerops.resync.models.base import CDFFile, CDFSequence, Model, ResourceType

from .data_classes import Change, FieldDifference, ModelDifference


def model_difference(current_model: Model, new_model: Model) -> ModelDifference:
    if type(current_model) != type(new_model):
        raise ValueError(f"Cannot compare model of type {type(current_model)} with {type(new_model)}")
    # The dump and load calls are to remove all read only fields
    current_reloaded = current_model.load_from_cdf_resources(current_model.dump_as_cdf_resource())
    new_reloaded = new_model.load_from_cdf_resources(new_model.dump_as_cdf_resource())

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

    return ModelDifference(model_name=current_model.model_name, changes={d.field_name: d for d in diffs})


def remove_only(model: Model) -> ModelDifference:
    # The dump and load calls are to remove all read only fields
    current_reloaded = model.load_from_cdf_resources(model.dump_as_cdf_resource())
    diffs = []
    for field_name in current_reloaded.model_fields:
        diff = _as_removals(getattr(current_reloaded, field_name), "Domain", field_name)
        diffs.append(diff)

    for function in current_reloaded.cdf_resources:
        diff = _as_removals(function(current_reloaded), "CDF", function.__name__)
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
        if current == new:
            unchanged.append(current)
        else:
            changed.append(Change(last=current, new=new))

    return FieldDifference(
        group=group, field_name=field_name, added=added, removed=removed, changed=changed, unchanged=unchanged
    )


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

    raise NotImplementedError(f"{type(value)} is not supported")
