from __future__ import annotations

from cognite.powerops.bootstrap.data_classes.shared import ExternalId


def clean_cdf_resources_for_diff(cdf_resources: dict[ExternalId, dict]) -> None:
    """Remove fields that are not relevant for diffing"""
    dynamic_fields = ["last_updated_time", "created_time", "parent_id", "root_id", "data_set_id", "id"]
    for resource in cdf_resources.values():
        for field in dynamic_fields:
            resource.pop(field, None)
        # remove the metadata if it is empty
        metadata = resource.get("metadata", {})
        if metadata == {}:
            resource.pop("metadata", None)
        else:
            for key, val in metadata.items():
                if isinstance(val, float):
                    # convert float to str
                    metadata[key] = str(val)

        # For sequences remove the column fields createdTime, lastUpdatedTime and metadata for each column
        if resource.get("columns"):
            for column in resource["columns"]:
                column.pop("createdTime", None)
                column.pop("lastUpdatedTime", None)
                column.pop("metadata", None)


def clean_local_resources_for_diff(local_resources: dict[ExternalId, dict]) -> None:
    for resource in local_resources.values():
        metadata = resource.get("metadata", {})
        for key, val in metadata.items():
            if isinstance(val, (float, int)):
                # convert float to str
                metadata[key] = str(val)
