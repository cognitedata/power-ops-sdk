from __future__ import annotations

import logging

from cognite.client import CogniteClient

from cognite.powerops.resync.diff import ModelDifference
from cognite.powerops.resync.models.base import AssetModel, Model  # type: ignore[attr-defined]

logger = logging.getLogger(__name__)


def _clean_relationships(client: CogniteClient, differences: ModelDifference, new_model: Model):
    if isinstance(new_model, AssetModel):
        not_create = _find_relationship_with_missing_time_series_target(client, new_model)
        relationship_diff = next((d for d in differences if d.field_name == "relationships"), None)
        if relationship_diff:
            relationship_diff.added = [r for r in relationship_diff.added if r.external_id not in not_create]
            relationship_diff.changed = [c for c in relationship_diff.changed if c.new.external_id not in not_create]


def _find_relationship_with_missing_time_series_target(client: CogniteClient, asset_model: AssetModel) -> set[str]:
    """Validates that all relationships in the collection have targets that exist in CDF"""
    time_series = asset_model.timeseries()
    # retrieve_multiple fails if no time series are provided
    if not time_series:
        return set()

    existing_time_series = client.time_series.retrieve_multiple(
        external_ids=list(set(time_series.as_external_ids())), ignore_unknown_ids=True
    )
    existing_timeseries_ids = {ts.external_id: ts for ts in existing_time_series}
    missing_timeseries = {t.external_id for t in time_series if t.external_id not in existing_timeseries_ids}

    relationships = asset_model.relationships()
    to_delete = {
        r.external_id
        for r in relationships
        if r.target_type
        and r.target_type.casefold() == "timeseries"
        and r.target_external_id in missing_timeseries
        and r.external_id
    }
    if to_delete:
        logger.warning(
            f"There are {len(to_delete)} relationships in {asset_model.model_name} that have targets "
            f"that do not exist in CDF. Examples {list(to_delete)[:3]}. These relationships will not be created.",
        )
    return to_delete
