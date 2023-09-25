from __future__ import annotations

import itertools
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from itertools import islice
from typing import Literal

import pandas as pd
from cognite.client.data_classes import FileMetadata, Sequence
from cognite.client.data_classes._base import CogniteResource
from cognite.client.data_classes.data_modeling.ids import AbstractDataclass
from cognite.client.data_classes.data_modeling.instances import InstanceCore
from deepdiff import DeepDiff  # type: ignore[import]
from typing_extensions import TypeAlias

from cognite.powerops.resync.models.base.cdf_resources import CDFResource
from cognite.powerops.resync.models.base.resource_type import Resource

Group: TypeAlias = Literal["CDF", "Domain"]


@dataclass
class Change:
    last: Resource
    new: Resource

    @property
    def changed_fields(self) -> str:
        last_dumped = (
            self.last.dump(camel_case=True)
            if isinstance(self.last, (CogniteResource, AbstractDataclass))
            else self.last.model_dump()
        )
        new_dumped = (
            self.new.dump(camel_case=True)
            if isinstance(self.new, (CogniteResource, AbstractDataclass))
            else self.new.model_dump()
        )
        return (
            DeepDiff(last_dumped, new_dumped, ignore_string_case=True)
            .pretty()
            .replace("added to dictionary.", f"will be added to {self.last.external_id}.")
            .replace("removed from dictionary.", f"will be removed from {self.last.external_id}.")
        )

    @property
    def is_changed_content(self) -> bool:
        if isinstance(self.last, (Sequence, FileMetadata)) and isinstance(self.new, (Sequence, FileMetadata)):
            last_hash = self.last.metadata.get(CDFResource.content_key_hash) if self.last.metadata else None
            new_hash = self.new.metadata.get(CDFResource.content_key_hash) if self.new.metadata else None
            return last_hash != new_hash or last_hash is None or new_hash is None
        return False


@dataclass
class FieldSummary:
    group: Group
    field_name: str
    added: int
    removed: int
    changed: int
    unchanged: int

    @property
    def total(self) -> int:
        return self.added + self.removed + self.changed + self.unchanged


@dataclass
class FieldIds:
    group: Group
    field_name: str
    added: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    changed: list[str] = field(default_factory=list)
    unchanged: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.added) + len(self.removed) + len(self.changed) + len(self.unchanged)


@dataclass
class FieldDifference:
    group: Group
    field_name: str
    added: list[Resource] = field(default_factory=list)
    removed: list[Resource] = field(default_factory=list)
    changed: list[Change] = field(default_factory=list)
    unchanged: list[Resource] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.added) + len(self.removed) + len(self.changed) + len(self.unchanged)

    @property
    def removed_ids(self) -> list[str]:
        output = []
        for item in self.removed:
            if isinstance(item, InstanceCore):
                output.append(item.as_id())  # type: ignore[union-attr]
            elif isinstance(item, AbstractDataclass):
                output.append(item)
            elif hasattr(item, "external_id"):
                output.append(item.external_id)
            else:
                raise NotImplementedError(f"Could not find identifier of {item}")
        return output

    def __add__(self, other: FieldDifference) -> FieldDifference:
        if self.field_name != other.field_name or self.group != other.group or not isinstance(other, FieldDifference):
            return NotImplemented
        return FieldDifference(
            group=self.group,
            field_name=self.field_name,
            added=self.added + other.added,
            removed=self.removed + other.removed,
            changed=self.changed + other.changed,
            unchanged=self.unchanged + other.unchanged,
        )

    def as_summary(self) -> FieldSummary:
        return FieldSummary(
            group=self.group,
            field_name=self.field_name,
            added=len(self.added),
            removed=len(self.removed),
            changed=len(self.changed),
            unchanged=len(self.unchanged),
        )

    def as_ids(self, limit: int = -1) -> FieldIds:
        def get_identifier(item: Resource) -> str:
            if hasattr(item, "external_id"):
                return item.external_id or "missing external_id"
            elif hasattr(item, "name"):
                return item.name or "missing name"
            raise NotImplementedError(f"Could not find external_id or name in {item}")

        limit_ = limit if limit > 0 else None
        return FieldIds(
            group=self.group,
            field_name=self.field_name,
            added=[get_identifier(item) for item in islice(self.added, limit_)],
            removed=[get_identifier(item) for item in islice(self.removed, limit_)],
            changed=[get_identifier(item.last) for item in islice(self.changed, limit_)],
            unchanged=[get_identifier(item) for item in islice(self.unchanged, limit_)],
        )

    def set_set_dataset(self, dataset: int) -> None:
        for item in itertools.chain(self.added, self.removed, self.unchanged):
            if hasattr(item, "data_set_id"):
                item.data_set_id = dataset
        for item in self.changed:
            if hasattr(item.last, "data_set_id"):
                item.last.data_set_id = dataset
            if hasattr(item.new, "data_set_id"):
                item.new.data_set_id = dataset


@dataclass
class ModelDifferenceSummary:
    model_name: str
    changes: list[FieldSummary]


@dataclass
class ModelDifference:
    model_name: str
    changes: dict[str, FieldDifference] = field(default_factory=dict)

    def __iter__(self) -> Iterator[FieldDifference]:
        return iter(self.changes.values())

    def __contains__(self, item):
        return item in self.changes

    def __getitem__(self, item):
        return self.changes[item]

    def __setitem__(self, key, value):
        self.changes[key] = value

    def __add__(self, other: ModelDifference) -> ModelDifference:
        if self.model_name != other.model_name or not isinstance(other, ModelDifference):
            return NotImplemented
        changes = {}
        for field_name in set(self.changes.keys()) | set(other.changes.keys()):
            if field_name not in self.changes:
                changes[field_name] = other.changes[field_name]
            elif field_name not in other.changes:
                changes[field_name] = self.changes[field_name]
            else:
                changes[field_name] = self.changes[field_name] + other.changes[field_name]
        return ModelDifference(model_name=self.model_name, changes=changes)

    def has_changes(self, group: str, exclude: set | frozenset = frozenset({"timeseries"})) -> bool:
        return any(
            change.total != len(change.unchanged)
            for change in self.changes.values()
            if change.field_name not in exclude and change.group == group
        )

    def filter_out(self, group: Group | None = None, field_names: set | frozenset = frozenset({"timeseries"})) -> bool:
        """
        Removes the field differences which match the filter criteria.

        Args:
            group: The name of the group to remove
            field_names: The field names to remove

        Returns:
            True if any field differences were removed, False otherwise
        """
        has_removed = False
        for field_name in list(self.changes.keys()):
            if field_name in field_names or self.changes[field_name].group == group:
                del self.changes[field_name]
                has_removed = True
        return has_removed

    def as_summary(self) -> ModelDifferenceSummary:
        return ModelDifferenceSummary(
            model_name=self.model_name,
            changes=[change.as_summary() for _, change in sorted(self.changes.items(), key=lambda k: k[0])],
        )


@dataclass
class ModelDifferences:
    models: list[ModelDifference]

    def append(self, model: ModelDifference) -> None:
        self.models.append(model)

    def has_changes(self, group: str, exclude: set | frozenset = frozenset({"timeseries", "labels"})) -> bool:
        return any(m.has_changes(group, exclude) for m in self.models)

    def as_markdown_detailed(self) -> str:
        report = []
        for model in self.models:
            summaries = [change.as_summary() for change in model]
            table = pd.DataFrame(
                [
                    _exclude_keys(asdict(summary), keys=["group", "name"])
                    for summary in summaries
                    if summary.group == "CDF"
                ],
                index=[summary.field_name for summary in summaries if summary.group == "CDF"],
            )
            ids = [change.as_ids(limit=5) for change in model if change.group == "CDF"]
            added_ids = []
            for id_ in ids:
                if id_.added:
                    added_ids.append(f"#### Added {id_.field_name}\n{id_.added}")
            removed_ids = []
            for id_ in ids:
                if id_.removed:
                    removed_ids.append(f"#### Removed {id_.field_name}\n{id_.removed}")
            changed_samples = []
            for change, id_ in zip((c for c in model if c.group == "CDF"), ids):
                if change.changed:
                    new_line = "\n * "
                    sample_line = new_line.join(
                        f"{i}: {c.changed_fields}" for c, i in zip(change.changed[:3], id_.changed[:3])
                    )
                    changed_samples.append(f"#### Changed {change.field_name}\n{sample_line}")
            samples = []
            if added_ids:
                samples.append("\n".join(added_ids))
            if removed_ids:
                samples.append("\n".join(removed_ids))
            if changed_samples:
                samples.append("\n".join(changed_samples))
            if samples:
                samples.insert(0, "### Samples")
            samples = "\n".join(samples)

            report.append(
                f"""## {model.model_name}

{table.T.to_markdown()}

**NOTE** Timeseries are not updated by `resync`

{samples}
    """
            )
        return "\n".join(report)

    def as_markdown_summary(self, no_headers: bool = False, skip_domain: bool = True) -> str:
        by_name = defaultdict(list)
        for model in self.models:
            for change in model:
                if skip_domain and change.group == "Domain":
                    continue
                by_name[change.field_name].append(change)
        report = []
        total_added = 0
        total_removed = 0
        total_changed = 0
        for name, changes in by_name.items():
            added = sum(len(change.added) for change in changes)
            removed = sum(len(change.removed) for change in changes)
            change_count = sum(len(change.changed) for change in changes)
            counts = []
            for count_name, count in zip(("added", "removed", "changed"), (added, removed, change_count)):
                if count > 0:
                    counts.append(f"**{count} {count_name}**")
                else:
                    counts.append(f"{count} {count_name}")
            counts_str = ", ".join(counts)
            report.append(f"* **{name}**: {counts_str}")
            total_added += added
            total_removed += removed
            total_changed += change_count

        list_ = "\n".join(report)
        totals = f"{total_added} added, {total_removed} removed, {total_changed} changed"
        if no_headers:
            top_row = f"**Summary:** {totals}"
        else:
            top_row = f"## Summary: {totals}"
        return f"""{top_row}
{list_ if list_ else 'No changes'}
"""

    def as_github_markdown(self) -> str:
        summary = self.as_markdown_summary()
        detailed = self.as_markdown_detailed()
        return f"""{summary}
<details>
<summary>Details</summary>

{detailed}
</details>
"""


def _exclude_keys(d: dict, keys: list[str]) -> dict:
    return {k: v for k, v in d.items() if k not in keys}
