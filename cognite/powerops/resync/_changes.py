from __future__ import annotations

import itertools
from dataclasses import dataclass, field, asdict
from itertools import islice
from typing import Literal

import pandas as pd
from cognite.client.data_classes import Sequence, FileMetadata
from cognite.client.data_classes._base import CogniteResource
from deepdiff import DeepDiff  # type: ignore[import]

from cognite.powerops.resync.models.base.resource_type import Resource
from cognite.powerops.resync.models.cdf_resources import CDFResource


@dataclass
class Change:
    last: Resource
    new: Resource

    @property
    def changed_fields(self) -> str:
        last_dumped = (
            self.last.dump(camel_case=True) if isinstance(self.last, CogniteResource) else self.last.model_dump()
        )
        new_dumped = self.new.dump(camel_case=True) if isinstance(self.new, CogniteResource) else self.new.model_dump()
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
    group: Literal["CDF", "Domain"]
    name: str
    added: int
    removed: int
    changed: int
    unchanged: int

    @property
    def total(self) -> int:
        return self.added + self.removed + self.changed + self.unchanged


@dataclass
class FieldIds:
    group: Literal["CDF", "Domain"]
    name: str
    added: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    changed: list[str] = field(default_factory=list)
    unchanged: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.added) + len(self.removed) + len(self.changed) + len(self.unchanged)


@dataclass
class FieldDifference:
    group: Literal["CDF", "Domain"]
    name: str
    added: list[Resource] = field(default_factory=list)
    removed: list[Resource] = field(default_factory=list)
    changed: list[Change] = field(default_factory=list)
    unchanged: list[Resource] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.added) + len(self.removed) + len(self.changed) + len(self.unchanged)

    def as_summary(self) -> FieldSummary:
        return FieldSummary(
            group=self.group,
            name=self.name,
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
            name=self.name,
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
                item.new.data_set_id = dataset


@dataclass
class ModelDifference:
    name: str
    changes: list[FieldDifference]

    def has_changes(self, exclude: set | frozenset = frozenset({"timeseries"})) -> bool:
        return any(change.total != change.unchanged for change in self.changes if change.name not in exclude)


@dataclass
class ModelDifferences:
    models: list[ModelDifference]

    def has_changes(self, exclude: set | frozenset = frozenset({"timeseries"})) -> bool:
        return any(m.has_changes(exclude) for m in self.models)

    def as_markdown(self) -> str:
        report = []
        for model in self.models:
            summaries = [change.as_summary() for change in model.changes]
            table = pd.DataFrame(
                [
                    _exclude_keys(asdict(summary), keys=["group", "name"])
                    for summary in summaries
                    if summary.group == "CDF"
                ],
                index=[summary.name for summary in summaries if summary.group == "CDF"],
            )
            ids = [change.as_ids(limit=5) for change in model.changes if change.group == "CDF"]
            added_ids = []
            for id_ in ids:
                if id_.added:
                    added_ids.append(f"#### Added {id_.name}\n{id_.added}")
            removed_ids = []
            for id_ in ids:
                if id_.removed:
                    removed_ids.append(f"#### Removed {id_.name}\n{id_.removed}")
            changed_samples = []
            for change, id_ in zip((c for c in model.changes if c.group == "CDF"), ids):
                if change.changed:
                    new_line = "\n * "
                    sample_line = new_line.join(
                        f"{i}: {c.changed_fields}" for c, i in zip(change.changed[:3], id_.changed[:3])
                    )
                    changed_samples.append(f"#### Changed {change.name}\n{sample_line}")
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
                f"""## {model.name}

{table.T.to_markdown()}

**NOTE** Timeseries are not updated by `resync`

{samples}
    """
            )
        return "\n".join(report)


def _exclude_keys(d: dict, keys: list[str]) -> dict:
    return {k: v for k, v in d.items() if k not in keys}
