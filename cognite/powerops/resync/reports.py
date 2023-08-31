from cognite.powerops.resync.models.base.model import FieldDifference
import pandas as pd
from dataclasses import asdict


def _exclude_keys(d: dict, keys: list[str]) -> dict:
    return {k: v for k, v in d.items() if k not in keys}


def as_markdown(changes_by_model: dict[str, list[FieldDifference]]) -> str:
    report = []
    for model_name, changes in changes_by_model.items():
        summaries = [change.as_summary() for change in changes]
        table = pd.DataFrame(
            [_exclude_keys(asdict(summary), keys=["group", "name"]) for summary in summaries if summary.group == "CDF"],
            index=[summary.name for summary in summaries if summary.group == "CDF"],
        )
        ids = [change.as_ids(limit=5) for change in changes if change.group == "CDF"]
        added_ids = []
        for id_ in ids:
            if id_.added:
                added_ids.append(f"#### Added {id_.name}\n{id_.added}")
        removed_ids = []
        for id_ in ids:
            if id_.removed:
                removed_ids.append(f"#### Removed {id_.name}\n{id_.removed}")
        changed_samples = []
        for change, id_ in zip((c for c in changes if c.group == "CDF"), ids):
            if change.changed:
                new_line = "\n * "
                changed_samples.append(
                    f"#### Changed {change.name}\n"
                    f"{new_line.join(f'{i}: {c.changed_fields}' for c, i in zip(change.changed[:3], id_.changed[:3]))}"
                )
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
            f"""## {model_name}

{table.T.to_markdown()}

**NOTE** Timeseries are not updated by `resync`

{samples}
"""
        )
    return "\n".join(report)
