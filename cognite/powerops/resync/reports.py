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
        report.append(
            f"""## {model_name}
{table.T.to_markdown()}
"""
        )
    return "\n".join(report)
