import json

import pandas as pd
from dotenv import dotenv_values
from yaml import safe_load

from cognite.powerops.preprocessor.utils import get_cdf_client


def make_base_mapping(row) -> dict:
    json_str = "".join(
        (
            row["transformations"],
            row["transformations1"],
            row["transformations2"],
            row["transformations3"],
        )
    )
    transformations = json.loads(json_str)

    return {
        "path": row["shop_model_path"],
        "timeseries_external_id": row["time_series_external_id"],
        "retrieve": row["retrieve"],
        "aggregation": row["aggregation"],
        "transformations": {
            "items": [
                {
                    "method": t["transformation"],
                    "arguments": json.dumps(t["kwargs"]) if t["kwargs"] is not None else None,
                }
                for t in transformations
            ]
        },
    }


env = dotenv_values(".env.heco-dev")

client = get_cdf_client(
    project=env["cdf_project"],
    tenant_id=env["cdf_tenant_id"],
    client_id=env["cdf_client_id"],
    client_secret=env["cdf_client_secret"],
    cluster=env["cdf_cluster"],
)


sequence_xid = "SHOP_Otta_base_mapping"
base_mapping = client.sequences.data.retrieve_dataframe(external_id=sequence_xid, limit=10000, start=0, end=-1)
base_mapping.where(pd.notnull(base_mapping), None, inplace=True)

mappings = []
for i, row in base_mapping.iterrows():
    mappings.append(make_base_mapping(row))


commands_file = client.files.download_bytes(external_id="SHOP_Glomma_commands")


# POWEROPS_SHOP_RUN_1b424f47-d893-425c-95e2-ed5b44c047c6

case = {
    "start_time": "2023-03-28 22:00:00",
    "end_time": "2023-04-09 22:00:00",
    "scenario": {
        "name": "Otta-Base",
        "commands": {"commands": safe_load(commands_file)["commands"]},
        "model_template": {
            "watercourse": "Otta",
            "model": {"file_external_id": "SHOP_Otta_model"},
            "base_mappings": {"items": mappings},
        },
    },
}
