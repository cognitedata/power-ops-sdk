"""File is mostly outdated, but kept around in since it has an overview of the plants, generators and their respective time series.
I believe that script was used to create time series in CDF, potentially populated them with random data.
This script is not in use, but kept around for reference."""

import os
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
from cognite.client import CogniteClient

# from cognite.client.data_classes import TimeSeriesWrite

REPO_ROOT = Path(__file__).parent.parent


def main():
    client = CogniteClient.default_oauth_client_credentials(
        project=os.environ["CDF_PROJECT"],
        cdf_cluster=os.environ["CDF_CLUSTER"],
        client_id=os.environ["IDP_CLIENT_ID"],
        client_secret=os.environ["IDP_CLIENT_SECRET"],
        tenant_id=os.environ["IDP_TENANT_ID"],
    )

    print(f"Connected to {client.config.project}")
    data_set_id = client.data_sets.retrieve(external_id="powerops:monitor").id

    plants_generators = {
        "Lund": ["Lund_G1"],
        "Dalby": ["Dalby_G1", "Dalby_G2"],
        "Holen": ["Holen_G1"],
        "Landet": ["Landet_G1"],
        "Lien_krv": ["Lien_krv_G1"],
        "Rull1": ["Rull1_G1", "Rull1_G2"],
        "Rull2": ["Rull2_G1", "Rull2_G2"],
        "Scott": ["Scott_G1"],
        "Strand_krv": ["Strand_krv_G1"],
    }

    plants_inlet = {
        "Lund": [1182, 1302],
        "Dalby": [1000, 1000],
        "Holen": [924, 950],
        "Landet": [975, 1020],
        "Lien_krv": [1182.5, 1183],
        "Rull1": [380.77, 380.77],
        "Rull2": [762, 762],
        "Scott": [1190, 1230.5],
        "Strand_krv": [893, 908],
    }

    start_date = "2026-01-01"
    end_date = datetime.now().strftime("%Y-%m-%d")

    for plant, min_max in plants_inlet.items():
        plant_name_prefix = f"{plant}_".lower()
        # for generator in generators:
        #     generator_name_prefix = f"{plant_name_prefix}{generator}_".lower()
        #     start_stop_cost_ts = client.time_series.create(TimeSeriesWrite(external_id=f"{generator_name_prefix}start_stop_cost", data_set_id=data_set_id))
        #     availability_time_series = client.time_series.create(TimeSeriesWrite(external_id=f"{generator_name_prefix}availability", data_set_id=data_set_id))

        # water_value_time_series = client.time_series.create(TimeSeriesWrite(external_id=f"{plant_name_prefix}water_value", data_set_id=data_set_id))
        # feeding_fee_time_series = client.time_series.create(TimeSeriesWrite(external_id=f"{plant_name_prefix}feeding_fee", data_set_id=data_set_id))
        # production_max_time_series = client.time_series.create(TimeSeriesWrite(external_id=f"{plant_name_prefix}production_max", data_set_id=data_set_id))
        # production_min_time_series = client.time_series.create(TimeSeriesWrite(external_id=f"{plant_name_prefix}production_min", data_set_id=data_set_id))
        # outlet_level_time_series = client.time_series.create(TimeSeriesWrite(external_id=f"{plant_name_prefix}outlet_level", data_set_id=data_set_id))
        # inlet_level_time_series = client.time_series.create(
        #     TimeSeriesWrite(external_id=f"inlet_level_{plant}", data_set_id=data_set_id)
        # )
        min_value = min_max[0]
        max_value = min_max[1]

        timestamps = pd.date_range(start=start_date, end=end_date, freq="h")
        values = np.random.uniform(low=min_value, high=max_value, size=len(timestamps))

        df = pd.DataFrame({f"inlet_level_{plant}": values}, index=timestamps)
        client.time_series.data.insert_dataframe(df)
        # head_direct_time_series = client.time_series.create(TimeSeriesWrite(external_id=f"{plant_name_prefix}head_direct", data_set_id=data_set_id))


if __name__ == "__main__":
    main()
