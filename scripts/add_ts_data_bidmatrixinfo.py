###this script adds time series data to the bid matrix information objects in the powerops client

from typing import Union

from cognite.client.data_classes import TimeSeries, TimeSeriesList
from cognite.client.exceptions import CogniteNotFoundError
from cognite.client import CogniteClient
from cognite.powerops import PowerOpsClient

from cognite.powerops.client._generated.v1.data_classes import BidMatrixInformationWrite
import random
import arrow


def create_time_series_objects():
    ts_list = [
        TimeSeries(external_id="emilie_ts_onhour", name="emilie_ts_onhour", metadata={"data_type": "on_hour"}),
        TimeSeries(external_id="emilie_ts_empty", name="emilie_ts_empty", metadata={"data_type": "none"}),
        TimeSeries(external_id="emilie_ts_offhour", name="emilie_ts_offhour", metadata={"data_type": "off_hour"}),
    ]
    return ts_list


# Function to get existing time series from CDF
def get_existing_time_series(
    client: CogniteClient, ts_list: list[TimeSeries]
) -> tuple[list[TimeSeries], list[TimeSeries]]:
    want_external_ids = {ts.external_id: ts for ts in ts_list}

    existing_ts_from_cdf = client.time_series.retrieve_multiple(
        external_ids=list(want_external_ids.keys()), ignore_unknown_ids=True
    )
    existing_ts_ext_ids = [ts.external_id for ts in existing_ts_from_cdf]

    existing_ts = [ts for ts in ts_list if ts.external_id in existing_ts_ext_ids]
    missing_ts = [ts for ts in ts_list if ts.external_id not in existing_ts_ext_ids]

    return existing_ts, missing_ts


# Function to create or update time series in CDF
def create_update_time_series(client: CogniteClient, to_be_updated: list[TimeSeries], missing_ts: list[TimeSeries]):
    created = []
    if missing_ts:
        created = client.time_series.create(missing_ts)
        if isinstance(created, TimeSeries):
            created = [created]

    updated = []
    if to_be_updated:
        updated = client.time_series.update(to_be_updated)
        if not isinstance(updated, TimeSeriesList):
            updated = [updated]

    return created + updated


# Function to clean data points for a list of time series
def clean_data_points(client: CogniteClient, ts_list: list[TimeSeries]):
    ranges = [{"external_id": ts.external_id, "start": 0, "end": "now"} for ts in ts_list]
    client.time_series.data.delete_ranges(ranges)


# Function to insert data points into according to the type of time series (definined in the metadata)
def insert_data_points(client: CogniteClient, ts_list: list[TimeSeries]):
    now = arrow.now().floor("hour")
    start_time = now.shift(days=-365).timestamp() * 1000
    end_time = now.timestamp() * 1000

    dps_to_insert = []
    for ts in ts_list:
        data_type = ts.metadata.get("data_type", "none")

        if data_type == "on_hour":
            data_points = [
                {"timestamp": start_time + i * 3600 * 1000, "value": random.random()}
                for i in range(int((end_time - start_time) / (3600 * 1000)))
            ]
            dps_to_insert.append({"external_id": ts.external_id, "datapoints": data_points})

        elif data_type == "off_hour":
            data_points = [
                {"timestamp": start_time + i * 3600 * 1000 + 1800 * 1000, "value": random.random()}
                for i in range(int((end_time - start_time) / (3600 * 1000)))
            ]
            dps_to_insert.append({"external_id": ts.external_id, "datapoints": data_points})

        elif data_type == "none":
            print("bar")

    # Insert the generated data points into CDF
    client.time_series.data.insert_multiple(dps_to_insert)


# Function to get a random number of items from a list
def get_random_item(list_of_items: list) -> Union[list, None]:  # Alternatively: Optional[list]
    num_to_choose = random.randint(0, len(list_of_items))
    return random.sample(list_of_items, num_to_choose)


# Function to link time series to bid matrix information objects
def link_ts_to_bid_matrix_info(power: PowerOpsClient, ts_list: list[TimeSeries]) -> list[BidMatrixInformationWrite]:
    upsert_list = []

    for matrix in power.v1.day_ahead_bid.bid_matrix_information.list(limit=None):
        matrix.alerts = None
        matrix_write = matrix.as_write()
        matrix_write.linked_time_series = get_random_item(ts_list)
        print(matrix_write.linked_time_series)
        upsert_list.append(matrix_write)
        # print(matrix_write)

    return upsert_list


# Main function to execute the above functions and upsert the bid matrix information objects with the linked time series (either updated or created)
def main():
    power_client = PowerOpsClient.from_settings()
    cdf_client = power_client.cdf

    ts_list = create_time_series_objects()
    # for ts in ts_list:
    #     print(ts.external_id)
    #     print(ts.metadata)

    existing_time_series, missing_time_series = get_existing_time_series(cdf_client, ts_list)
    # print(existing_time_series)
    # for ts in missing_time_series:
    #     print(ts.external_id)

    all_ts = create_update_time_series(cdf_client, existing_time_series, missing_time_series)
    print(all_ts)

    clean_data_points(cdf_client, all_ts)

    insert_data_points(cdf_client, all_ts)

    upsert_list = link_ts_to_bid_matrix_info(power_client, ts_list)

    # Upsert the bid matrix information objects with the linked time series:
    power_client.v1.upsert(upsert_list, replace=True)


# Printing the results with the upsert (comment out the upsert above first):
# res = power.v1.upsert(upsertlist, replace=True)
# for r in res:
#    print(r)

# check one of the matrices:
# print(power.v1.day_ahead_bid.bid_matrix_information.retrieve(external_id="total_bid_matrix_calculation_generated_8e412afb-a56c-4ca4-ac53-3c2b3fec37f7_4cf4_2023-5-1_8bd1_total_bid_matrix_6e32"))


if __name__ == "__main__":
    main()
