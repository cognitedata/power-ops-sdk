###this script adds time series data to the bid matrix information objects in the powerops client

from cognite.client.data_classes import TimeSeries, TimeSeriesList
from cognite.client.exceptions import CogniteNotFoundError
from cognite.client import CogniteClient
from cognite.powerops import PowerOpsClient
from cognite.powerops.client._generated.v1.data_classes import BidMatrixInformationWrite

from typing import Union
import random
import arrow


def create_time_series_objects() -> list[TimeSeries]:
    """
    Create a list of time series objects with predefined metadata.

    Returns:
        list[TimeSeries]: A list of TimeSeries objects.
    """

    ts_list = [
        TimeSeries(external_id="emilie_ts_onhour", name="emilie_ts_onhour", metadata={"data_type": "on_hour"}),
        TimeSeries(external_id="emilie_ts_empty", name="emilie_ts_empty", metadata={"data_type": "none"}),
        TimeSeries(external_id="emilie_ts_offhour", name="emilie_ts_offhour", metadata={"data_type": "off_hour"}),
    ]
    return ts_list


def get_existing_time_series(
    client: CogniteClient, ts_list: list[TimeSeries]
) -> tuple[list[TimeSeries], list[TimeSeries]]:
    """
    Fetches time series that exist in CDF based on a list of input time series to search for.

    Args:
        client (CogniteClient): An authenticated CDF client.
        ts_list (list[TimeSeries]): A list of time series to search for.

    Returns:
        existing_ts (list[TimeSeries]): A list of the time series that already exist in CDF based on the input list.
        missing_ts (list[TimeSeries]): A list of the time series that do not exist in CDF.
    """

    want_external_ids = {ts.external_id: ts for ts in ts_list}
    existing_ts_from_cdf = client.time_series.retrieve_multiple(
        external_ids=list(want_external_ids.keys()), ignore_unknown_ids=True
    )
    existing_ts_ext_ids = [ts.external_id for ts in existing_ts_from_cdf]

    existing_ts = [ts for ts in ts_list if ts.external_id in existing_ts_ext_ids]
    missing_ts = [ts for ts in ts_list if ts.external_id not in existing_ts_ext_ids]

    return existing_ts, missing_ts


def create_update_time_series(client: CogniteClient, to_be_updated: list[TimeSeries], missing_ts: list[TimeSeries]):
    """
    Creates or updates time series in CDF

    Args:
        client (CogniteClient): An authenticated CDF client.
        to_be_updated: list[TimeSeries]: A time series list holding the time series that should be updated.
        missing_ts: list[TimeSeries]: A time series list to be created from the missing time series (not in CDF)

    Returns:
        A list of the created and updated time series as list[TimeSeries]:
    """

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


def clean_data_points(client: CogniteClient, ts_list: list[TimeSeries]):
    """
    Cleans data points for a list of time series

    Args:
        client (CogniteClient): An authenticated CDF client.
        ts_list (list[TimeSeries]): A list of time series whose data points need to be cleaned/deleted.
    """

    ranges = [{"external_id": ts.external_id, "start": 0, "end": "now"} for ts in ts_list]
    client.time_series.data.delete_ranges(ranges)


def insert_data_points(client: CogniteClient, ts_list: list[TimeSeries]):
    """
    Inserts data points into time series according to the type of time series (defined in the metadata)

    Args:
        client (CogniteClient): An authenticated CDF client.
        ts_list (list[TimeSeries]): A list of time series to insert data points into.
    """

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

        # elif data_type == "none":
        #     print("something")

    # Insert the generated data points into CDF
    client.time_series.data.insert_multiple(dps_to_insert)


def get_random_item(list_of_items: list) -> Union[list, None]:  # Alternatively: Optional[list]
    """
    Gets a random number of items from a list

    Args:
        list_of_items (list): A list of items to choose from.

    Returns:
        Union[list, None]: A random number of items from the list, or None if the list is empty.
    """

    num_to_choose = random.randint(0, len(list_of_items))
    return random.sample(list_of_items, num_to_choose)


def link_ts_to_bid_matrix_info(power: PowerOpsClient, ts_list: list[TimeSeries]) -> list[BidMatrixInformationWrite]:
    """
    Links time series to bid matrix information objects.

    Args:
        power (PowerOpsClient): An authenticated PowerOps client.
        ts_list (list[TimeSeries]): A list of time series to be linked.

    Returns:
        list[BidMatrixInformationWrite]: A list of bid matrix information objects with linked time series.
    """
    upsert_list = []

    for matrix in power.v1.day_ahead_bid.bid_matrix_information.list(limit=5):
        print(f"before aswrite: {matrix}")
        matrix.alerts = None
        matrix_write = matrix.as_write()
        print(f"after aswrite: {matrix_write}")
        matrix_write.linked_time_series = get_random_item(ts_list)
        print(matrix_write.linked_time_series)
        upsert_list.append(matrix_write)
        # print(matrix_write)

    return upsert_list


def main():
    """
    Main function to execute the script.

    This function initializes the PowerOps and CDF clients, creates or updates time series,
    cleans existing data points, inserts new data points, and links the time series to bid matrix information objects using upsert.
    """

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
    power_client.v1.upsert(upsert_list, replace=False)


if __name__ == "__main__":
    main()
