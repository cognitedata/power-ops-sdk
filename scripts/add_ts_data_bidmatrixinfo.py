###this script adds time series data to the bid matrix information objects in the powerops client

from cognite.client.data_classes import TimeSeries
from cognite.client.exceptions import CogniteNotFoundError
from cognite.powerops import PowerOpsClient
import random
import datetime

power = PowerOpsClient.from_settings()  # Powerops SDK Client
cdf = power.cdf  # Cognite SDK Client


def main():
    exid_hourly = "emilie_ts_onhour"  # exid_hourly = "wim_ts_hourly"
    exid_empty = "wim_ts_empty"
    ts_list = [exid_hourly, exid_empty]

    now = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)
    start_time = int((now - datetime.timedelta(days=365)).timestamp() * 1000)
    end_time = int(now.timestamp() * 1000)

    # Checking if the time series already exists and adding them to the list existing_time_series if they do
    # ignore_unknown_ids=True will ignore any external_ids that don't exist in CDF. Remove this if you want to raise an error if the external_id doesn't exist
    try:
        existing_time_series = cdf.time_series.retrieve_multiple(external_ids=ts_list, ignore_unknown_ids=True)
        existing_time_series_ids = [ts.external_id for ts in existing_time_series]

    except CogniteNotFoundError as e:
        print(f"Error: {e}")
    # print(existing_time_series)

    # Adding the time series to a list, if they don't exist
    time_series_to_create = []
    if exid_hourly not in existing_time_series_ids:
        time_series_to_create.append(TimeSeries(name="emilie_ts_onhour", external_id=exid_hourly))

    if exid_empty not in existing_time_series_ids:
        time_series_to_create.append(TimeSeries(external_id=exid_empty))

    # Creating the time series that don't exist
    if time_series_to_create:
        cdf.time_series.create(time_series_to_create)

    # Insert data points for the hourly ts
    if exid_hourly not in existing_time_series_ids:
        data_points = [
            {"timestamp": start_time + i * 3600 * 1000, "value": random.random()}
            for i in range(int((end_time - start_time) / (3600 * 1000)))
        ]
        cdf.time_series.data.insert(datapoints=data_points, external_id=exid_hourly)

    def get_random_timeseries(list_of_time_series):
        # Choosing a random number between 1 and length of ts_list
        num_to_choose = random.randint(0, len(list_of_time_series))
        return random.sample(list_of_time_series, num_to_choose)

    print(get_random_timeseries(ts_list))

    # Adding the timeseries to the list of matrices to upsert
    upsertlist = []

    for matrix in power.v1.day_ahead_bid.bid_matrix_information.list(limit=10):
        matrix.alerts = None
        mat = matrix.as_write()
        mat.linked_time_series = get_random_timeseries(ts_list)
        upsertlist.append(mat)
        print(mat)

    power.v1.upsert(upsertlist, replace=True)
    # Printing the results:
    # res = power.v1.upsert(upsertlist, replace=True)
    # for r in res:
    #    print(r)

    # check one of the matrices:
    # print(power.v1.day_ahead_bid.bid_matrix_information.retrieve(external_id="total_bid_matrix_calculation_generated_8e412afb-a56c-4ca4-ac53-3c2b3fec37f7_4cf4_2023-5-1_8bd1_total_bid_matrix_6e32"))


if __name__ == "__main__":
    main()
