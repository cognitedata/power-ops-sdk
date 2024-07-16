###this script adds time series data to the bid matrix information objects in the powerops client

from cognite.client.data_classes import TimeSeries
from cognite.powerops import PowerOpsClient
import random
import datetime


power = PowerOpsClient.from_settings()
cdf = power.cdf

start_time = int((datetime.datetime.now() - datetime.timedelta(days=365)).timestamp() * 1000)
end_time = int(datetime.datetime.now().timestamp() * 1000)

# creating time series, with data points every hour
if not cdf.time_series.retrieve(external_id="wim_ts_hourly"):
    wim_ts_hourly = cdf.time_series.create(TimeSeries(name="wim_ts_hourly", external_id="wim_ts_hourly"))
    data_points = [
        {"timestamp": start_time + i * 3600 * 1000, "value": random.random()}
        for i in range(int((end_time - start_time) / (3600 * 1000)))
    ]
    cdf.time_series.data.insert(datapoints=data_points, external_id="wim_ts_hourly")

# creating an empty time series, without a name to check the external_id default
if not cdf.time_series.retrieve(external_id="wim_ts_empty"):
    wim_ts_empty = cdf.time_series.create(TimeSeries(external_id="wim_ts_empty"))

tsList = ["wim_ts_hourly", "wim_ts_empty"]


def get_random_timeseries():
    choice = random.choice([1, 2, 3])
    if choice == 1:
        return [random.choice(tsList)]  # Velg en tidsserie
    elif choice == 2:
        return random.sample(tsList, 2)  # Velg to tidsserier
    else:
        return tsList


# Adding the timeseries to the list of matrices to upsert
upsertlist = []

for matrix in power.v1.day_ahead_bid.bid_matrix_information.list():
    matrix.alerts = None
    exid = matrix.external_id
    mat = matrix.as_write()
    mat.external_id = exid
    mat.linked_time_series = get_random_timeseries()
    upsertlist.append(mat)

# printing the matrices:
# for matrix in upsertlist:
#     print(matrix)

power.v1.upsert(upsertlist, replace=True)

# check one of the matrices:
# print(power.v1.day_ahead_bid.bid_matrix_information.retrieve(external_id="total_bid_matrix_calculation_generated_8e412afb-a56c-4ca4-ac53-3c2b3fec37f7_4cf4_2023-5-1_8bd1_total_bid_matrix_6e32"))
