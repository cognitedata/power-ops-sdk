###this script adds time series data to the bid matrix information objects in the powerops client

from cognite.client.data_classes import TimeSeries
from cognite.client.exceptions import CogniteNotFoundError
from cognite.powerops import PowerOpsClient
import random
import datetime

power = PowerOpsClient.from_settings()
cdf = power.cdf

exid_hourly = "emilies_points"  # exid_hourly = "wim_ts_hourly"
exid_empty = "wim_ts_empty"
tsList = [exid_hourly, exid_empty]

start_time = int((datetime.datetime.now() - datetime.timedelta(days=365)).timestamp() * 1000)
end_time = int(datetime.datetime.now().timestamp() * 1000)

# Checking if the time series already exists and adding them to the list existing_time_series if they do
# ignore_unknown_ids=True will ignore any external_ids that don't exist in CDF. Remove this if you want to raise an error if the external_id doesn't exist
try:
    existing_time_series = cdf.time_series.retrieve_multiple(external_ids=tsList, ignore_unknown_ids=True)
    existing_time_series_ids = []
    for ts in existing_time_series:
        existing_time_series_ids.append(ts.external_id)

except CogniteNotFoundError as e:
    print(f"Error: {e}")
# print(existing_time_series)


# Adding the time series to a list, if they don't exist
time_series_to_create = []
if exid_hourly not in existing_time_series_ids:
    time_series_to_create.append(TimeSeries(name="emilies_points", external_id=exid_hourly))

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
    # Choosing a random number between 1 and length of tsList
    num_to_choose = random.randint(1, len(list_of_time_series))
    return random.sample(list_of_time_series, num_to_choose)


# print(get_random_timeseries())
# print(type(random.sample(tsList, 2)))


# Adding the timeseries to the list of matrices to upsert
upsertlist = []

for matrix in power.v1.day_ahead_bid.bid_matrix_information.list():
    matrix.alerts = None
    exid = matrix.external_id  # This line can be removed once the .as_write() bug is fixed
    mat = matrix.as_write()
    # Keeping the exid from before .as_write, as there's a bug in the .as_write() that was overwriting the external_id:
    mat.external_id = exid  # This line can be removed once the .as_write() bug is fixed
    mat.linked_time_series = get_random_timeseries(tsList)
    upsertlist.append(mat)
    # print(mat)


# Printing the results:
res = power.v1.upsert(upsertlist, replace=True)
for r in res:
    print(r)


# check one of the matrices:
# print(power.v1.day_ahead_bid.bid_matrix_information.retrieve(external_id="total_bid_matrix_calculation_generated_8e412afb-a56c-4ca4-ac53-3c2b3fec37f7_4cf4_2023-5-1_8bd1_total_bid_matrix_6e32"))
