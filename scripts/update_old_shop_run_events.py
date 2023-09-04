from datetime import datetime
from cognite.powerops.clients.powerops_client import get_powerops_client
from cognite.client.utils._time import datetime_to_ms
from cognite.client.data_classes import Event


STRING_FORMAT_METADATA = "%Y-%m-%d %H:%M:%S"
EVENT_TYPE = "POWEROPS_PROCESS_REQUESTED"
EVENT_SUBTYPE = "POWEROPS_SHOP_RUN"

LIST_LIMIT = 5000


def str_time_to_ms(str_time: str) -> int:
    return datetime_to_ms(datetime.strptime(str_time, STRING_FORMAT_METADATA))


def convert_event(event: Event):
    shop_starttime = event.metadata["shop:starttime"]
    shop_endtime = event.metadata["shop:endtime"]
    event.start_time = str_time_to_ms(shop_starttime)
    event.end_time = str_time_to_ms(shop_endtime)
    event.subtype = EVENT_SUBTYPE
    return event


def main():
    client = get_powerops_client().cdf

    events_to_convert = filter(
        lambda e: e.subtype is None,
        client.events.list(type=EVENT_TYPE, metadata={"process_type": EVENT_SUBTYPE}, limit=LIST_LIMIT),
    )
    converted = tuple(map(convert_event, events_to_convert))
    print(f"Converted {len(converted)} events")
    if len(converted) == LIST_LIMIT:
        print(f"WARNING: The limit of {LIST_LIMIT} events was reached. There might be more events to convert.")

    answer = input("Do you wish to write back the conversion (y/n)\n")
    if not answer.casefold() == "y":
        print("Aborting.")
        return

    client.events.update(converted)
    print("Done")


if __name__ == "__main__":
    main()
