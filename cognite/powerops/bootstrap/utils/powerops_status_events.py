import datetime
from uuid import uuid4

from cognite.client.data_classes import Event


def create_bootstrap_finished_event() -> Event:
    """Creating a POWEROPS_BOOTSTRAP_FINISHED Event in CDF to signal that bootstrap scripts have been ran"""
    current_time = int(datetime.datetime.utcnow().timestamp() * 1000)  # in milliseconds
    event = Event(
        start_time=current_time,
        end_time=current_time,
        external_id=f"POWEROPS_BOOTSTRAP_FINISHED_{str(uuid4())}",
        type="POWEROPS_BOOTSTRAP_FINISHED",
        subtype=None,
        source="PowerOps bootstrap",
        description="Manual run of bootstrap scripts finsihed",
    )
    print(f"Created status event '{event.external_id}'")

    return event
