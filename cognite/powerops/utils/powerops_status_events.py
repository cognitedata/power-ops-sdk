from uuid import uuid4

import arrow
from cognite.client.data_classes import Event


def create_bootstrap_finished_event() -> Event:
    """Creating a POWEROPS_BOOTSTRAP_FINISHED Event in CDF to signal that bootstrap scripts have been ran"""
    current_time = arrow.utcnow().int_timestamp * 1000  # timestamp in millis
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
