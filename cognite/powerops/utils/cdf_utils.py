from cognite.client import ClientConfig, CogniteClient
from cognite.client.data_classes import Asset, Event, FileMetadata, Relationship, Sequence, TimeSeries
from typing import Union

#from dm.client import PowerOpsClient, get_power_ops_client


def simple_relationship(
    source: Union[Asset, TimeSeries, FileMetadata, Sequence, Event],
    target: Union[Asset, TimeSeries, FileMetadata, Sequence, Event],
    label_external_id: str,
) -> Relationship:
    """Simplifies Cognite Python SDK creation of Relationships."""
    external_id = f"{source.external_id}.{target.external_id}"
    source_type = "file" if isinstance(source, FileMetadata) else source.__class__.__name__
    target_type = "file" if isinstance(target, FileMetadata) else target.__class__.__name__

    return Relationship(
        external_id=external_id,
        source_type=source_type,
        target_type=target_type,
        source_external_id=source.external_id,
        target_external_id=target.external_id,
        labels=[label_external_id],
    )


class PowerOpsCogniteClient(CogniteClient):
    dm: PowerOpsClient

    def __init__(self, config: ClientConfig, space_id: str, data_model: str, schema_version: int):
        super().__init__(config)
        self.dm = get_power_ops_client(
            config=config,
            space_external_id=space_id,
            data_model=data_model,
            schema_version=schema_version,
        )
