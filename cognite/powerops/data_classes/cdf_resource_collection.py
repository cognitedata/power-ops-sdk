from __future__ import annotations

from typing import Union

import pandas as pd
from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, Event, LabelDefinition, Relationship, Sequence
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
from deepdiff import DeepDiff
from pydantic import BaseModel, Extra

import cognite.powerops.logger as logger
from cognite.powerops.data_classes.shop_file_config import ShopFileConfig
from cognite.powerops.utils.cdf_utils import upsert_cognite_resources
from cognite.powerops.utils.files import upload_shop_config_file

custom_logger = logger.get_logger(__name__)


ExternalId = str

AddableResourceT = Union["CogniteResource", list["CogniteResource"], "SequenceContent", list[ShopFileConfig]]


class SequenceRows(BaseModel):
    rows: list[tuple[int, list[str]]]
    columns_external_ids: list[str]

    def to_pandas(self) -> pd.DataFrame:
        return pd.DataFrame(
            data=[row[1] for row in self.rows],
            index=[row[0] for row in self.rows],
            columns=self.columns_external_ids,
        )


class SequenceContent(BaseModel):
    sequence_external_id: str
    data: Union[pd.DataFrame, SequenceRows]

    class Config:
        arbitrary_types_allowed = True

    def dump(self) -> dict:
        return {
            self.sequence_external_id: self.data.to_dict()
            if isinstance(self.data, pd.DataFrame)
            else self.data.to_pandas().to_dict()
        }


def clean_cdf_resources_for_diff(cdf_resources: dict[ExternalId, dict]) -> None:
    """Remove fields that are not relevant for diffing"""
    dynamic_fields = ["last_updated_time", "created_time", "parent_id", "root_id", "data_set_id", "id"]
    for resource in cdf_resources.values():
        for field in dynamic_fields:
            resource.pop(field, None)
        # remove the metadata if it is empty
        metadata = resource.get("metadata", {})
        if metadata == {}:
            resource.pop("metadata", None)
        else:
            for key, val in metadata.items():
                if isinstance(val, float):
                    # convert float to str
                    metadata[key] = str(val)

        # For sequences remove the column fields createdTime, lastUpdatedTime and metadata for each column
        if resource.get("columns"):
            for column in resource["columns"]:
                column.pop("createdTime", None)
                column.pop("lastUpdatedTime", None)
                column.pop("metadata", None)


def clean_local_resources_for_diff(local_resources: dict[ExternalId, dict]) -> None:
    for resource in local_resources.values():
        metadata = resource.get("metadata", {})
        for key, val in metadata.items():
            if isinstance(val, (float, int)):
                # convert float to str
                metadata[key] = str(val)


class BootstrapResourceCollection(BaseModel):
    # CDF resources
    assets: dict[ExternalId, Asset] = {}
    relationships: dict[ExternalId, Relationship] = {}
    sequences: dict[ExternalId, Sequence] = {}
    label_definitions: dict[ExternalId, LabelDefinition] = {}
    events: dict[ExternalId, Event] = {}

    # other resources
    sequence_content: dict[ExternalId, SequenceContent] = {}
    shop_file_configs: dict[ExternalId, ShopFileConfig] = {}

    class Config:
        arbitrary_types_allowed = True
        extra = Extra.forbid

    @property
    def all_cdf_resources(self) -> list[CogniteResource]:
        return (
            list(self.assets.values())
            + list(self.relationships.values())
            + list(self.sequences.values())
            + list(self.label_definitions.values())
            + list(self.events.values())
        )

    def add(self, resources_to_append: AddableResourceT):
        # sort the resource by type and append to the correct list
        if isinstance(resources_to_append, (list, CogniteResourceList)):
            for resource in resources_to_append:
                self._add_resource(resource)
        else:
            self._add_resource(resources_to_append)

    def _add_resource(self, resource: CogniteResource | SequenceContent | ShopFileConfig):
        if isinstance(resource, Asset):
            self.assets[resource.external_id] = resource
        elif isinstance(resource, Sequence):
            self.sequences[resource.external_id] = resource
        elif isinstance(resource, Relationship):
            self.relationships[resource.external_id] = resource
        elif isinstance(resource, LabelDefinition):
            self.label_definitions[resource.external_id] = resource
        elif isinstance(resource, Event):
            self.events[resource.external_id] = resource
        elif isinstance(resource, SequenceContent):
            self.sequence_content[resource.sequence_external_id] = resource
        elif isinstance(resource, ShopFileConfig):
            self.shop_file_configs[resource.external_id] = resource
        else:
            raise ValueError(f"Unknown resource type: {type(resource)}")

    def write_to_cdf(
        self,
        client: CogniteClient,
        data_set_external_id: str,
        overwrite: bool = False,
        skip_dm: bool = False,
    ):
        # CDF auth and data set
        data_set_id = client.data_sets.retrieve(external_id=data_set_external_id).id
        for resource in self.all_cdf_resources:
            resource.data_set_id = data_set_id

        api_by_type = {
            "label_definitions": client.labels,
            "assets": client.assets,
            "sequences": client.sequences,
            "relationships": client.relationships,
            "events": client.events,
        }
        for resource_type, api in api_by_type.items():
            resources = list(getattr(self, resource_type).values())
            custom_logger.debug(f"Processing {len(resources)} {resource_type}...")
            upsert_cognite_resources(api, resource_type, resources)

        custom_logger.debug(f"Processing {len(self.sequence_content)} sequences...")
        for sequence_external_id, sequence_data in self.sequence_content.items():
            if isinstance(sequence_data.data, pd.DataFrame):
                try:
                    client.sequences.data.insert_dataframe(
                        dataframe=sequence_data.data, external_id=sequence_external_id
                    )
                except Exception as e:
                    custom_logger.warning(f"Failed to insert sequence dataframe for {sequence_external_id}: {e}")
            else:
                try:
                    client.sequences.data.insert(
                        rows=sequence_data.data.rows,
                        external_id=sequence_external_id,
                        column_external_ids=sequence_data.data.columns_external_ids,
                    )
                except Exception as e:
                    custom_logger.warning(f"Failed to insert sequence rows for {sequence_external_id}: {e}")

        custom_logger.debug(f"Processing {len(self.shop_file_configs)} files...")
        for shop_config in self.shop_file_configs.values():
            upload_shop_config_file(client=client, config=shop_config, overwrite=overwrite, data_set_id=data_set_id)

    def __add__(self, other: "BootstrapResourceCollection") -> "BootstrapResourceCollection":
        return BootstrapResourceCollection(
            assets=self.assets | other.assets,
            relationships=self.relationships | other.relationships,
            label_definitions=self.label_definitions | other.label_definitions,
            events=self.events | other.events,
            sequences=self.sequences | other.sequences,
            sequence_content=self.sequence_content | other.sequence_content,
            shop_file_configs=self.shop_file_configs | other.shop_file_configs,
        )

    def __iadd__(self, other: "BootstrapResourceCollection") -> "BootstrapResourceCollection":
        self.assets |= other.assets
        self.relationships |= other.relationships
        self.label_definitions |= other.label_definitions
        self.events |= other.events
        self.sequences |= other.sequences
        self.sequence_content |= other.sequence_content
        self.shop_file_configs |= other.shop_file_configs
        return self

    def difference(self, cdf: "BootstrapResourceCollection") -> dict[str, str]:
        local = self

        resource_diff: dict[str, str] = {}

        # First, we handle the cdf resources (everything except sequence data and shop files)
        for cdf_resource in [
            "assets",
            "relationships",
            "sequences",
            "label_definitions",
            "events",
        ]:
            local_resources = {
                resource.external_id: resource.dump()
                for resource in getattr(local, cdf_resource).values()
                if not (isinstance(resource, Event) and resource.type == "POWEROPS_BOOTSTRAP_FINISHED")
            }
            clean_local_resources_for_diff(local_resources)
            cdf_resources = {
                resource.external_id: resource.dump()
                for resource in getattr(cdf, cdf_resource).values()
                if not (isinstance(resource, Event) and resource.type == "POWEROPS_BOOTSTRAP_FINISHED")
            }
            clean_cdf_resources_for_diff(cdf_resources)

            resource_diff[cdf_resource] = DeepDiff(cdf_resources, local_resources, ignore_string_case=True).pretty()

        # Then we handle the sequence content
        local_sequence_content = {external_id: data.dump() for external_id, data in local.sequence_content.items()}
        cdf_sequence_content = {external_id: data.dump() for external_id, data in cdf.sequence_content.items()}
        sequence_content_diff = {
            "sequence_content": DeepDiff(
                cdf_sequence_content, local_sequence_content, ignore_nan_inequality=True
            ).pretty()
        }

        # And then finally the shop files
        shop_file_diff = {
            "shop_files": DeepDiff(
                {external_id: config.dict(exclude={"path"}) for external_id, config in cdf.shop_file_configs.items()},
                {external_id: config.dict(exclude={"path"}) for external_id, config in local.shop_file_configs.items()},
            ).pretty()
        }

        return resource_diff | sequence_content_diff | shop_file_diff

    @classmethod
    def prettify_differences(cls, diff_per_resource_type: dict[str, str]) -> str:
        output_message = ""
        for resource_type, differences in diff_per_resource_type.items():
            if differences:
                output_message += "--------------------------------------------\n"
                output_message += f"Difference for resource type {resource_type}: \n"
                output_message += "--------------------------------------------\n"
                output_message += differences
                output_message += "\n"
        return output_message.replace("added to dictionary.", "missing in CDF.").replace(
            "removed from dictionary.", "missing locally."
        )

    @classmethod
    def from_cdf(
        cls,
        client: CogniteClient,
        data_set_external_id: str,
    ) -> "BootstrapResourceCollection":
        """
        Function that creates a BootstrapResourceCollection from a CDF data set (typically the bootstrap data set)
        """

        data_set_id = client.data_sets.retrieve(external_id=data_set_external_id).id
        bootstrap_resource_collection = BootstrapResourceCollection()

        # start by downloading all the resources from CDF
        for resource_type in [
            "assets",
            "sequences",
            "relationships",
            "labels",
            "events",
        ]:
            # get the api for the resource type
            api = getattr(client, resource_type)
            # get all the resources from CDF
            resources = api.list(data_set_ids=[data_set_id], limit=None)
            # add the resources to the bootstrap resource collection
            bootstrap_resource_collection.add(resources)

        file_meta = client.files.list(data_set_ids=[data_set_id], limit=None)
        existing_external_ids = {f.external_id for f in file_meta}
        shop_files = []
        for f in file_meta:
            shop_config = ShopFileConfig.from_file_meta(f)
            if shop_config.md5_hash is None and shop_config.external_id in existing_external_ids:
                file_content = client.files.download_bytes(external_id=shop_config.external_id)
                shop_config.set_md5_hash(file_content)
            shop_files.append(shop_config)
        bootstrap_resource_collection.add(shop_files)

        # then download the sequence data
        for external_id in bootstrap_resource_collection.sequences:
            sequence_data = client.sequences.data.retrieve_dataframe(
                external_id=external_id, limit=None, start=0, end=-1
            )
            bootstrap_resource_collection.add(SequenceContent(sequence_external_id=external_id, data=sequence_data))

        return bootstrap_resource_collection
