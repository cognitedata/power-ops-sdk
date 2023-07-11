from __future__ import annotations

import hashlib
import logging
from typing import Union, cast

from cognite.client.data_classes import Asset, Event, LabelDefinition, Relationship
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
from cognite.client.data_classes.data_modeling import EdgeApply, NodeApply
from cognite.client.exceptions import CogniteAPIError
from deepdiff import DeepDiff
from pydantic import BaseModel, Extra

from cognite.powerops.clients.cogshop.data_classes import (
    FileRef,
    FileRefApply,
    Mapping,
    MappingApply,
    ModelTemplate,
    ModelTemplateApply,
    Transformation,
    TransformationApply,
)
from cognite.powerops.clients.cogshop.data_classes._core import DomainModel, DomainModelApply, InstancesApply
from cognite.powerops.clients.powerops_client import PowerOpsClient
from cognite.powerops.resync.config.cogshop.shop_file_config import ShopFileConfig
from cognite.powerops.resync.config.shared import ExternalId
from cognite.powerops.resync.models.cdf_resources import CDFFile, CDFSequence
from cognite.powerops.utils.cdf.calls import upsert_cognite_resources

logger = logging.getLogger(__name__)

AddableResourceT = Union[
    "CogniteResource",
    list["CogniteResource"],
    list[ShopFileConfig],
    list[DomainModelApply],
    InstancesApply,
    CDFFile,
    list[CDFFile],
    CDFSequence,
    list[CDFSequence],
]


def dump_cdf_resource(resource) -> dict:
    """Legacy or DM resource."""
    try:
        dump_func = resource.dump
    except AttributeError:
        dump_func = resource.dict
    return dump_func()


class ResourceCollection(BaseModel):
    # CDF resources
    assets: dict[ExternalId, Asset] = {}
    relationships: dict[ExternalId, Relationship] = {}
    label_definitions: dict[ExternalId, LabelDefinition] = {}
    events: dict[ExternalId, Event] = {}
    files: dict[ExternalId, CDFFile] = {}
    sequences: dict[ExternalId, CDFSequence] = {}

    # dm resources:
    nodes: dict[ExternalId, NodeApply] = {}
    edges: dict[ExternalId, EdgeApply] = {}
    model_templates: dict[ExternalId, ModelTemplateApply] = {}
    mappings: dict[ExternalId, MappingApply] = {}
    file_refs: dict[ExternalId, FileRefApply] = {}
    transformations: dict[ExternalId, TransformationApply] = {}

    class Config:
        arbitrary_types_allowed = True
        extra = Extra.forbid

    @property
    def all_cdf_resources(self) -> list[CogniteResource]:
        """Not including DM."""
        return [
            *list(self.assets.values()),
            *list(self.relationships.values()),
            *list(self.sequences.values()),
            *list(self.label_definitions.values()),
            *list(self.events.values()),
        ]

    def add(self, resources_to_append: AddableResourceT):
        # sort the resource by type and append to the correct list
        if isinstance(resources_to_append, (list, CogniteResourceList)):
            for resource in resources_to_append:
                self._add_resource(resource)
        elif isinstance(resources_to_append, InstancesApply):
            for node in resources_to_append.nodes:
                self._add_resource(node)
            for edge in resources_to_append.edges:
                self._add_resource(edge)
        else:
            self._add_resource(resources_to_append)

    def _add_resource(self, resource: CogniteResource | ShopFileConfig):
        # cdf
        if isinstance(resource, Asset):
            self.assets[resource.external_id] = resource
        elif isinstance(resource, Relationship):
            self.relationships[resource.external_id] = resource
        elif isinstance(resource, LabelDefinition):
            self.label_definitions[resource.external_id] = resource
        elif isinstance(resource, Event):
            self.events[resource.external_id] = resource
        elif isinstance(resource, CDFFile):
            self.files[resource.external_id] = resource
        elif isinstance(resource, CDFSequence):
            self.sequences[resource.external_id] = resource
        # dm
        elif isinstance(resource, NodeApply):
            self.nodes[resource.external_id] = resource
        elif isinstance(resource, EdgeApply):
            self.edges[resource.external_id] = resource
        elif isinstance(resource, ModelTemplateApply):
            self.model_templates[resource.external_id] = resource
        elif isinstance(resource, MappingApply):
            self.mappings[resource.external_id] = resource
        elif isinstance(resource, TransformationApply):
            self.transformations[resource.external_id] = resource
        elif isinstance(resource, FileRefApply):
            self.file_refs[resource.external_id] = resource
        else:
            raise ValueError(f"Unknown resource type: {type(resource)}")

    def write_to_cdf(
        self,
        po_client: PowerOpsClient,
        data_set_external_id: str,
        overwrite: bool = False,
    ):
        # CDF auth and data set
        data_set_id = po_client.cdf.data_sets.retrieve(external_id=data_set_external_id).id
        for resource in self.all_cdf_resources:
            resource.data_set_id = data_set_id

        cdf_api_by_type = {
            "label_definitions": po_client.cdf.labels,
            "assets": po_client.cdf.assets,
            "relationships": po_client.cdf.relationships,
            "events": po_client.cdf.events,
        }
        for resource_type, api in cdf_api_by_type.items():
            resources = list(getattr(self, resource_type).values())
            logger.debug(f"Processing {len(resources)} {resource_type}...")
            upsert_cognite_resources(api, resource_type, resources)

        po_client.cdf.data_modeling.instances.apply(
            nodes=list(self.nodes.values()), edges=list(self.edges.values()), replace=True
        )

        logger.debug(f"Processing {len(self.files)} files...")
        for file in self.files.values():
            po_client.cdf.files.upload_bytes(
                content=file.content, data_set_id=data_set_id, overwrite=overwrite, **file.meta.dump(camel_case=False)
            )

        logger.debug(f"Processing {len(self.sequences)} sequences...")
        upsert_cognite_resources(
            po_client.cdf.sequences, "sequences", [cdf_sequence.sequence for cdf_sequence in self.sequences.values()]
        )
        for sequence_external_id, cdf_sequence in self.sequences.items():
            if cdf_sequence.content.empty:
                logger.warning(f"Empty sequence dataframe for {sequence_external_id}")
                continue
            try:
                po_client.cdf.sequences.data.insert_dataframe(
                    dataframe=cdf_sequence.content, external_id=sequence_external_id
                )
            except CogniteAPIError as e:
                logger.warning(f"Failed to insert sequence dataframe for {sequence_external_id}: {e}")

    def difference(self, cdf: "ResourceCollection") -> dict[str, str]:
        local = self

        resource_diff: dict[str, str] = {}

        # First, we handle the cdf resources (everything except sequence data and shop files)
        for cdf_resource in [
            "assets",
            "relationships",
            "label_definitions",
            "events",
            "model_templates",
            "file_refs",
            "mappings",
            "transformations",
            "sequences",
            "files",
        ]:
            local_resources = {
                resource.external_id: dump_cdf_resource(resource)
                for resource in getattr(local, cdf_resource).values()
                if not (isinstance(resource, Event) and resource.type == "POWEROPS_BOOTSTRAP_FINISHED")
            }
            clean_local_resources_for_diff(local_resources)
            cdf_resources = {
                resource.external_id: dump_cdf_resource(resource)
                for resource in getattr(cdf, cdf_resource).values()
                if not (isinstance(resource, Event) and resource.type == "POWEROPS_BOOTSTRAP_FINISHED")
            }
            clean_cdf_resources_for_diff(cdf_resources)

            resource_diff[cdf_resource] = DeepDiff(cdf_resources, local_resources, ignore_string_case=True).pretty()

        # Then we handle the sequence content
        local_sequence_content = {external_id: data.content.to_dict() for external_id, data in local.sequences.items()}
        cdf_sequence_content = {external_id: data.content.to_dict() for external_id, data in cdf.sequences.items()}
        sequence_content_diff = {
            "sequence_content": DeepDiff(
                cdf_sequence_content, local_sequence_content, ignore_nan_inequality=True
            ).pretty()
        }

        return resource_diff | sequence_content_diff

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
        po_client: PowerOpsClient,
        data_set_external_id: str,
    ) -> "ResourceCollection":
        """
        Function that creates a BootstrapResourceCollection from a CDF data set (typically the bootstrap data set)
        """

        data_set_id = po_client.cdf.data_sets.retrieve(external_id=data_set_external_id).id
        bootstrap_resource_collection = ResourceCollection()

        # start by downloading all the resources from CDF
        for resource_type in [
            "assets",
            "relationships",
            "labels",
            "events",
        ]:
            # get the api for the resource type
            api = getattr(po_client.cdf, resource_type)
            # get all the resources from CDF
            resources = api.list(data_set_ids=[data_set_id], limit=None)
            # add the resources to the bootstrap resource collection
            bootstrap_resource_collection.add(resources)

        for resource_type in [
            "file_refs",
            "transformations",
            "mappings",
            "model_templates",
        ]:
            # get the api for the resource type
            api = getattr(po_client.cog_shop, resource_type)
            # get all the resources from CDF
            actual_resources = api.list(limit=None)
            # convert actual resources to their "apply" form, for comparison
            apply_resources = to_dm_apply(actual_resources)
            # add the resources to the bootstrap resource collection
            bootstrap_resource_collection.add(apply_resources)

        file_meta = po_client.cdf.files.list(data_set_ids=[data_set_id], limit=None)
        shop_files = []
        for f in file_meta:
            if f.metadata.get("md5_hash") is None:
                file_content = po_client.cdf.files.download_bytes(external_id=f.external_id)
                md5_hash = hashlib.md5(file_content.replace(b"\r\n", b"\n")).hexdigest()
                f.metadata["md5_hash"] = md5_hash

            shop_files.append(CDFFile(meta=f))
        bootstrap_resource_collection.add(shop_files)

        # then download the sequence data
        all_sequences = po_client.cdf.sequences.list(data_set_ids=[data_set_id], limit=None)

        for sequence in all_sequences:
            sequence_data = po_client.cdf.sequences.data.retrieve_dataframe(
                external_id=sequence.external_id, limit=None, start=0, end=-1
            )
            bootstrap_resource_collection.add(CDFSequence(sequence=sequence, content=sequence_data))

        return bootstrap_resource_collection


def to_dm_apply(instances: list[DomainModel] | DomainModel) -> list[DomainModelApply]:
    items = cast(list[DomainModel], list(instances))
    apply_items = []

    type_map = {
        ModelTemplate: ModelTemplateApply,
        Mapping: MappingApply,
        Transformation: TransformationApply,
        FileRef: FileRefApply,
    }

    for item in items:
        if type(item) not in type_map:
            raise NotImplementedError(f"Dont know how to convert {item!r}.")
        apply_type = type_map[type(item)]

        apply_items.append(
            apply_type(**{field: value for field, value in item.dict().items() if field in apply_type.__fields__})
        )
    return apply_items


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
