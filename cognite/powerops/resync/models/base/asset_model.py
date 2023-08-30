from __future__ import annotations

from abc import ABC
from collections import defaultdict

from typing import TypeVar, ClassVar, Optional, Union, Callable, Iterable, Type as TypingType, Any
from typing_extensions import Self

from cognite.client.data_classes import Asset, Relationship, AssetList, TimeSeries

from cognite.powerops.clients.powerops_client import PowerOpsClient
from cognite.powerops.resync.models.base.asset_type import AssetType
from cognite.powerops.resync.models.base.model import Model
from cognite.powerops.resync.models.cdf_resources import CDFFile, CDFSequence
from cognite.powerops.resync.utils.serializer import get_pydantic_annotation


class AssetModel(Model, ABC):
    root_asset: ClassVar[Optional[Asset]] = None

    def assets(self) -> list[Asset]:
        return [item.as_asset() for item in self._asset_types()]

    def relationships(self) -> list[Relationship]:
        return [edge for item in self._asset_types() for edge in item.relationships()]

    def parent_assets(cls, include_root: bool = True) -> AssetList:
        if not cls.root_asset:
            return AssetList([])

        def _to_name(external_id: str) -> str:
            parts = external_id.replace("_", " ").split(" ")
            if parts[0].lower() == "rkom":
                parts[0] = parts[0].upper()
            else:
                parts[0] = parts[0].title()
            # The replacing is to have avoid doing changes, even though this is inconsistent with the other parent
            # assets.
            return " ".join(parts).replace("Bid process", "Bid").replace("bid process", "bid")

        parent_and_description_ids = set()
        for field_name, field in cls.model_fields.items():
            annotation, outer = get_pydantic_annotation(field.annotation, cls)
            if issubclass(annotation, AssetType) and outer is list:
                parent_and_description_ids.add((annotation.parent_external_id, annotation.parent_description or ""))

        return AssetList(
            ([cls.root_asset] if include_root else [])
            + [
                Asset(
                    external_id=parent_id,
                    name=_to_name(parent_id),
                    parent_external_id=cls.root_asset.external_id,
                    description=description,
                )
                for parent_id, description in parent_and_description_ids
            ]
        )

    cdf_resources: ClassVar[dict[Union[Callable, tuple[Callable, str]], type]] = {
        **dict(Model.cdf_resources.items()),
        assets: Asset,
        relationships: Relationship,
        parent_assets: Asset,
    }

    # Need to set classmethod here to have access to the underlying function in cdf_resources
    parent_assets = classmethod(parent_assets)

    @classmethod
    def load_from_cdf_resources(cls: TypingType[Self], data: dict[str, Any]) -> Self:
        loaded_by_type_external_id = cls._load_by_type_external_id(data)

        parsed = cls._create_cls_arguments(loaded_by_type_external_id)

        instance = cls(**parsed)

        cls._set_linked_resources(loaded_by_type_external_id)
        return instance

    def sort_lists(self) -> None:
        for field_name, field in self.model_fields.items():
            annotation, outer = get_pydantic_annotation(field.annotation, type(self))
            if issubclass(annotation, (AssetType, CDFFile, CDFSequence)) and outer is list:
                getattr(self, field_name).sort(key=lambda x: x.external_id)
                for asset_type in getattr(self, field_name):
                    asset_type.sort_lists()

    @classmethod
    def from_cdf(
        cls: TypingType[T_Asset_Model],
        client: PowerOpsClient,
        data_set_external_id: str,
    ) -> T_Asset_Model:
        cdf = client.cdf
        parent_assets = cls.parent_assets(include_root=False)

        assets = cdf.assets.list(
            asset_subtree_external_ids=parent_assets.as_external_ids(),
            limit=-1,
            data_set_external_ids=[data_set_external_id],
        )
        relationships = cdf.relationships.list(
            source_external_ids=assets.as_external_ids(),
            source_types=["asset"],
            target_types=["timeseries", "asset", "sequence", "file"],
            data_set_external_ids=[data_set_external_id],
            limit=-1,
        )
        # TimeSeries are a special case as resync are only referring to them by external id.
        # So we do not need to fetch them
        time_series = [
            TimeSeries(external_id=relationship.target_external_id)
            for relationship in relationships
            if relationship.target_type.casefold() == "timeseries"
        ]
        sequence_ids = [
            relationship.target_external_id
            for relationship in relationships
            if relationship.target_type.casefold() == "sequence"
        ]
        if sequence_ids:
            sequences = cdf.sequences.retrieve_multiple(external_ids=sequence_ids)
            cdf_sequences = [CDFSequence(sequence=sequence) for sequence in sequences]
            cls._add_missing_hash(cdf, cdf_sequences)
        else:
            cdf_sequences = []

        file_ids = [
            relationship.target_external_id
            for relationship in relationships
            if relationship.target_type.casefold() == "file"
        ]
        if file_ids:
            files = cdf.files.retrieve_multiple(external_ids=file_ids)
            cdf_files = [CDFFile(meta=file) for file in files]
            cls._add_missing_hash(cdf, cdf_files)
        else:
            cdf_files = []

        return cls.load_from_cdf_resources(
            {
                "assets": assets,
                "relationships": relationships,
                "sequences": cdf_sequences,
                "timeseries": time_series,
                "files": cdf_files,
            }
        )

    def _asset_types(self) -> Iterable[AssetType]:
        yield from (item for item in self._resource_types() if isinstance(item, AssetType))

    @classmethod
    def _create_cls_arguments(cls, loaded_by_type_external_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
        asset_by_parent_external_id = defaultdict(list)
        for asset in loaded_by_type_external_id["assets"].values():
            asset_by_parent_external_id[asset.parent_external_id].append(asset)
        arguments = {}
        asset_type_by_external_id = {}
        for field_name, field in cls.model_fields.items():
            annotation, outer = get_pydantic_annotation(field.annotation, cls)
            if issubclass(annotation, AssetType) and (
                assets := asset_by_parent_external_id.get(annotation.parent_external_id)
            ):
                if outer is list:
                    arguments[field_name] = [annotation.from_asset(asset) for asset in assets]
                    asset_type_by_external_id.update({asset.external_id: asset for asset in arguments[field_name]})
                else:
                    raise NotImplementedError()
            else:
                raise NotImplementedError()
        loaded_by_type_external_id["assets"] = asset_type_by_external_id
        return arguments

    @classmethod
    def _set_linked_resources(cls, loaded_by_type_external_id: dict[str, dict[str, Any]]) -> None:
        relationships_by_source_external_id = defaultdict(list)
        for relationship in loaded_by_type_external_id["relationships"].values():
            relationships_by_source_external_id[relationship.source_external_id].append(relationship)

        for source_id, relationships in relationships_by_source_external_id.items():
            if not (source := loaded_by_type_external_id["assets"].get(source_id)):
                # Todo print warning
                # Missing source
                continue

            for relationship in relationships:
                target_type = relationship.target_type.casefold()
                for key in (target_type + "s", target_type):
                    if target_items := loaded_by_type_external_id.get(key):
                        break
                else:
                    # Todo print warning
                    # Missing target type
                    continue
                if not (target := target_items.get(relationship.target_external_id)):
                    # Todo print warning
                    # Missing target external id
                    continue

                field_name = relationship.labels[0].external_id.split(".")[1]
                if field_name not in source.model_fields:
                    field_name += "s"
                if field_name not in source.model_fields:
                    field_name = field_name.rsplit("_", maxsplit=1)[0]
                if field_name not in source.model_fields and field_name == "reservoirs":
                    field_name = "inlet_reservoir"

                if field_name not in source.model_fields:
                    raise ValueError(f"Cannot find field {field_name} in {source}")

                annotation, outer = get_pydantic_annotation(source.model_fields[field_name].annotation, source)
                if outer is list:
                    getattr(source, field_name).append(target)
                elif outer is dict:
                    getattr(source, field_name)[target.external_id] = target
                elif outer is None:
                    setattr(source, field_name, target)
                else:
                    raise NotImplementedError()


T_Asset_Model = TypeVar("T_Asset_Model", bound=AssetModel)
