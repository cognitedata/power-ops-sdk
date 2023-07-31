from __future__ import annotations
import json

from pathlib import Path
from typing import ClassVar, Optional, Union

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, TimeSeries
from pydantic import ConfigDict, Field

from cognite.powerops.cdf_labels import AssetLabel
from cognite.powerops.resync.models.base import AssetModel, AssetType, NonAssetType
from cognite.powerops.resync.models.cdf_resources import CDFSequence
from cognite.powerops.resync.models.helpers import match_field_from_relationship


class Generator(AssetType):
    parent_external_id: ClassVar[str] = "generators"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.GENERATOR
    p_min: float
    penstock: str
    startcost: float
    start_stop_cost_time_series: Optional[TimeSeries] = None
    generator_efficiency_curve: Optional[CDFSequence] = None
    turbine_efficiency_curve: Optional[CDFSequence] = None

    @classmethod
    def _from_asset(
        cls,
        asset: Asset,
        start_stop_cost_time_series: Optional[TimeSeries] = None,
        generator_efficiency_curve: Optional[CDFSequence] = None,
        turbine_efficiency_curve: Optional[CDFSequence] = None,
    ) -> Generator:
        return cls(
            _external_id=asset.external_id,
            name=asset.name,
            description=asset.description,
            p_min=float(asset.metadata.get("p_min", 0.0)),
            penstock=asset.metadata.get("penstock", ""),
            startcost=float(asset.metadata.get("startcost", 0.0)),
            start_stop_cost_time_series=start_stop_cost_time_series,
            generator_efficiency_curve=generator_efficiency_curve,
            turbine_efficiency_curve=turbine_efficiency_curve,
        )

    @classmethod
    def from_cdf(
        cls,
        client: CogniteClient,
        external_id: Optional[str] = "",
        asset: Optional[Asset] = None,
        fetch_metadata: bool = True,
        fetch_content: bool = False,
    ) -> Generator:
        if asset and external_id:
            raise ValueError("Only one of asset and external_id can be provided")
        if external_id:
            asset = client.assets.retrieve(external_id)
        if not asset:
            raise ValueError(f"Could not retrieve asset with {external_id=}")
        cdf_fields = {
            "start_stop_cost_time_series": None,
            "generator_efficiency_curve": None,
            "turbine_efficiency_curve": None,
        }

        if fetch_metadata:
            relationships = client.relationships.list(
                source_external_ids=[asset.external_id],
                source_types=["asset"],
                target_types=["timeseries", "sequence"],
                limit=-1,
            )
            for r in relationships:
                field = match_field_from_relationship(cls.model_fields.keys(), r)
                if r.target_type.lower() == "sequence":
                    cdf_fields[field] = CDFSequence.from_cdf(client, r.target_external_id, fetch_content)
                if r.target_type.lower() == "timeseries":
                    cdf_fields[field] = client.time_series.retrieve(r.target_external_id)

        return cls._from_asset(asset, **cdf_fields)


class Reservoir(AssetType):
    parent_external_id: ClassVar[str] = "reservoirs"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.RESERVOIR
    display_name: str
    ordering: str

    @classmethod
    def _from_asset(cls, asset: Asset) -> Reservoir:
        return cls(
            _external_id=asset.external_id,
            name=asset.name,
            description=asset.description,
            display_name=asset.metadata.get("display_name", ""),
            ordering=asset.metadata.get("ordering", ""),
        )

    @classmethod
    def from_cdf(
        cls,
        client: CogniteClient,
        external_id: Optional[str] = "",
        asset: Optional[Asset] = None,
        fetch_metadata: bool = False,
        fetch_content: bool = False,
    ) -> Reservoir:
        if asset and external_id:
            raise ValueError("Only one of asset and external_id can be provided")
        if asset:
            return cls._from_asset(asset)
        if asset := client.assets.retrieve(external_id):
            return cls._from_asset(asset)
        raise ValueError(f"Could not retrieve asset with {external_id=}")


class Plant(AssetType):
    parent_external_id: ClassVar[str] = "plants"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.PLANT
    display_name: str
    ordering: str
    head_loss_factor: float
    outlet_level: float
    p_min: float
    p_max: float
    penstock_head_loss_factors: dict
    generators: list[Generator] = Field(default_factory=list)
    inlet_reservoir: Optional[Reservoir] = None
    p_min_time_series: Optional[TimeSeries] = None
    p_max_time_series: Optional[TimeSeries] = None
    water_value_time_series: Optional[TimeSeries] = None
    feeding_fee_time_series: Optional[TimeSeries] = None
    outlet_level_time_series: Optional[TimeSeries] = None
    inlet_level_time_series: Optional[TimeSeries] = None
    head_direct_time_series: Optional[TimeSeries] = None

    @classmethod
    def _from_asset(
        cls,
        asset: Asset,
        generators: Optional[list[Generator]],
        inlet_reservoir: Optional[Reservoir] = None,
        p_min_time_series: Optional[TimeSeries] = None,
        p_max_time_series: Optional[TimeSeries] = None,
        water_value_time_series: Optional[TimeSeries] = None,
        feeding_fee_time_series: Optional[TimeSeries] = None,
        outlet_level_time_series: Optional[TimeSeries] = None,
        inlet_level_time_series: Optional[TimeSeries] = None,
        head_direct_time_series: Optional[TimeSeries] = None,
    ) -> Plant:
        penstock_head_loss_factors_raw: str = asset.metadata.get("penstock_head_loss_factors", "")
        try:
            penstock_head_loss_factors = json.loads(penstock_head_loss_factors_raw)
            if not isinstance(penstock_head_loss_factors, dict):
                raise TypeError
        except (json.JSONDecodeError, TypeError):
            penstock_head_loss_factors = {}

        return cls(
            _external_id=asset.external_id,
            name=asset.name,
            description=asset.description,
            display_name=asset.metadata.get("display_name", ""),
            ordering=asset.metadata.get("ordering", ""),
            head_loss_factor=float(asset.metadata.get("head_loss_factor", 0.0)),
            outlet_level=float(asset.metadata.get("outlet_level", 0.0)),
            p_min=float(asset.metadata.get("p_min", 0.0)),
            p_max=float(asset.metadata.get("p_max", 0.0)),
            penstock_head_loss_factors=penstock_head_loss_factors,
            generators=generators,
            inlet_reservoir=inlet_reservoir,
            p_min_time_series=p_min_time_series,
            p_max_time_series=p_max_time_series,
            water_value_time_series=water_value_time_series,
            feeding_fee_time_series=feeding_fee_time_series,
            outlet_level_time_series=outlet_level_time_series,
            inlet_level_time_series=inlet_level_time_series,
            head_direct_time_series=head_direct_time_series,
        )

    @classmethod
    def from_cdf(
        cls,
        client: CogniteClient,
        external_id: Optional[str] = "",
        asset: Optional[Asset] = None,
        fetch_metadata: bool = True,
        fetch_content: bool = False,
    ) -> Plant:
        if asset and external_id:
            raise ValueError("Only one of asset and external_id can be provided")
        if external_id:
            asset = client.assets.retrieve(external_id)
        if not asset:
            raise ValueError(f"Could not retrieve asset with {external_id=}")
        cdf_fields = {
            "generators": [],
            "inlet_reservoir": None,
            "p_min_time_series": None,
            "p_max_time_series": None,
            "water_value_time_series": None,
            "feeding_fee_time_series": None,
            "outlet_level_time_series": None,
            "inlet_level_time_series": None,
            "head_direct_time_series": None,
        }
        if fetch_metadata:
            relationships = client.relationships.list(
                source_external_ids=[asset.external_id],
                source_types=["asset"],
                target_types=["timeseries", "asset"],
                limit=-1,
            )
            for r in relationships:
                field = match_field_from_relationship(cls.model_fields.keys(), r)
                if r.target_type.lower() == "asset":
                    # todo: handle later -- we only want to instantiate a class
                    # one per ext id. Probably a dict when re-written to high-level.
                    # finding the field is still its own challenge
                    cdf_fields[field] = None if field == "inlet_reservoir" else []

                if r.target_type.lower() == "timeseries":
                    cdf_fields[field] = client.time_series.retrieve(r.target_external_id)
        return cls._from_asset(asset, **cdf_fields)


class WaterCourseShop(NonAssetType):
    penalty_limit: str


class Watercourse(AssetType):
    model_config: ClassVar[ConfigDict] = ConfigDict(protected_namespaces=tuple())
    parent_external_id: ClassVar[str] = "watercourses"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.WATERCOURSE
    shop: WaterCourseShop
    config_version: Optional[str] = Field(exclude=True)
    model_file: Optional[Path] = Field(exclude=True)
    processed_model_file: Optional[Path] = Field(exclude=True)
    plants: list[Plant]
    production_obligation_time_series: list[TimeSeries] = Field(default_factory=list)

    @classmethod
    def _from_asset(
        cls,
        asset: Asset,
        config_version: str = "",
        model_file: Optional[Path] = None,
        processed_model_file: Optional[Path] = None,
        plants: Optional[list[Plant]] = None,
        production_obligation_time_series: Optional[list[TimeSeries]] = None,
    ) -> Watercourse:
        return cls(
            _external_id=asset.external_id,
            name=asset.name,
            description=asset.description,
            shop=WaterCourseShop(penalty_limit=asset.metadata.get("shop:penalty_limit", "")),
            config_version=config_version,
            model_file=model_file,
            processed_model_file=processed_model_file,
            plants=plants or [],
            production_obligation_time_series=production_obligation_time_series or [],
        )

    @classmethod
    def from_cdf(
        cls,
        client: CogniteClient,
        external_id: Optional[str] = "",
        asset: Optional[Asset] = None,
        fetch_metadata: bool = True,
        fetch_content: bool = False,
    ) -> Watercourse:
        if asset and external_id:
            raise ValueError("Only one of asset and external_id can be provided")
        if external_id:
            asset = client.assets.retrieve(external_id)
        if not asset:
            raise ValueError(f"Could not retrieve asset with {external_id=}")
        cdf_fields = {
            "config_version": None,
            "plants": [],
            "production_obligation_time_series": [],
        }
        if fetch_metadata:
            relationships = client.relationships.list(
                source_external_ids=[asset.external_id],
                source_types=["asset"],
                target_types=["timeseries", "asset"],
                limit=-1,
            )
            for r in relationships:
                field = match_field_from_relationship(cls.model_fields.keys(), r)
                if r.target_type.lower() == "asset":
                    cdf_fields[field] = []
                if r.target_type.lower() == "timeseries":
                    cdf_fields[field] = client.time_series.retrieve(r.target_external_id)

        return cls._from_asset(asset, **cdf_fields)


class PriceArea(AssetType):
    parent_external_id: ClassVar[str] = "price_areas"
    label: ClassVar[Union[AssetLabel, str]] = AssetLabel.PRICE_AREA
    dayahead_price_time_series: Optional[TimeSeries] = None
    plants: list[Plant] = Field(default_factory=list)
    watercourses: list[Watercourse] = Field(default_factory=list)

    @classmethod
    def _from_asset(
        cls,
        asset: Asset,
        dayahead_price_time_series: Optional[TimeSeries] = None,
        plants: Optional[list[Plant]] = None,
        watercourses: Optional[list[Watercourse]] = None,
    ) -> PriceArea:
        return cls(
            _external_id=asset.external_id,
            name=asset.name,
            description=asset.description,
            dayahead_price_time_series=None,
            plants=plants or [],
            watercourses=watercourses or [],
        )

    @classmethod
    def from_cdf(
        cls,
        client: CogniteClient,
        external_id: Optional[str] = "",
        asset: Optional[Asset] = None,
        fetch_metadata: bool = True,
        fetch_content: bool = False,
    ) -> PriceArea:
        if asset and external_id:
            raise ValueError("Only one of asset and external_id can be provided")
        if external_id:
            asset = client.assets.retrieve(external_id)
        if not asset:
            raise ValueError(f"Could not retrieve asset with {external_id=}")
        cdf_fields = {
            "dayahead_price_time_series": None,
            "plants": [],
            "watercourses": [],
        }
        if fetch_metadata:
            relationships = client.relationships.list(
                source_external_ids=[asset.external_id],
                source_types=["asset"],
                target_types=["timeseries", "asset"],
                limit=-1,
            )
            for r in relationships:
                field = match_field_from_relationship(cls.model_fields.keys(), r)
                if r.target_type.lower() == "asset":
                    cdf_fields[field] = []
                if r.target_type.lower() == "timeseries":
                    cdf_fields[field] = client.time_series.retrieve(r.target_external_id)

        return cls._from_asset(asset, **cdf_fields)


class ProductionModel(AssetModel):
    root_asset: ClassVar[Asset] = Asset(external_id="power_ops", name="PowerOps")
    plants: list[Plant] = Field(default_factory=list)
    reservoirs: list[Reservoir] = Field(default_factory=list)
    generators: list[Generator] = Field(default_factory=list)
    watercourses: list[Watercourse] = Field(default_factory=list)
    price_areas: list[PriceArea] = Field(default_factory=list)

    def _prepare_for_diff(self: ProductionModel) -> dict:
        clone = self.model_copy(deep=True)

        for model_field in clone.model_fields:
            field_value = getattr(clone, model_field)
            if isinstance(field_value, list) and field_value and isinstance(field_value[0], AssetType):
                # Sort the asset types to have comparable order for diff
                _sorted = sorted(field_value, key=lambda x: x.external_id)
                # Prepare each asset type for diff
                _prepared = map(lambda x: x._asset_type_prepare_for_diff(), _sorted)
                setattr(clone, model_field, list(_prepared))
            elif isinstance(field_value, AssetType):
                # does not apply to this model, but
                # might be used in a higher level of abstraction
                field_value._asset_type_prepare_for_diff()
        return clone.model_dump()
