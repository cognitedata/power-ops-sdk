from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from cognite.client import data_modeling as dm
from pydantic import Field

from ._core import DomainModel, DomainModelApply, TypeApplyList, TypeList

if TYPE_CHECKING:
    from ._date_time_interval import DateTimeIntervalApply
    from ._duration import DurationApply
    from ._market_agreement import MarketAgreementApply
    from ._market_participant import MarketParticipantApply
    from ._mba_domain import MBADomainApply
    from ._reason import ReasonApply
    from ._reserve_bid import ReserveBidApply

__all__ = [
    "BidTimeSeries",
    "BidTimeSeriesApply",
    "BidTimeSeriesList",
    "BidTimeSeriesApplyList",
    "BidTimeSeriesFields",
    "BidTimeSeriesTextFields",
]


BidTimeSeriesTextFields = Literal[
    "m_rid",
    "auction",
    "quantity_measure_unit_name",
    "currency_unit_name",
    "price_measure_unit_name",
    "registered_resources_mrid",
    "flow_direction",
    "energy_price_measure_unit",
    "standard_market_product_type",
    "original_market_product_type",
]
BidTimeSeriesFields = Literal[
    "m_rid",
    "auction",
    "quantity_measure_unit_name",
    "currency_unit_name",
    "price_measure_unit_name",
    "divisible",
    "block_bid",
    "status",
    "priority",
    "registered_resources_mrid",
    "flow_direction",
    "step_increment_quantity",
    "energy_price_measure_unit",
    "standard_market_product_type",
    "original_market_product_type",
]

_BIDTIMESERIES_PROPERTIES_BY_FIELD = {
    "m_rid": "mRID",
    "auction": "auction",
    "quantity_measure_unit_name": "quantityMeasureUnitName",
    "currency_unit_name": "currencyUnitName",
    "price_measure_unit_name": "priceMeasureUnitName",
    "divisible": "divisible",
    "block_bid": "blockBid",
    "status": "status",
    "priority": "priority",
    "registered_resources_mrid": "registeredResourcesMRID",
    "flow_direction": "flowDirection",
    "step_increment_quantity": "stepIncrementQuantity",
    "energy_price_measure_unit": "energyPriceMeasureUnit",
    "standard_market_product_type": "standardMarketProductType",
    "original_market_product_type": "originalMarketProductType",
}


class BidTimeSeries(DomainModel):
    space: str = "power-ops"
    m_rid: Optional[str] = Field(None, alias="mRID")
    auction: Optional[str] = None
    acquiring_domain: Optional[str] = Field(None, alias="acquiringDomain")
    connecting_domain: Optional[str] = Field(None, alias="connectingDomain")
    provider_market_participant: Optional[str] = Field(None, alias="providerMarketParticipant")
    quantity_measure_unit_name: Optional[str] = Field(None, alias="quantityMeasureUnitName")
    currency_unit_name: Optional[str] = Field(None, alias="currencyUnitName")
    price_measure_unit_name: Optional[str] = Field(None, alias="priceMeasureUnitName")
    divisible: Optional[bool] = None
    linked_bid: Optional[str] = Field(None, alias="linkedBid")
    multipart_bid: Optional[str] = Field(None, alias="multipartBid")
    exclusive_bid: Optional[str] = Field(None, alias="exclusiveBid")
    block_bid: Optional[bool] = Field(None, alias="blockBid")
    status: Optional[int] = None
    priority: Optional[int] = None
    registered_resources_mrid: Optional[str] = Field(None, alias="registeredResourcesMRID")
    flow_direction: Optional[str] = Field(None, alias="flowDirection")
    step_increment_quantity: Optional[float] = Field(None, alias="stepIncrementQuantity")
    energy_price_measure_unit: Optional[str] = Field(None, alias="energyPriceMeasureUnit")
    market_agreement: Optional[str] = Field(None, alias="marketAgreement")
    activation_constraint: Optional[str] = Field(None, alias="activationConstraint")
    resting_constraint: Optional[str] = Field(None, alias="restingConstraint")
    minimum_constraint: Optional[str] = Field(None, alias="minimumConstraint")
    maximum_constraint: Optional[str] = Field(None, alias="maximumConstraint")
    standard_market_product_type: Optional[str] = Field(None, alias="standardMarketProductType")
    original_market_product_type: Optional[str] = Field(None, alias="originalMarketProductType")
    validity_period: Optional[str] = Field(None, alias="validityPeriod")
    reason: Optional[str] = None

    def as_apply(self) -> BidTimeSeriesApply:
        return BidTimeSeriesApply(
            external_id=self.external_id,
            m_rid=self.m_rid,
            auction=self.auction,
            acquiring_domain=self.acquiring_domain,
            connecting_domain=self.connecting_domain,
            provider_market_participant=self.provider_market_participant,
            quantity_measure_unit_name=self.quantity_measure_unit_name,
            currency_unit_name=self.currency_unit_name,
            price_measure_unit_name=self.price_measure_unit_name,
            divisible=self.divisible,
            linked_bid=self.linked_bid,
            multipart_bid=self.multipart_bid,
            exclusive_bid=self.exclusive_bid,
            block_bid=self.block_bid,
            status=self.status,
            priority=self.priority,
            registered_resources_mrid=self.registered_resources_mrid,
            flow_direction=self.flow_direction,
            step_increment_quantity=self.step_increment_quantity,
            energy_price_measure_unit=self.energy_price_measure_unit,
            market_agreement=self.market_agreement,
            activation_constraint=self.activation_constraint,
            resting_constraint=self.resting_constraint,
            minimum_constraint=self.minimum_constraint,
            maximum_constraint=self.maximum_constraint,
            standard_market_product_type=self.standard_market_product_type,
            original_market_product_type=self.original_market_product_type,
            validity_period=self.validity_period,
            reason=self.reason,
        )


class BidTimeSeriesApply(DomainModelApply):
    space: str = "power-ops"
    m_rid: Optional[str] = Field(None, alias="mRID")
    auction: Optional[str] = None
    acquiring_domain: Union[MBADomainApply, str, None] = Field(None, repr=False, alias="acquiringDomain")
    connecting_domain: Union[MBADomainApply, str, None] = Field(None, repr=False, alias="connectingDomain")
    provider_market_participant: Union[MarketParticipantApply, str, None] = Field(
        None, repr=False, alias="providerMarketParticipant"
    )
    quantity_measure_unit_name: Optional[str] = Field(None, alias="quantityMeasureUnitName")
    currency_unit_name: Optional[str] = Field(None, alias="currencyUnitName")
    price_measure_unit_name: Optional[str] = Field(None, alias="priceMeasureUnitName")
    divisible: Optional[bool] = None
    linked_bid: Union[ReserveBidApply, str, None] = Field(None, repr=False, alias="linkedBid")
    multipart_bid: Union[ReserveBidApply, str, None] = Field(None, repr=False, alias="multipartBid")
    exclusive_bid: Union[ReserveBidApply, str, None] = Field(None, repr=False, alias="exclusiveBid")
    block_bid: Optional[bool] = Field(None, alias="blockBid")
    status: Optional[int] = None
    priority: Optional[int] = None
    registered_resources_mrid: Optional[str] = Field(None, alias="registeredResourcesMRID")
    flow_direction: Optional[str] = Field(None, alias="flowDirection")
    step_increment_quantity: Optional[float] = Field(None, alias="stepIncrementQuantity")
    energy_price_measure_unit: Optional[str] = Field(None, alias="energyPriceMeasureUnit")
    market_agreement: Union[MarketAgreementApply, str, None] = Field(None, repr=False, alias="marketAgreement")
    activation_constraint: Union[DurationApply, str, None] = Field(None, repr=False, alias="activationConstraint")
    resting_constraint: Union[DurationApply, str, None] = Field(None, repr=False, alias="restingConstraint")
    minimum_constraint: Union[DurationApply, str, None] = Field(None, repr=False, alias="minimumConstraint")
    maximum_constraint: Union[DurationApply, str, None] = Field(None, repr=False, alias="maximumConstraint")
    standard_market_product_type: Optional[str] = Field(None, alias="standardMarketProductType")
    original_market_product_type: Optional[str] = Field(None, alias="originalMarketProductType")
    validity_period: Union[DateTimeIntervalApply, str, None] = Field(None, repr=False, alias="validityPeriod")
    reason: Union[ReasonApply, str, None] = Field(None, repr=False)

    def _to_instances_apply(self, cache: set[str]) -> dm.InstancesApply:
        if self.external_id in cache:
            return dm.InstancesApply(dm.NodeApplyList([]), dm.EdgeApplyList([]))

        sources = []
        properties = {}
        if self.m_rid is not None:
            properties["mRID"] = self.m_rid
        if self.auction is not None:
            properties["auction"] = self.auction
        if self.acquiring_domain is not None:
            properties["acquiringDomain"] = {
                "space": "power-ops",
                "externalId": self.acquiring_domain
                if isinstance(self.acquiring_domain, str)
                else self.acquiring_domain.external_id,
            }
        if self.connecting_domain is not None:
            properties["connectingDomain"] = {
                "space": "power-ops",
                "externalId": self.connecting_domain
                if isinstance(self.connecting_domain, str)
                else self.connecting_domain.external_id,
            }
        if self.provider_market_participant is not None:
            properties["providerMarketParticipant"] = {
                "space": "power-ops",
                "externalId": self.provider_market_participant
                if isinstance(self.provider_market_participant, str)
                else self.provider_market_participant.external_id,
            }
        if self.quantity_measure_unit_name is not None:
            properties["quantityMeasureUnitName"] = self.quantity_measure_unit_name
        if self.currency_unit_name is not None:
            properties["currencyUnitName"] = self.currency_unit_name
        if self.price_measure_unit_name is not None:
            properties["priceMeasureUnitName"] = self.price_measure_unit_name
        if self.divisible is not None:
            properties["divisible"] = self.divisible
        if self.linked_bid is not None:
            properties["linkedBid"] = {
                "space": "power-ops",
                "externalId": self.linked_bid if isinstance(self.linked_bid, str) else self.linked_bid.external_id,
            }
        if self.multipart_bid is not None:
            properties["multipartBid"] = {
                "space": "power-ops",
                "externalId": self.multipart_bid
                if isinstance(self.multipart_bid, str)
                else self.multipart_bid.external_id,
            }
        if self.exclusive_bid is not None:
            properties["exclusiveBid"] = {
                "space": "power-ops",
                "externalId": self.exclusive_bid
                if isinstance(self.exclusive_bid, str)
                else self.exclusive_bid.external_id,
            }
        if self.block_bid is not None:
            properties["blockBid"] = self.block_bid
        if self.status is not None:
            properties["status"] = self.status
        if self.priority is not None:
            properties["priority"] = self.priority
        if self.registered_resources_mrid is not None:
            properties["registeredResourcesMRID"] = self.registered_resources_mrid
        if self.flow_direction is not None:
            properties["flowDirection"] = self.flow_direction
        if self.step_increment_quantity is not None:
            properties["stepIncrementQuantity"] = self.step_increment_quantity
        if self.energy_price_measure_unit is not None:
            properties["energyPriceMeasureUnit"] = self.energy_price_measure_unit
        if self.market_agreement is not None:
            properties["marketAgreement"] = {
                "space": "power-ops",
                "externalId": self.market_agreement
                if isinstance(self.market_agreement, str)
                else self.market_agreement.external_id,
            }
        if self.activation_constraint is not None:
            properties["activationConstraint"] = {
                "space": "power-ops",
                "externalId": self.activation_constraint
                if isinstance(self.activation_constraint, str)
                else self.activation_constraint.external_id,
            }
        if self.resting_constraint is not None:
            properties["restingConstraint"] = {
                "space": "power-ops",
                "externalId": self.resting_constraint
                if isinstance(self.resting_constraint, str)
                else self.resting_constraint.external_id,
            }
        if self.minimum_constraint is not None:
            properties["minimumConstraint"] = {
                "space": "power-ops",
                "externalId": self.minimum_constraint
                if isinstance(self.minimum_constraint, str)
                else self.minimum_constraint.external_id,
            }
        if self.maximum_constraint is not None:
            properties["maximumConstraint"] = {
                "space": "power-ops",
                "externalId": self.maximum_constraint
                if isinstance(self.maximum_constraint, str)
                else self.maximum_constraint.external_id,
            }
        if self.standard_market_product_type is not None:
            properties["standardMarketProductType"] = self.standard_market_product_type
        if self.original_market_product_type is not None:
            properties["originalMarketProductType"] = self.original_market_product_type
        if self.validity_period is not None:
            properties["validityPeriod"] = {
                "space": "power-ops",
                "externalId": self.validity_period
                if isinstance(self.validity_period, str)
                else self.validity_period.external_id,
            }
        if self.reason is not None:
            properties["reason"] = {
                "space": "power-ops",
                "externalId": self.reason if isinstance(self.reason, str) else self.reason.external_id,
            }
        if properties:
            source = dm.NodeOrEdgeData(
                source=dm.ContainerId("power-ops", "BidTimeSeries"),
                properties=properties,
            )
            sources.append(source)
        if sources:
            this_node = dm.NodeApply(
                space=self.space,
                external_id=self.external_id,
                existing_version=self.existing_version,
                sources=sources,
            )
            nodes = [this_node]
        else:
            nodes = []

        edges = []
        cache.add(self.external_id)

        if isinstance(self.acquiring_domain, DomainModelApply):
            instances = self.acquiring_domain._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.connecting_domain, DomainModelApply):
            instances = self.connecting_domain._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.provider_market_participant, DomainModelApply):
            instances = self.provider_market_participant._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.linked_bid, DomainModelApply):
            instances = self.linked_bid._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.multipart_bid, DomainModelApply):
            instances = self.multipart_bid._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.exclusive_bid, DomainModelApply):
            instances = self.exclusive_bid._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.market_agreement, DomainModelApply):
            instances = self.market_agreement._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.activation_constraint, DomainModelApply):
            instances = self.activation_constraint._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.resting_constraint, DomainModelApply):
            instances = self.resting_constraint._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.minimum_constraint, DomainModelApply):
            instances = self.minimum_constraint._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.maximum_constraint, DomainModelApply):
            instances = self.maximum_constraint._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.validity_period, DomainModelApply):
            instances = self.validity_period._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        if isinstance(self.reason, DomainModelApply):
            instances = self.reason._to_instances_apply(cache)
            nodes.extend(instances.nodes)
            edges.extend(instances.edges)

        return dm.InstancesApply(dm.NodeApplyList(nodes), dm.EdgeApplyList(edges))


class BidTimeSeriesList(TypeList[BidTimeSeries]):
    _NODE = BidTimeSeries

    def as_apply(self) -> BidTimeSeriesApplyList:
        return BidTimeSeriesApplyList([node.as_apply() for node in self.data])


class BidTimeSeriesApplyList(TypeApplyList[BidTimeSeriesApply]):
    _NODE = BidTimeSeriesApply
